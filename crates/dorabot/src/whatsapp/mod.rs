//! WhatsApp adapter wiring for the bot (`dorabot` side).
//!
//! The neutral WhatsApp adapter lives in `doracore::messaging::whatsapp` — it
//! knows how to talk to Meta's Cloud API and how to normalize inbound webhook
//! JSON into neutral [`InboundMessage`]s, but nothing more. This module is the
//! bridge that plugs that island into the running service:
//!
//! - [`init`] builds the adapter from env (`WhatsAppConfig::from_env`), returns
//!   an axum [`Router`] to mount on the shared webhook server plus the outbound
//!   [`WhatsAppMessenger`] and the inbound receiver.
//! - [`spawn_inbound_consumer`] drains inbound messages and drives the
//!   download-and-return flow.
//!
//! Scope (vertical slice): a user sends a link → we download the audio via the
//! platform-neutral [`SourceRegistry`] primitives → we send the file back via
//! the neutral [`Messenger`]. No menus, preview cards, or callback buttons yet.
//! The whole path is gated on the four `WHATSAPP_*` secrets being present; when
//! any is missing the adapter simply never initializes and the Telegram bot is
//! unaffected.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use axum::Router;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use url::Url;

use doracore::download::source::{DownloadRequest, SourceProgress, SourceRegistry};
use doracore::messaging::capabilities::Capabilities;
use doracore::messaging::types::{
    ChatRef, InboundEvent, InboundMessage, MediaKind, MediaSource, OutboundMessage, TextStyle,
};
use doracore::messaging::whatsapp::server as wa_server;
use doracore::messaging::{Messenger, WhatsAppConfig, WhatsAppMessenger};

/// Public path the webhook is mounted at. Configure this exact path (on the
/// bot's public HTTPS host) as the callback URL in the Meta App dashboard.
const WEBHOOK_MOUNT_PATH: &str = "/whatsapp/webhook";

/// Cap concurrent inbound downloads so a burst of links can't exhaust the box.
const MAX_CONCURRENT_DOWNLOADS: usize = 4;

static DOWNLOAD_SEM: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_DOWNLOADS));

/// Monotonic per-process counter for unique temp output filenames.
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Everything the service needs to run the WhatsApp adapter, produced by
/// [`init`]. The caller mounts `router` on the shared webhook server and hands
/// `rx` + `messenger` to [`spawn_inbound_consumer`].
pub struct WhatsAppRuntime {
    /// axum router (already nested at [`WEBHOOK_MOUNT_PATH`]) to merge onto the
    /// bot's webhook app.
    pub router: Router,
    /// Outbound sender used to reply to users.
    pub messenger: Arc<WhatsAppMessenger>,
    /// Inbound stream of normalized messages from the webhook.
    pub rx: UnboundedReceiver<InboundMessage>,
}

/// Cheap check: are the WhatsApp secrets configured? Used to warn when the
/// adapter is configured but the bot is running in a mode that can't serve a
/// webhook (polling).
pub fn is_configured() -> bool {
    WhatsAppConfig::from_env().is_some()
}

/// Build the WhatsApp runtime from the environment. Returns `None` (adapter
/// disabled, zero behavior change) when any required `WHATSAPP_*` var is unset.
pub fn init() -> Option<WhatsAppRuntime> {
    let cfg = WhatsAppConfig::from_env()?;
    log::info!(
        "WhatsApp adapter enabled (phone_number_id={}, graph {}), webhook at {}",
        cfg.phone_number_id,
        cfg.graph_version,
        WEBHOOK_MOUNT_PATH,
    );

    let messenger = Arc::new(WhatsAppMessenger::from_config(cfg.clone()));
    let (tx, rx) = mpsc::unbounded_channel::<InboundMessage>();
    let router = Router::new().nest(WEBHOOK_MOUNT_PATH, wa_server::router(Arc::new(cfg), tx));

    Some(WhatsAppRuntime { router, messenger, rx })
}

/// Spawn the background task that drains inbound WhatsApp messages and drives
/// the download-and-return flow. Each message is handled on its own task so a
/// slow download never blocks the channel; a semaphore bounds concurrency.
pub fn spawn_inbound_consumer(mut rx: UnboundedReceiver<InboundMessage>, messenger: Arc<WhatsAppMessenger>) {
    tokio::spawn(async move {
        log::info!("WhatsApp inbound consumer started");
        while let Some(msg) = rx.recv().await {
            let messenger = Arc::clone(&messenger);
            tokio::spawn(async move {
                handle_inbound(msg, messenger).await;
            });
        }
        log::warn!("WhatsApp inbound channel closed; consumer exiting");
    });
}

async fn handle_inbound(msg: InboundMessage, messenger: Arc<WhatsAppMessenger>) {
    let chat = msg.chat;
    match msg.event {
        InboundEvent::Text { body } => {
            handle_text(&*messenger, SourceRegistry::global(), &chat, &body).await;
        }
        other => {
            // Actions/documents aren't part of the slice yet — nudge the user
            // toward the one thing WhatsApp supports today: send a link.
            log::debug!("whatsapp: ignoring non-text inbound event: {other:?}");
            let _ = messenger.send(&chat, hint_message()).await;
        }
    }
}

/// Core slice logic, factored out of [`handle_inbound`] so it can be unit-tested
/// with a fake [`Messenger`] and a [`SourceRegistry`] carrying a mock source.
async fn handle_text(messenger: &dyn Messenger, registry: &SourceRegistry, chat: &ChatRef, body: &str) {
    let Some(url) = extract_url(body) else {
        let _ = messenger.send(chat, hint_message()).await;
        return;
    };

    let Some(source) = registry.resolve(&url) else {
        let _ = messenger
            .send(chat, text_msg("Unsupported link — I can't download from that site."))
            .await;
        return;
    };

    // Bound concurrent downloads; if the semaphore is closed we're shutting down.
    let _permit = match DOWNLOAD_SEM.acquire().await {
        Ok(p) => p,
        Err(_) => return,
    };

    let _ = messenger.send(chat, text_msg("⏳ Downloading…")).await;

    let max_bytes = Capabilities::WHATSAPP.max_audio_bytes;
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let output_path = format!("{}/wa_{seq}.mp3", *crate::core::config::DOWNLOAD_FOLDER);

    let request = DownloadRequest {
        url: url.clone(),
        output_path,
        format: "mp3".to_string(),
        audio_bitrate: Some("320k".to_string()),
        video_quality: None,
        max_file_size: Some(max_bytes),
        time_range: None,
        carousel_mask: None,
        concurrent_fragments: 1,
        quality_preset: None,
        cancel_flag: None,
        experimental_fast_encode: false,
    };

    // Drain progress events — no in-message progress UI in the slice.
    let (ptx, mut prx) = mpsc::unbounded_channel::<SourceProgress>();
    tokio::spawn(async move { while prx.recv().await.is_some() {} });

    match source.download(&request, ptx).await {
        Ok(out) => {
            if out.file_size > max_bytes {
                let _ = messenger
                    .send(
                        chat,
                        text_msg("That file is larger than WhatsApp's 16 MB limit — I can't deliver it here."),
                    )
                    .await;
                crate::download::pipeline::schedule_cleanup(out.file_path);
                return;
            }

            let media = OutboundMessage::Media {
                kind: MediaKind::Audio,
                source: MediaSource::LocalPath(out.file_path.clone()),
                caption: None,
                style: TextStyle::Markdown,
                keyboard: None,
            };
            if let Err(e) = messenger.send(chat, media).await {
                log::warn!("whatsapp: media send failed: {e}");
                let _ = messenger
                    .send(chat, text_msg("Downloaded, but sending it back failed. Try again?"))
                    .await;
            }
            crate::download::pipeline::schedule_cleanup(out.file_path);
        }
        Err(e) => {
            log::warn!("whatsapp: download failed for {url}: {e}");
            let _ = messenger
                .send(
                    chat,
                    text_msg("Download failed — the link might be private, unsupported, or too large."),
                )
                .await;
        }
    }
}

/// First `http`/`https` URL in a whitespace-tokenized body, if any.
fn extract_url(body: &str) -> Option<Url> {
    body.split_whitespace().find_map(|tok| {
        let u = Url::parse(tok).ok()?;
        matches!(u.scheme(), "http" | "https").then_some(u)
    })
}

fn text_msg(body: impl Into<String>) -> OutboundMessage {
    OutboundMessage::Text {
        body: body.into(),
        style: TextStyle::Markdown,
        keyboard: None,
    }
}

fn hint_message() -> OutboundMessage {
    text_msg("Send me a link (YouTube, etc.) and I'll send back the audio 🎧")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use doracore::core::error::AppError;
    use doracore::download::source::{DownloadOutput, DownloadSource, MediaMetadata};
    use doracore::messaging::Keyboard;
    use doracore::messaging::types::{MessageHandle, Platform};

    /// Records every outbound send instead of hitting the network.
    struct RecordingMessenger {
        caps: Capabilities,
        sent: Arc<Mutex<Vec<OutboundMessage>>>,
    }

    impl RecordingMessenger {
        fn new() -> Self {
            Self {
                caps: Capabilities::WHATSAPP,
                sent: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Messenger for RecordingMessenger {
        fn capabilities(&self) -> &Capabilities {
            &self.caps
        }
        async fn send(&self, _chat: &ChatRef, message: OutboundMessage) -> anyhow::Result<MessageHandle> {
            self.sent.lock().unwrap().push(message);
            Ok(MessageHandle {
                platform: Platform::WhatsApp,
                chat_id: "test".into(),
                message_id: "wamid.TEST".into(),
            })
        }
        async fn edit_text(
            &self,
            handle: &MessageHandle,
            _body: String,
            _style: TextStyle,
            _keyboard: Option<Keyboard>,
        ) -> anyhow::Result<MessageHandle> {
            Ok(handle.clone())
        }
        async fn delete(&self, _handle: &MessageHandle) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A download source that writes a tiny file to a temp path and reports it,
    /// so the slice can be exercised without yt-dlp or the network.
    struct FakeSource {
        file_size: u64,
    }

    #[async_trait]
    impl DownloadSource for FakeSource {
        fn name(&self) -> &str {
            "fake"
        }
        fn supports_url(&self, url: &Url) -> bool {
            url.as_str().contains("youtu.be")
        }
        async fn get_metadata(&self, _url: &Url) -> Result<MediaMetadata, AppError> {
            Ok(MediaMetadata {
                title: "Fake".into(),
                artist: "Fake".into(),
            })
        }
        async fn estimate_size(&self, _url: &Url) -> Option<u64> {
            Some(self.file_size)
        }
        async fn is_livestream(&self, _url: &Url) -> bool {
            false
        }
        async fn download(
            &self,
            request: &DownloadRequest,
            _progress_tx: mpsc::UnboundedSender<SourceProgress>,
        ) -> Result<DownloadOutput, AppError> {
            let path = std::env::temp_dir().join(format!("wa_fake_{}.mp3", SEQ.fetch_add(1, Ordering::Relaxed)));
            std::fs::write(&path, b"id3-ish-bytes").ok();
            let _ = request; // output_path unused by the fake
            Ok(DownloadOutput {
                file_path: path.to_string_lossy().into_owned(),
                duration_secs: Some(1),
                file_size: self.file_size,
                mime_hint: Some("audio/mpeg".into()),
                additional_files: None,
            })
        }
    }

    fn chat() -> ChatRef {
        ChatRef {
            platform: Platform::WhatsApp,
            chat_id: "49151".into(),
        }
    }

    fn registry_with(source: FakeSource) -> SourceRegistry {
        let mut reg = SourceRegistry::new();
        reg.register(Arc::new(source));
        reg
    }

    #[tokio::test]
    async fn link_triggers_download_then_media_send() {
        let m = RecordingMessenger::new();
        let sent = Arc::clone(&m.sent);
        let reg = registry_with(FakeSource { file_size: 1_000 });

        handle_text(&m, &reg, &chat(), "grab this https://youtu.be/abc please").await;

        let sent = sent.lock().unwrap();
        // Ack text, then the audio media.
        assert!(sent.iter().any(|o| matches!(o, OutboundMessage::Text { .. })));
        assert!(
            sent.iter().any(|o| matches!(
                o,
                OutboundMessage::Media {
                    kind: MediaKind::Audio,
                    source: MediaSource::LocalPath(_),
                    ..
                }
            )),
            "expected an audio media send, got {sent:?}"
        );
    }

    #[tokio::test]
    async fn oversize_file_is_refused_not_sent() {
        let m = RecordingMessenger::new();
        let sent = Arc::clone(&m.sent);
        // One byte over the WhatsApp audio ceiling.
        let reg = registry_with(FakeSource {
            file_size: Capabilities::WHATSAPP.max_audio_bytes + 1,
        });

        handle_text(&m, &reg, &chat(), "https://youtu.be/big").await;

        let sent = sent.lock().unwrap();
        assert!(
            !sent.iter().any(|o| matches!(o, OutboundMessage::Media { .. })),
            "oversize file must not be sent as media"
        );
    }

    #[tokio::test]
    async fn unsupported_link_replies_with_text() {
        let m = RecordingMessenger::new();
        let sent = Arc::clone(&m.sent);
        // Registry has no source matching this host.
        let reg = registry_with(FakeSource { file_size: 1 });

        handle_text(&m, &reg, &chat(), "https://example.com/whatever").await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(matches!(sent[0], OutboundMessage::Text { .. }));
    }

    #[test]
    fn extract_url_finds_first_http() {
        assert!(extract_url("hello https://youtu.be/x world").is_some());
        assert!(extract_url("no link here").is_none());
        assert!(extract_url("ftp://nope.example").is_none());
    }
}
