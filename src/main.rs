use futures_util::{SinkExt, StreamExt};
use prost::Message;
use cockatiel_client::{proto::container::Payload, proto::*, CockatielClient, PromptKind};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TwitchAdapterConfig {
    channel: Option<String>,
    oauth_token: Option<String>,
    username: Option<String>,
    client_id: Option<String>,
}

/// Parses a Twitch IRC PRIVMSG line to extract the display name and message text
fn parse_twitch_privmsg(line: &str) -> Option<(String, String)> {
    if !line.contains("PRIVMSG") {
        return None;
    }

    let mut display_name = "Unknown".to_string();
    if line.starts_with('@') {
        if let Some(tags_end) = line.find(' ') {
            let tags = &line[1..tags_end];
            for tag in tags.split(';') {
                if tag.starts_with("display-name=") {
                    let name = &tag["display-name=".len()..];
                    if !name.is_empty() {
                        display_name = name.to_string();
                    }
                }
            }
        }
    }

    if display_name == "Unknown" {
        if let Some(nick_end) = line.find('!') {
            if let Some(nick_start) = line.find(':') {
                if nick_start < nick_end {
                    display_name = line[nick_start + 1..nick_end].to_string();
                }
            }
        }
    }

    if let Some(msg_idx) = line.find(" PRIVMSG ") {
        let remainder = &line[msg_idx + 9..];
        if let Some(colon_idx) = remainder.find(':') {
            let message_text = remainder[colon_idx + 1..].trim().to_string();
            return Some((display_name, message_text));
        }
    }

    None
}

/// Parse a moderator command (!ban / !timeout) from a chat message.
/// Returns (query_id, payload_json) if it matches.
fn parse_mod_command(message: &str, author: &str) -> Option<(String, serde_json::Value)> {
    let trimmed = message.trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("!ban") {
        let args = trimmed[5..].trim();
        let (target, rest) = match args.split_once(char::is_whitespace) {
            Some((t, r)) => (t, r),
            None => (args, ""),
        };
        let target = target.trim_start_matches('@').to_string();
        if target.is_empty() {
            return None;
        }
        let reason = rest.trim().to_string();
        return Some((
            "mod_ban".to_string(),
            serde_json::json!({
                "platform": "twitch",
                "handle": target,
                "reason": reason,
                "actor": { "platform": "twitch", "handle": author },
            }),
        ));
    }

    if lower.starts_with("!timeout") {
        let args = trimmed[9..].trim();
        let mut parts = args.split_whitespace();
        let target = parts.next().unwrap_or("").trim_start_matches('@').to_string();
        if target.is_empty() {
            return None;
        }
        let mut duration_secs = 300i64;
        let mut reason = String::new();
        if let Some(d) = parts.next() {
            if let Ok(secs) = d.parse::<i64>() {
                duration_secs = secs;
            } else {
                reason = d.to_string();
            }
        }
        let rest: Vec<&str> = parts.collect();
        if !rest.is_empty() {
            if !reason.is_empty() {
                reason = format!("{} {}", reason, rest.join(" "));
            } else {
                reason = rest.join(" ");
            }
        }
        return Some((
            "mod_timeout".to_string(),
            serde_json::json!({
                "platform": "twitch",
                "handle": target,
                "duration_secs": duration_secs,
                "reason": reason,
                "actor": { "platform": "twitch", "handle": author },
            }),
        ));
    }

    None
}

/// Loopback port for the OAuth redirect listener. Read from the top-level
/// `oauth_redirect_port` key in config.json (default 3000, so existing
/// platform-console registrations keep working).
fn load_oauth_redirect_port() -> u16 {
    std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|v| v.get("oauth_redirect_port").cloned())
        .and_then(|p| p.as_u64())
        .filter(|p| (1..=u16::MAX as u64).contains(p))
        .map(|p| p as u16)
        .unwrap_or(3000)
}

fn load_adapter_config() -> Option<TwitchAdapterConfig> {
    // Secrets live in `.env` (loaded into env at startup); `channel` and
    // `username` are public settings, read from config.json below.
    let saved = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|v| v.get("module_specific").cloned())
        .unwrap_or_else(|| serde_json::json!({}));
    let channel = saved.get("channel").cloned().and_then(|c| serde_json::from_value(c).ok());
    let username = saved
        .get("username")
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("TWITCH_USERNAME").ok().filter(|s| !s.is_empty()));

    Some(TwitchAdapterConfig {
        channel,
        oauth_token: Some(std::env::var("TWITCH_OAUTH_TOKEN").unwrap_or_default()),
        username,
        client_id: Some(std::env::var("TWITCH_CLIENT_ID").unwrap_or_default()),
    })
    .filter(|c| {
        !c.oauth_token.as_deref().unwrap_or("").is_empty()
            && !c.client_id.as_deref().unwrap_or("").is_empty()
    })
}

fn save_adapter_config(channel: &str, oauth_token: &str, username: &str, client_id: &str) {
    // Secrets live in `.env`; `channel`/`username` are public settings that
    // stay in config.json.
    cockatiel_client::write_env_file(
        ".env",
        &[
            ("TWITCH_OAUTH_TOKEN", oauth_token),
            ("TWITCH_CLIENT_ID", client_id),
        ],
    );
    let path = PathBuf::from("config.json");
    if let Ok(data) = std::fs::read_to_string(&path) {
        if let Ok(mut json_val) = serde_json::from_str::<serde_json::Value>(&data) {
            json_val["module_specific"] = json!({ "channel": channel, "username": username });
            if let Ok(pretty) = serde_json::to_string_pretty(&json_val) {
                let _ = std::fs::write(&path, pretty);
                info!("Successfully saved Twitch configuration (secrets → .env)");
            }
        }
    }
}

/// Automatically queries Twitch's /validate endpoint to get the exact lowercase username
async fn fetch_twitch_username(raw_token: &str) -> Option<String> {
    let client = reqwest::Client::new();
    let res = client
        .get("https://id.twitch.tv/oauth2/validate")
        .header("Authorization", format!("OAuth {}", raw_token))
        .send()
        .await
        .ok()?;

    if res.status().is_success() {
        let json: serde_json::Value = res.json().await.ok()?;
        json.get("login")?.as_str().map(|s| s.to_string())
    } else {
        None
    }
}

async fn capture_oauth_token_concurrent(
    write_ws: &mut WsWriteHalf,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    client_id: &str,
    oauth_redirect_port: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", oauth_redirect_port)).await?;
    let redirect_uri = format!("http://localhost:{}", oauth_redirect_port);
    let auth_url = format!(
        "https://id.twitch.tv/oauth2/authorize?client_id={}&redirect_uri={}&response_type=token&scope=chat:read+chat:edit",
        client_id, redirect_uri
    );

    println!("\n--------------------------------------------------");
    println!("  Opening browser for official Twitch authorization...");
    println!("  Ensure your Redirect URI in the Twitch Developer");
    println!("  Console is set to exactly: {}", redirect_uri);
    println!("--------------------------------------------------\n");

    let _ = open::that(&auth_url);

    // Future 1: TCP Server running with fragment-capturing JS
    let tcp_fut = async {
        let (mut socket, _) = listener.accept().await?;
        let mut buf = [0; 4096];
        let n = socket.read(&mut buf).await?;
        let request = String::from_utf8_lossy(&buf[..n]);

        if request.contains("GET /callback?token=") {
            let token_start = request.find("GET /callback?token=").unwrap() + 20;
            let space_idx = request[token_start..]
                .find(' ')
                .unwrap_or(request[token_start..].len());
            let token = request[token_start..token_start + space_idx].to_string();

            let response_html = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n\
            <html><body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'>\
            <h1 style='color:#a970ff;'>Twitch authentication successful!</h1>\
            <p>You can close this window and return to your terminal.</p>\
            </body></html>";
            let _ = socket.write_all(response_html.as_bytes()).await;
            Ok(token)
        } else if request.contains("error=") {
            let response_html = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n\
            <html><body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'>\
            <h1 style='color:#ff4f4f;'>Twitch Redirect Error (Mismatch)</h1>\
            <p>Please check that your Redirect URI in the Twitch Developer Console is set to exactly <b>{}</b>.</p>\
            </body></html>", redirect_uri);
            let _ = socket.write_all(response_html.as_bytes()).await;
            Err(format!("Twitch returned a redirect_mismatch error. Make sure {} is added under Redirect URIs in your Twitch Console app settings.", redirect_uri).into())
        } else {
            // Serve landing page with JS to extract window.location.hash and forward to /callback
            let landing_html = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n\
            <html>\
            <script>\
                if (window.location.hash) {\
                    const hashParams = new URLSearchParams(window.location.hash.substring(1));\
                    const token = hashParams.get('access_token');\
                    if (token) {\
                        window.location.href = '/callback?token=' + token;\
                    }\
                }\
            </script>\
            <body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'>\
            <h1 style='color:#a970ff;'>Authenticating with Twitch...</h1>\
            <p>Processing authorization token...</p>\
            </body></html>";
            let _ = socket.write_all(landing_html.as_bytes()).await;

            let (mut socket2, _) = listener.accept().await?;
            let mut buf2 = [0; 4096];
            let n2 = socket2.read(&mut buf2).await?;
            let request2 = String::from_utf8_lossy(&buf2[..n2]);

            if request2.contains("GET /callback?token=") {
                let token_start = request2.find("GET /callback?token=").unwrap() + 20;
                let space_idx = request2[token_start..]
                    .find(' ')
                    .unwrap_or(request2[token_start..].len());
                let token = request2[token_start..token_start + space_idx].to_string();

                let success_html = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n\
                <html><body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'>\
                <h1 style='color:#a970ff;'>Twitch authentication successful!</h1>\
                <p>You can close this window and return to your terminal.</p>\
                </body></html>";
                let _ = socket2.write_all(success_html.as_bytes()).await;
                Ok(token)
            } else {
                Err("Failed to capture token from browser redirect flow.".into())
            }
        }
    };

    // Future 2: Engine prompt fallback — ask the operator to paste the full
    // redirect URL if the automatic browser+localhost capture didn't complete.
    let prompt_fut = prompt_for_input(
        write_ws,
        prompt_rx,
        auth_token,
        module_name,
        instance_uuid,
        "Twitch OAuth Authorization",
        "Opening the browser for official Twitch authorization...\n\n\
         If the automatic browser flow already succeeded, no input is needed.\n\
         Otherwise, paste the full redirect URL from your browser's address bar.",
        "Paste full redirect URL",
        PromptKind::Credential,
        90,
    );

    tokio::select! {
        res = tcp_fut => res,
        prompt_res = prompt_fut => {
            match prompt_res {
                Some(input) => {
                    let m_trimmed = input.trim();
                    if m_trimmed.contains("error=") {
                        return Err(format!("Twitch auth error detected. Ensure your Redirect URI in the Twitch Console is set strictly to '{}'. Details: {}", redirect_uri, m_trimmed).into());
                    }

                    let raw_token = if let Some(idx) = m_trimmed.find("access_token=") {
                        let start = idx + 13;
                        let end = m_trimmed[start..].find('&').map(|i| i + start).unwrap_or(m_trimmed.len());
                        m_trimmed[start..end].to_string()
                    } else if let Some(idx) = m_trimmed.find("token=") {
                        let start = idx + 6;
                        let end = m_trimmed[start..].find('&').map(|i| i + start).unwrap_or(m_trimmed.len());
                        m_trimmed[start..end].to_string()
                    } else {
                        m_trimmed.to_string()
                    };

                    if raw_token.is_empty() {
                        Err("Empty token provided".into())
                    } else {
                        Ok(raw_token)
                    }
                }
                None => Err("Token capture failed: browser flow timed out and no redirect URL was provided".into()),
            }
        }
    }
}

/// Send a Prompt to the engine (forwarded to connected UIs) and wait for the
/// operator's response (`PromptResponse.reason`). Returns None on cancel/timeout.
async fn prompt_for_input(
    write_ws: &mut WsWriteHalf,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    title: &str,
    details: &str,
    input_label: &str,
    kind: PromptKind,
    timeout: u32,
) -> Option<String> {
    let prompt_id = uuid::Uuid::now_v7().to_string();
    let prompt_type = match kind {
        PromptKind::Boolean => PromptType::Boolean,
        PromptKind::String => PromptType::String,
        PromptKind::Credential => PromptType::Credential,
    };
    let prompt = Prompt {
        prompt_id_uuid7: prompt_id.clone(),
        prompt: title.to_string(),
        details: details.to_string(),
        yes_dialog: "Submit".to_string(),
        no_dialog: "Cancel".to_string(),
        timeout,
        origin: module_name.to_string(),
        origin_uuid7: String::new(),
        instructions: String::new(),
        link: String::new(),
        input_label: input_label.to_string(),
        prompt_type: prompt_type as i32,
    };
    let container = Container {
        version: 1,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(Payload::Prompt(prompt)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_err() {
        return None;
    }
    if write_ws.send(WsMessage::Binary(buf.into())).await.is_err() {
        return None;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64 + 10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), prompt_rx.recv()).await {
            Ok(Some(resp)) if resp.prompt_id_uuid7 == prompt_id => {
                return if resp.accepted {
                    Some(resp.reason)
                } else {
                    None
                };
            }
            Ok(Some(_)) => continue, // a different prompt's response
            Ok(None) => return None,
            // The 10s poll interval elapsed with no response yet: keep waiting
            // until the real deadline (the `timeout` seconds above), rather than
            // bailing out 10 seconds in and auto-cancelling every prompt.
            Err(_) => continue,
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("Starting Twitch Adapter Module...");

    let cockatiel = CockatielClient::connect("config.json").await?;

    // Split the stream for concurrent read/write
    let (write_ws_cockatiel, mut read_ws_cockatiel) = cockatiel.stream.split();
    let write_ws_cockatiel = Arc::new(tokio::sync::Mutex::new(write_ws_cockatiel));
    let auth_token = cockatiel.auth_token.clone();
    let instance_uuid = cockatiel.instance_uuid7.clone();
    let module_name = cockatiel.config.module_name.clone();
    let oauth_redirect_port = load_oauth_redirect_port();

    // Channel for outbound SendToPlatforms messages → Twitch IRC writer.
    let (send_outbound_tx, mut send_outbound_rx) = tokio::sync::mpsc::channel::<String>(64);

    // Channel carrying PromptResponses from the engine to the configure loop,
    // so `prompt_for_input` can await the operator's typed answer.
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptResponse>();
    let prompt_tx_task = prompt_tx.clone();
    let write_task = write_ws_cockatiel.clone();
    let auth_task = auth_token.clone();
    let module_task = module_name.clone();
    let instance_task = instance_uuid.clone();

    tokio::spawn(async move {
        while let Some(msg) = read_ws_cockatiel.next().await {
            match msg {
                Ok(WsMessage::Binary(data)) => {
                    if let Ok(container) = cockatiel_client::proto::Container::decode(data.as_ref()) {
                        info!("Received from engine: {:?}", container.payload.as_ref().map(|p| std::mem::discriminant(p)));

                        // Answer the engine's liveness probe with our auth token
                        // so a quiet period never severs us.
                        if let Some(Payload::AuthVerify(_)) = container.payload {
                            let reply = Container {
                                version: 1,
                                auth_token: auth_task.clone(),
                                module_name: module_task.clone(),
                                module_instance_uuid7: instance_task.clone(),
                                payload: Some(Payload::AuthVerify(AuthVerify {
                                    cur_auth: auth_task.clone(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = write_task.lock().await;
                                let _ = w.send(WsMessage::Binary(buf.into())).await;
                            }
                        }
                        // Handle outbound SendToPlatforms: forward the message to
                        // the Twitch IRC writer over a channel.
                        else if let Some(Payload::SendToPlatforms(send)) = container.payload {
                            if let Err(e) = send_outbound_tx.send(send.msg.clone()).await {
                                error!("Twitch outbound channel closed; dropping message: {}", e);
                            }
                        } else if let Some(Payload::PromptResponse(resp)) = container.payload {
                            // Forward operator answers to the awaiting prompt.
                            let _ = prompt_tx_task.send(resp);
                        }
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    info!("Engine closed connection");
                    break;
                }
                Err(e) => {
                    error!("Engine WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Re-acquire credentials whenever Twitch rejects them (bad channel/oauth).
    // Env vars are read once; on rejection the locals are cleared so the
    // prompt path runs and asks for fresh, valid credentials.
    cockatiel_client::load_env_file(".env");
    // `channel`/`username` are public → config.json; oauth/client_id are
    // secrets → .env (env vars).
    let env_channel = std::env::var("TWITCH_CHANNEL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("config.json")
                .ok()
                .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
                .and_then(|v| v.get("module_specific").cloned())
                .and_then(|s| s.get("channel").cloned())
                .and_then(|c| c.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_default();
    let env_oauth = std::env::var("TWITCH_OAUTH_TOKEN").unwrap_or_default();
    let env_username = std::env::var("TWITCH_USERNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("config.json")
                .ok()
                .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
                .and_then(|v| v.get("module_specific").cloned())
                .and_then(|s| s.get("username").cloned())
                .and_then(|u| u.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_default();
    let env_client_id = std::env::var("TWITCH_CLIENT_ID").unwrap_or_default();

    // Set when Twitch rejects the saved credentials; skips the saved-config
    // fast paths so the module prompts the operator for fresh credentials via
    // the prompt subwindow instead of silently looping on the bad saved config.
    let mut force_prompt = false;

    'configure: loop {
        let mut channel = env_channel.clone();
        let mut oauth_token = env_oauth.clone();
        let mut username = env_username.clone();
        let mut client_id = env_client_id.clone();

        // Non-interactive fast path: if a saved config exists, use it without
        // prompting (enables the TUI to supply credentials via file). Just the
        // channel is enough to proceed — a missing oauth token is acquired via
        // the browser flow below, and a missing client_id falls back to
        // anonymous read-only chat.
        if !force_prompt && channel.is_empty() && oauth_token.is_empty() && username.is_empty() && client_id.is_empty() {
            if let Some(saved) = load_adapter_config() {
                let saved_channel = saved.channel.unwrap_or_default();
                let saved_oauth = saved.oauth_token.unwrap_or_default();
                let saved_user = saved.username.unwrap_or_default();
                let saved_cid = saved.client_id.unwrap_or_default();
                if !saved_channel.is_empty() {
                    info!(
                        "Using saved Twitch configuration for channel '{}' (no prompt).",
                        saved_channel
                    );
                    channel = saved_channel;
                    oauth_token = saved_oauth;
                    username = saved_user;
                    client_id = saved_cid;
                }
            }
        }

        if channel.is_empty() {
        if !force_prompt {
            if let Some(saved) = load_adapter_config() {
            if let Some(saved_chan) = saved.channel {
                let confirm = prompt_for_input(
                    &mut *write_ws_cockatiel.lock().await,
                    &mut prompt_rx,
                    &auth_token,
                    &module_name,
                    &instance_uuid,
                    "Use Saved Twitch Configuration?",
                    &format!(
                        "A saved Twitch configuration was found for channel '{}'.\n\n\
                         Connect to this existing channel, or set up a new stream?",
                        saved_chan
                    ),
                    "",
                    PromptKind::Boolean,
                    120,
                )
                .await;
                if let Some(choice) = confirm {
                    if choice.trim().eq_ignore_ascii_case("y")
                        || choice.trim().eq_ignore_ascii_case("yes")
                    {
                        channel = saved_chan;
                        oauth_token = saved.oauth_token.unwrap_or_default();
                        username = saved.username.unwrap_or_default();
                        client_id = saved.client_id.unwrap_or_default();
                    }
                }
            }
        }
        }
    }

    if channel.is_empty() {
        let input = prompt_for_input(
            &mut *write_ws_cockatiel.lock().await,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
            "Twitch Live Chat Configuration Required",
            "Enter the Twitch channel name or stream link to connect to.",
            "Twitch channel name or stream link",
            PromptKind::String,
            300,
        )
        .await;
        if let Some(val) = input {
            let trimmed = val.trim();
            if let Some(idx) = trimmed.find("twitch.tv/") {
                channel = trimmed[idx + 10..]
                    .split('/')
                    .next()
                    .unwrap_or("")
                    .to_string();
            } else {
                channel = trimmed.to_string();
            }
        } else {
            warn!("No channel provided. Re-prompting...");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue 'configure;
        }

        if oauth_token.is_empty() {
            let client_id_input = prompt_for_input(
                &mut *write_ws_cockatiel.lock().await,
                &mut prompt_rx,
                &auth_token,
                &module_name,
                &instance_uuid,
                "Twitch Client ID Required",
                &format!("Paste your Twitch Client ID (or press Enter for anonymous read-only).\n\n\
                 To create one:\n\
                 1. Go to https://dev.twitch.tv/console/apps\n\
                 2. Click 'Register Your Application'.\n\
                 3. Settings:\n\
                    - Name: tiel-bot (nsfw words are banned)\n\
                    - OAuth Redirect URI: http://localhost:{}\n\
                    - Set the client type to public\n\
                    - Category: Chat Bot\n\
                 4. Click 'Create' and copy your Client ID.", oauth_redirect_port),
                "Twitch Client ID (or blank for anonymous)",
                PromptKind::String,
                300,
            )
            .await;
            client_id = client_id_input.unwrap_or_default().trim().to_string();

            if !client_id.is_empty() {
                match capture_oauth_token_concurrent(
                    &mut *write_ws_cockatiel.lock().await,
                    &mut prompt_rx,
                    &auth_token,
                    &module_name,
                    &instance_uuid,
                    &client_id,
                    oauth_redirect_port,
                )
                .await
                {
                    Ok(raw_token) => {
                        let clean_token = raw_token.strip_prefix("oauth:").unwrap_or(&raw_token);
                        oauth_token = format!("oauth:{}", clean_token);

                        if let Some(fetched_user) = fetch_twitch_username(clean_token).await {
                            info!(
                                "Successfully verified token and retrieved username: {}",
                                fetched_user
                            );
                            username = fetched_user;
                        } else {
                            info!("Could not auto-fetch username via API, falling back to channel name.");
                            username = channel.clone();
                        }
                        info!("Successfully acquired and configured OAuth token!");
                    }
                    Err(e) => {
                        error!("Token capture failed: {}", e);
                    }
                }
            }
        }

        if username.is_empty() {
            username = if oauth_token.is_empty() {
                let unique_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos()
                    % 10000;
                format!("justinfan{}", unique_id)
            } else {
                channel.clone()
            };
        }

        save_adapter_config(&channel, &oauth_token, &username, &client_id);
    } else if oauth_token.is_empty() && !client_id.is_empty() {
        // Channel is configured but we don't have a token yet — acquire it via
        // the browser OAuth flow using the saved client_id. Bounded by a timeout
        // so it can never hang forever; on failure we fall back to read-only.
        println!("\n  Opening browser to authorize the bot on Twitch...");
        let mut ws_guard = write_ws_cockatiel.lock().await;
        let capture = capture_oauth_token_concurrent(
            &mut *ws_guard,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
            &client_id,
            oauth_redirect_port,
        );
        match tokio::time::timeout(std::time::Duration::from_secs(90), capture).await {
            Ok(Ok(raw_token)) => {
                let clean_token = raw_token.strip_prefix("oauth:").unwrap_or(&raw_token);
                oauth_token = format!("oauth:{}", clean_token);
                if let Some(fetched_user) = fetch_twitch_username(clean_token).await {
                    info!(
                        "Successfully verified token and retrieved username: {}",
                        fetched_user
                    );
                    username = fetched_user;
                } else {
                    username = channel.clone();
                }
                save_adapter_config(&channel, &oauth_token, &username, &client_id);
                info!("Successfully acquired and configured OAuth token!");
            }
            Ok(Err(e)) => {
                error!(
                    "Token capture failed: {}. Connecting in read-only mode.",
                    e
                );
            }
            Err(_) => {
                warn!("OAuth capture timed out after 90s. Connecting in read-only mode.");
            }
        }
    }

    let twitch_ws_url = "wss://irc-ws.chat.twitch.tv:443";

        'reconnect: loop {
            info!("Connecting to Twitch IRC WebSocket at {}...", twitch_ws_url);
            let (ws_stream, _) = match connect_async(twitch_ws_url).await {
                Ok(val) => val,
                Err(e) => {
                    error!(
                        "Failed to connect to Twitch IRC: {}. Retrying in 5 seconds...",
                        e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue 'reconnect;
                }
            };

        let (mut write_ws, mut read_ws) = ws_stream.split();

        let pass = if oauth_token.is_empty() {
            "oauth:123456"
        } else {
            &oauth_token
        };
        let nick = if username.is_empty() {
            "justinfan123"
        } else {
            &username.to_lowercase()
        };

        write_ws
            .send(WsMessage::Text(format!("PASS {}", pass)))
            .await?;
        write_ws
            .send(WsMessage::Text(format!("NICK {}", nick)))
            .await?;
        write_ws
            .send(WsMessage::Text(format!("JOIN #{}", channel.to_lowercase())))
            .await?;
        write_ws
            .send(WsMessage::Text(
                "CAP req :twitch.tv/tags twitch.tv/commands".to_string(),
            ))
            .await?;

        info!(
            "Successfully connected to Twitch channel #{} as nick '{}'!",
            channel, nick
        );

        'irc: loop {
            tokio::select! {
                msg_result = read_ws.next() => {
                    let Some(msg_result) = msg_result else { break 'irc };
                    match msg_result {
                        Ok(WsMessage::Text(text)) => {
                            for line in text.lines() {
                                // Twitch rejects bad oauth/nick — clear creds and re-prompt.
                                if line.contains("Login authentication failed")
                                    || line.contains("Improperly formatted auth")
                                    || line.contains("Login unsuccessful")
                                    || line.contains("authentication failed")
                                {
                                    error!(
                                        "Twitch rejected credentials for channel '{}' ({}). Re-acquiring...",
                                        channel, line
                                    );
                                    force_prompt = true;
                                    channel.clear();
                                    oauth_token.clear();
                                    username.clear();
                                    client_id.clear();
                                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                    continue 'configure;
                                }

                                if line.starts_with("PING") {
                                    let _ = write_ws
                                        .send(WsMessage::Text("PONG :tmi.twitch.tv".to_string()))
                                        .await;
                                    continue;
                                }

                                if let Some((author_name, message_text)) = parse_twitch_privmsg(line) {
                                    info!("[Twitch Chat] {}: {}", author_name, message_text);

                                    let pre_process_msg = MessagePreProcess {
                audio: vec![],
                audio_type: String::new(),
                                        message_uuid7: String::new(),
                                        raw_message: Some(ChatMessage {
                                            platform: "twitch".into(),
                                            raw_data: line.as_bytes().to_vec(),
                                            raw_message: message_text.clone(),
                                            user_uuid7: author_name.to_string(),
                                            command: None,
                                            user_data: None,
                                        }),
                                    };

                                    let container = cockatiel_client::proto::Container {
                                        version: 1,
                                        auth_token: auth_token.clone(),
                                        module_name: module_name.clone(),
                                        module_instance_uuid7: instance_uuid.clone(),
                                        payload: Some(Payload::MessagePreProcess(pre_process_msg)),
                                    };
                                    let mut buf = Vec::new();
                                    use prost::Message;
                                    if container.encode(&mut buf).is_ok() {
                                        if let Err(e) = write_ws_cockatiel.lock().await.send(WsMessage::Binary(buf.into())).await {
                                            error!("Failed to send chat message to Cockatiel engine: {}", e);
                                        }
                                    }

                                    // Handle moderator commands (!ban / !timeout).
                                    if let Some((qid, payload)) = parse_mod_command(&message_text, &author_name) {
                                        info!("Mod command detected: {} target={}", qid, payload);
                                        let query = Container {
                                            version: 1,
                                            auth_token: auth_token.clone(),
                                            module_name: module_name.clone(),
                                            module_instance_uuid7: instance_uuid.clone(),
                                            payload: Some(Payload::DatabaseQuery(DatabaseQuery {
                                                query_id: qid,
                                                sql: payload.to_string(),
                                                params: vec![],
                                            })),
                                        };
                                        let mut qbuf = Vec::new();
                                        if query.encode(&mut qbuf).is_ok() {
                                            if let Err(e) = write_ws_cockatiel.lock().await.send(WsMessage::Binary(qbuf.into())).await {
                                                error!("Failed to send mod command to engine: {}", e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(WsMessage::Close(_)) => {
                            error!("Twitch IRC WebSocket closed connection.");
                            break 'irc;
                        }
                        Err(e) => {
                            error!("Twitch WebSocket error: {}", e);
                            break 'irc;
                        }
                        _ => {}
                    }
                }
                outbound = send_outbound_rx.recv() => {
                    match outbound {
                        Some(msg) => {
                            // Send the outbound message to the Twitch channel as the bot.
                            info!("Sending to Twitch #{}: {}", channel, msg);
                            let safe = msg.replace('\n', " ").replace('\r', " ");
                            if let Err(e) = write_ws
                                .send(WsMessage::Text(format!("PRIVMSG #{} :{}", channel.to_lowercase(), safe)))
                                .await
                            {
                                error!("Failed to send outbound Twitch message: {}", e);
                            }
                        }
                        None => break 'irc,
                    }
                }
            }
        }

        info!("Disconnected from Twitch. Reconnecting in 5 seconds...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}
