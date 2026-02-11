use axum::{
    extract::{ws::Message as AxumMessage, ws::WebSocket, ws::WebSocketUpgrade, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::{future::join_all, SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::ErrorKind,
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tracing::{error, info, warn};

static LOGGED_WS_SAMPLE: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct AppState {
    markets: Arc<Vec<MarketConfig>>,
    http: Client,
    live: Arc<RwLock<LiveState>>,
    broadcaster: broadcast::Sender<Snapshot>,
    book_broadcaster: broadcast::Sender<OrderBookUpdateMessage>,
    paper: Arc<RwLock<PaperState>>,
}

#[derive(Debug, Default, Clone)]
struct LiveState {
    latest: HashMap<String, MarketSnapshot>,
    token_map: HashMap<String, TokenMapping>,
    token_set: HashSet<String>,
    orderbooks: HashMap<String, MarketOrderBook>,
}

impl LiveState {
    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Copy)]
enum BookOutcome {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct BookLevel {
    key: String,
    price: f64,
    size: f64,
}

#[derive(Debug, Clone, Default)]
struct BookSide {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

#[derive(Debug, Clone)]
struct MarketOrderBook {
    label: String,
    slug: String,
    event_title: Option<String>,
    window: Option<WindowInfo>,
    up: BookSide,
    down: BookSide,
}

#[derive(Debug, Clone)]
struct TokenMapping {
    label: String,
    slug: String,
    window: Option<WindowInfo>,
    event_title: Option<String>,
    outcome: OutcomeSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutcomeSide {
    Up,
    Down,
}

#[derive(Debug, Deserialize, Clone)]
struct MarketsConfigFile {
    markets: Vec<MarketConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct MarketConfig {
    label: String,
    slug: Option<String>,
    slug_prefix: Option<String>,
    window_minutes: Option<u64>,
    yes_token_id: Option<String>,
    no_token_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedMarket {
    slug: String,
    window: Option<WindowInfo>,
}

#[derive(Debug, Default, Clone, Copy)]
struct PriceHint {
    last_trade: Option<f64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

impl PriceHint {
    fn derive_yes(self) -> Option<f64> {
        if let Some(last) = self.last_trade {
            return Some(last);
        }
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            (Some(bid), None) => Some(bid),
            (None, Some(ask)) => Some(ask),
            _ => None,
        }
    }

    fn derive_pair(self) -> (Option<f64>, Option<f64>) {
        let up = self.derive_yes();
        let down = up.map(|y| (1.0 - y).clamp(0.0, 1.0));
        (up, down)
    }
}

#[derive(Debug, Default, Clone)]
struct EventMetadata {
    title: Option<String>,
    up_token_id: Option<String>,
    down_token_id: Option<String>,
    price_hint: PriceHint,
}

#[derive(Debug, Serialize, Clone)]
struct Snapshot {
    ts: i64,
    markets: Vec<MarketSnapshot>,
}

#[derive(Debug, Serialize, Clone)]
struct MarketSnapshot {
    label: String,
    slug: String,
    event_title: Option<String>,
    window: Option<WindowInfo>,
    price: PriceInfo,
    source: String,
}

#[derive(Debug, Serialize, Clone)]
struct WindowInfo {
    start_ts: i64,
    end_ts: i64,
    window_minutes: u64,
}

#[derive(Debug, Serialize, Clone)]
struct PriceInfo {
    up: Option<f64>,
    down: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
struct OrderBookSnapshot {
    ts: i64,
    markets: Vec<MarketOrderBookSnapshot>,
}

#[derive(Debug, Serialize, Clone)]
struct MarketOrderBookSnapshot {
    label: String,
    slug: String,
    event_title: Option<String>,
    window: Option<WindowInfo>,
    up: OrderBookSideSnapshot,
    down: OrderBookSideSnapshot,
}

#[derive(Debug, Serialize, Clone, Default)]
struct OrderBookSideSnapshot {
    bids: Vec<OrderBookLevelSnapshot>,
    asks: Vec<OrderBookLevelSnapshot>,
}

#[derive(Debug, Serialize, Clone)]
struct OrderBookLevelSnapshot {
    price: f64,
    size: f64,
}

#[derive(Debug, Serialize, Clone)]
struct OrderBookUpdateMessage {
    message_type: String,
    ts: i64,
    label: String,
    slug: String,
    event_title: Option<String>,
    window: Option<WindowInfo>,
    outcome: String,
    deltas: OrderBookSideSnapshot,
    book: OrderBookSideSnapshot,
}

#[derive(Debug, Clone)]
struct PaperState {
    markets: HashMap<String, PaperMarketState>,
    per_market_budget: f64,
    paused: bool,
}

impl PaperState {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            per_market_budget: 200.0,
            paused: false,
        }
    }
}

#[derive(Debug, Clone)]
struct PaperMarketState {
    label: String,
    slug: String,
    cash: f64,
    realized_pnl: f64,
    step_index: usize,
    expected_side: Option<OutcomeSide>,
    done: bool,
    open_positions: Vec<PaperPosition>,
    sell_orders: Vec<PaperSellOrder>,
    trades: Vec<PaperTrade>,
}

#[derive(Debug, Clone)]
struct PaperPosition {
    side: OutcomeSide,
    entry_price: f64,
    size: f64,
    notional: f64,
    target_price: f64,
    step: usize,
    order_id: String,
}

#[derive(Debug, Serialize, Clone)]
struct PaperSellOrder {
    id: String,
    ts: i64,
    label: String,
    slug: String,
    side: String,
    price: f64,
    target_price: f64,
    slippage: f64,
    size: f64,
    status: String,
    filled_ts: Option<i64>,
}

#[derive(Debug, Serialize, Clone)]
struct PaperTrade {
    ts: i64,
    label: String,
    slug: String,
    action: String,
    side: String,
    price: f64,
    target_price: f64,
    slippage: f64,
    size: f64,
    notional: f64,
    realized_pnl: f64,
    step: usize,
}

#[derive(Debug, Serialize, Clone)]
struct PaperStateResponse {
    ts: i64,
    paused: bool,
    markets: Vec<PaperMarketReport>,
}

#[derive(Debug, Deserialize)]
struct PauseRequest {
    paused: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PauseResponse {
    paused: bool,
}

#[derive(Debug, Serialize, Clone)]
struct PaperMarketReport {
    label: String,
    slug: String,
    status: String,
    cash: f64,
    realized_pnl: f64,
    step_index: usize,
    open_positions: Vec<PaperPositionReport>,
    sell_orders: Vec<PaperSellOrder>,
    trades: Vec<PaperTrade>,
}

#[derive(Debug, Serialize, Clone)]
struct PaperPositionReport {
    side: String,
    entry_price: f64,
    size: f64,
    notional: f64,
    target_price: f64,
    step: usize,
    order_id: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let markets = Arc::new(load_markets());
    let http = Client::builder()
        .timeout(Duration::from_secs(8))
        .user_agent("cdpm2.0/0.1")
        .build()
        .expect("http client");

    let live = Arc::new(RwLock::new(LiveState::new()));
    let (broadcaster, _) = broadcast::channel(1024);
    let (book_broadcaster, _) = broadcast::channel(2048);
    let paper = Arc::new(RwLock::new(PaperState::new()));
    let state = AppState {
        markets,
        http,
        live,
        broadcaster,
        book_broadcaster,
        paper,
    };

    tokio::spawn(live_ws_task(state.clone()));

    let static_dir = env::var("FRONTEND_DIST").unwrap_or_else(|_| "frontend/dist".into());
    let static_service = ServeDir::new(static_dir.clone())
        .fallback(ServeFile::new(format!("{static_dir}/index.html")));

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/snapshot", get(snapshot_handler))
        .route("/api/orderbooks", get(orderbooks_handler))
        .route("/api/paper/state", get(paper_state_handler))
        .route("/api/paper/pause", axum::routing::post(paper_pause_handler))
        .route("/api/paper/reset", axum::routing::post(paper_reset_handler))
        .route("/ws", get(ws_handler))
        .route("/ws/orderbook", get(ws_orderbook_handler))
        .with_state(state)
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any))
        .fallback_service(static_service);

    let addr = "0.0.0.0:3001";
    info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind port");
    axum::serve(listener, app).await.expect("server");
}

async fn health() -> Json<Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn snapshot_handler(State(state): State<AppState>) -> Json<Snapshot> {
    if let Some(snapshot) = snapshot_from_live(&state).await {
        return Json(snapshot);
    }

    Json(build_snapshot(&state).await)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_loop(socket, state))
}

async fn ws_orderbook_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_orderbook_loop(socket, state))
}

async fn ws_loop(mut socket: WebSocket, state: AppState) {
    let mut rx = state.broadcaster.subscribe();
    let initial = snapshot_from_live_or_fetch(&state).await;
    if let Ok(payload) = serde_json::to_string(&initial) {
        if socket.send(AxumMessage::Text(payload)).await.is_err() {
            return;
        }
    }

    loop {
        match rx.recv().await {
            Ok(snapshot) => {
                if let Ok(payload) = serde_json::to_string(&snapshot) {
                    if socket.send(AxumMessage::Text(payload)).await.is_err() {
                        break;
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

async fn ws_orderbook_loop(mut socket: WebSocket, state: AppState) {
    let mut rx = state.book_broadcaster.subscribe();
    let initial = orderbooks_from_live(&state).await;
    if let Ok(payload) = serde_json::to_string(&initial) {
        if socket.send(AxumMessage::Text(payload)).await.is_err() {
            return;
        }
    }

    loop {
        match rx.recv().await {
            Ok(update) => {
                if let Ok(payload) = serde_json::to_string(&update) {
                    if socket.send(AxumMessage::Text(payload)).await.is_err() {
                        break;
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

async fn build_snapshot(state: &AppState) -> Snapshot {
    let ts = now_ts();
    let tasks = state
        .markets
        .iter()
        .cloned()
        .map(|market| fetch_market_snapshot(state.http.clone(), market, ts));
    let markets = join_all(tasks).await;
    Snapshot { ts, markets }
}

async fn snapshot_from_live(state: &AppState) -> Option<Snapshot> {
    let live = state.live.read().await;
    if live.latest.is_empty() {
        return None;
    }

    let ts = now_ts();
    let mut markets = Vec::with_capacity(state.markets.len());
    for cfg in state.markets.iter() {
        if let Some(snapshot) = live.latest.get(&cfg.label) {
            markets.push(snapshot.clone());
        }
    }

    Some(Snapshot { ts, markets })
}

async fn broadcast_snapshot(state: &AppState) {
    if let Some(snapshot) = snapshot_from_live(state).await {
        let _ = state.broadcaster.send(snapshot);
    }
}

async fn snapshot_from_live_or_fetch(state: &AppState) -> Snapshot {
    if let Some(snapshot) = snapshot_from_live(state).await {
        return snapshot;
    }
    build_snapshot(state).await
}

async fn orderbooks_handler(State(state): State<AppState>) -> Json<OrderBookSnapshot> {
    Json(orderbooks_from_live(&state).await)
}

async fn paper_state_handler(State(state): State<AppState>) -> Json<PaperStateResponse> {
    Json(paper_state_from_live(&state).await)
}

async fn paper_pause_handler(
    State(state): State<AppState>,
    maybe_req: Option<Json<PauseRequest>>,
) -> Json<PauseResponse> {
    let mut paper = state.paper.write().await;
    let next = maybe_req
        .and_then(|payload| payload.paused)
        .unwrap_or(!paper.paused);
    paper.paused = next;
    Json(PauseResponse { paused: paper.paused })
}

async fn paper_reset_handler(State(state): State<AppState>) -> Json<PaperStateResponse> {
    let mut paper = state.paper.write().await;
    *paper = PaperState::new();
    drop(paper);
    Json(paper_state_from_live(&state).await)
}

async fn orderbooks_from_live(state: &AppState) -> OrderBookSnapshot {
    let ts = now_ts();
    let live = state.live.read().await;
    let mut markets = Vec::with_capacity(state.markets.len());
    for cfg in state.markets.iter() {
        if let Some(book) = live.orderbooks.get(&cfg.label) {
            markets.push(MarketOrderBookSnapshot {
                label: book.label.clone(),
                slug: book.slug.clone(),
                event_title: book.event_title.clone(),
                window: book.window.clone(),
                up: snapshot_side(&book.up),
                down: snapshot_side(&book.down),
            });
        }
    }
    OrderBookSnapshot { ts, markets }
}

async fn paper_state_from_live(state: &AppState) -> PaperStateResponse {
    let ts = now_ts();
    let paper = state.paper.read().await;
    let mut markets = Vec::new();
    for market in paper.markets.values() {
        markets.push(PaperMarketReport {
            label: market.label.clone(),
            slug: market.slug.clone(),
            status: if market.done { "done".to_string() } else { "active".to_string() },
            cash: market.cash,
            realized_pnl: market.realized_pnl,
            step_index: market.step_index,
            open_positions: market
                .open_positions
                .iter()
                .map(|pos| PaperPositionReport {
                    side: outcome_label(pos.side).to_string(),
                    entry_price: pos.entry_price,
                    size: pos.size,
                    notional: pos.notional,
                    target_price: pos.target_price,
                    step: pos.step,
                    order_id: pos.order_id.clone(),
                })
                .collect(),
            sell_orders: market.sell_orders.clone(),
            trades: market.trades.clone(),
        });
    }
    PaperStateResponse {
        ts,
        paused: paper.paused,
        markets,
    }
}

fn snapshot_side(side: &BookSide) -> OrderBookSideSnapshot {
    OrderBookSideSnapshot {
        bids: side
            .bids
            .iter()
            .map(|level| OrderBookLevelSnapshot {
                price: level.price,
                size: level.size,
            })
            .collect(),
        asks: side
            .asks
            .iter()
            .map(|level| OrderBookLevelSnapshot {
                price: level.price,
                size: level.size,
            })
            .collect(),
    }
}

async fn fetch_market_snapshot(http: Client, market: MarketConfig, now_ts: i64) -> MarketSnapshot {
    let resolved = resolve_market(&market, now_ts);
    let window = resolved
        .window
        .clone()
        .or_else(|| parse_slug_window(&resolved.slug));
    let event_meta = fetch_event_metadata(&http, &resolved.slug).await;

    let up_token_id = market
        .yes_token_id
        .clone()
        .or(event_meta.up_token_id.clone());
    let down_token_id = market
        .no_token_id
        .clone()
        .or(event_meta.down_token_id.clone());

    let (up_price, down_price, source) = fetch_prices(
        &http,
        up_token_id.as_deref(),
        down_token_id.as_deref(),
        event_meta.price_hint,
    )
    .await;

    MarketSnapshot {
        label: market.label,
        slug: resolved.slug,
        event_title: event_meta.title,
        window,
        price: PriceInfo {
            up: up_price,
            down: down_price,
        },
        source,
    }
}

fn resolve_market(market: &MarketConfig, now_ts: i64) -> ResolvedMarket {
    if let Some(prefix) = &market.slug_prefix {
        let window_minutes = market.window_minutes.unwrap_or(15);
        let epoch_end = align_epoch_end(now_ts, window_minutes);
        // Polymarket uses the START timestamp in slugs, not the end
        let epoch_start = epoch_end - (window_minutes as i64 * 60);
        let slug = format!("{prefix}-{epoch_start}");
        let window = Some(window_from_epoch(epoch_end, window_minutes));
        return ResolvedMarket { slug, window };
    }

    if let Some(slug) = &market.slug {
        let window = parse_slug_window(slug);
        return ResolvedMarket {
            slug: slug.clone(),
            window,
        };
    }

    let window_minutes = market.window_minutes.unwrap_or(15);
    let epoch_end = align_epoch_end(now_ts, window_minutes);
    let epoch_start = epoch_end - (window_minutes as i64 * 60);
    let fallback_prefix = fallback_slug_prefix(&market.label);
    let slug = format!("{fallback_prefix}-{epoch_start}");
    let window = Some(window_from_epoch(epoch_end, window_minutes));
    error!(
        "market '{}' missing slug configuration, using fallback {}",
        market.label, slug
    );
    ResolvedMarket { slug, window }
}

fn align_epoch_end(now_ts: i64, window_minutes: u64) -> i64 {
    let window_seconds = (window_minutes as i64).saturating_mul(60);
    if window_seconds == 0 {
        return now_ts;
    }
    let remainder = ((now_ts % window_seconds) + window_seconds) % window_seconds;
    if remainder == 0 {
        // We're exactly at a boundary, use the next window
        now_ts + window_seconds
    } else {
        // We're in the middle of a window, return the end of the CURRENT window (not previous)
        now_ts - remainder + window_seconds
    }
}

fn window_from_epoch(epoch_end: i64, window_minutes: u64) -> WindowInfo {
    let window_seconds = (window_minutes as i64).saturating_mul(60);
    let start_ts = epoch_end - window_seconds;
    WindowInfo {
        start_ts,
        end_ts: epoch_end,
        window_minutes,
    }
}

fn fallback_slug_prefix(label: &str) -> String {
    let mut result = String::new();
    let mut prev_dash = false;
    for ch in label.chars() {
        if ch.is_ascii_alphanumeric() {
            result.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            result.push('-');
            prev_dash = true;
        }
    }
    let sanitized = result.trim_matches('-');
    if sanitized.is_empty() {
        "market".into()
    } else {
        sanitized.replace("--", "-")
    }
}

async fn live_ws_task(state: AppState) {
    let mut backoff = Duration::from_secs(2);
    loop {
        let token_set = refresh_market_metadata(&state).await;
        if token_set.is_empty() {
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        }

        let urls = clob_ws_urls();
        let mut connected = false;
        for ws_url in urls {
            let endpoint = clob_ws_market_endpoint(&ws_url);
            info!("connecting to clob ws {endpoint}");
            match connect_async(&endpoint).await {
                Ok((stream, _)) => {
                    connected = true;
                    backoff = Duration::from_secs(2);
                    info!("clob ws connected {endpoint}");
                    if let Err(err) = run_clob_session(stream, state.clone()).await {
                        match err {
                            tokio_tungstenite::tungstenite::Error::Io(ref io_err)
                                if matches!(io_err.kind(), ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted) =>
                            {
                                warn!("clob ws session ended: {err}");
                            }
                            tokio_tungstenite::tungstenite::Error::ConnectionClosed
                            | tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
                                warn!("clob ws session ended: {err}");
                            }
                            tokio_tungstenite::tungstenite::Error::Protocol(_) => {
                                warn!("clob ws session ended: {err}");
                            }
                            _ => error!("clob ws session ended: {err}"),
                        }
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    break;
                }
                Err(err) => {
                    error!("clob ws connect failed for {endpoint}: {err}");
                }
            }
        }

        if !connected {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}

fn clob_ws_urls() -> Vec<String> {
    if let Ok(raw) = env::var("CLOB_WS_URLS") {
        let parsed = raw
            .split(',')
            .map(|entry| entry.trim())
            .filter(|entry| !entry.is_empty())
            .map(|entry| entry.to_string())
            .collect::<Vec<_>>();
        if !parsed.is_empty() {
            return parsed;
        }
    }

    if let Ok(single) = env::var("CLOB_WS_URL") {
        if !single.trim().is_empty() {
            return vec![single];
        }
    }

    vec!["wss://ws-subscriptions-clob.polymarket.com".to_string()]
}

fn clob_ws_market_endpoint(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/ws/market") {
        trimmed.to_string()
    } else if trimmed.ends_with("/ws") {
        format!("{trimmed}/market")
    } else {
        format!("{trimmed}/ws/market")
    }
}

async fn run_clob_session(
    stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    state: AppState,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let (mut write, mut read) = stream.split();
    let mut ping = interval(Duration::from_secs(10));
    let mut refresh = interval(Duration::from_secs(5));
    let mut current_tokens = refresh_market_metadata(&state).await;

    if !current_tokens.is_empty() {
        send_subscription(&mut write, &current_tokens).await?;
    }

    loop {
        tokio::select! {
            _ = ping.tick() => {
                // Use a control ping frame to keep the CLOB connection alive.
                write.send(WsMessage::Ping(Vec::new())).await?;
            }
            _ = refresh.tick() => {
                let next_tokens = refresh_market_metadata(&state).await;
                if next_tokens != current_tokens && !next_tokens.is_empty() {
                    send_subscription(&mut write, &next_tokens).await?;
                    current_tokens = next_tokens;
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        handle_ws_text(&state, &text).await;
                    }
                    Some(Ok(WsMessage::Binary(bin))) => {
                        if let Ok(text) = String::from_utf8(bin) {
                            handle_ws_text(&state, &text).await;
                        }
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {
                        write.send(WsMessage::Pong(payload)).await?;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {}
                    Some(Ok(WsMessage::Close(_))) => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

async fn send_subscription(
    write: &mut (impl SinkExt<WsMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tokens: &HashSet<String>,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    if tokens.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = tokens.iter().cloned().collect();
    let payload = serde_json::json!({
        "type": "market",
        "operation": "subscribe",
        "markets": [],
        "assets_ids": ids,
        "initial_dump": true,
        "custom_feature_enabled": true,
    });
    write.send(WsMessage::Text(payload.to_string())).await?;
    Ok(())
}

async fn handle_ws_text(state: &AppState, text: &str) {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("pong") || trimmed.eq_ignore_ascii_case("ping") {
        return;
    }

    if !LOGGED_WS_SAMPLE.swap(true, Ordering::Relaxed) {
        let sample = if trimmed.len() > 500 {
            format!("{}...", &trimmed[..500])
        } else {
            trimmed.to_string()
        };
        info!("clob ws sample: {sample}");
    }

    let value: Value = match serde_json::from_str(trimmed) {
        Ok(value) => value,
        Err(_) => return,
    };
    process_ws_value(state, &value).await;
}

async fn process_ws_value(state: &AppState, value: &Value) {
    let mut updates = Vec::new();
    let mut books = Vec::new();
    collect_ws_updates(value, &mut updates);
    collect_ws_books(value, &mut books);
    for (token_id, hint) in updates {
        apply_price_update(state, &token_id, hint).await;
    }
    for update in books {
        apply_book_update(state, update).await;
    }
}

fn collect_ws_updates(value: &Value, updates: &mut Vec<(String, PriceHint)>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_ws_updates(item, updates);
            }
        }
        Value::Object(map) => {
            if let Some(token_id) = find_ws_token_id(value) {
                if let Some(hint) = extract_ws_price_hint(value) {
                    updates.push((token_id, hint));
                }
            }

            for key in ["data", "payload", "message"] {
                if let Some(nested) = map.get(key) {
                    collect_ws_updates(nested, updates);
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone)]
struct BookUpdateLite {
    asset_id: String,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

fn collect_ws_books(value: &Value, updates: &mut Vec<BookUpdateLite>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_ws_books(item, updates);
            }
        }
        Value::Object(map) => {
            if map.get("event_type").and_then(|v| v.as_str()) == Some("book") {
                if let Some(update) = parse_book_update(map) {
                    updates.push(update);
                }
            }

            for key in ["data", "payload", "message"] {
                if let Some(nested) = map.get(key) {
                    collect_ws_books(nested, updates);
                }
            }
        }
        _ => {}
    }
}

fn parse_book_update(map: &Map<String, Value>) -> Option<BookUpdateLite> {
    let asset_id = map
        .get("asset_id")
        .or_else(|| map.get("assetId"))
        .or_else(|| map.get("token_id"))
        .or_else(|| map.get("tokenId"))
        .and_then(value_to_string)?;

    let bids = parse_book_levels(map.get("bids"));
    let asks = parse_book_levels(map.get("asks"));

    Some(BookUpdateLite {
        asset_id,
        bids,
        asks,
    })
}

fn parse_book_levels(value: Option<&Value>) -> Vec<BookLevel> {
    let mut levels = Vec::new();
    let entries = match value.and_then(|v| v.as_array()) {
        Some(entries) => entries,
        None => return levels,
    };

    for entry in entries {
        if let Some(level) = parse_book_level(entry) {
            levels.push(level);
        }
    }

    levels
}

fn parse_book_level(value: &Value) -> Option<BookLevel> {
    let (price_value, size_value) = if let Some(arr) = value.as_array() {
        (arr.get(0)?, arr.get(1)?)
    } else if let Some(obj) = value.as_object() {
        (obj.get("price")?, obj.get("size")?)
    } else {
        return None;
    };

    let key = value_to_string(price_value)?;
    let price = value_to_f64(price_value)?;
    let size = value_to_f64(size_value)?;

    Some(BookLevel { key, price, size })
}

async fn apply_book_update(state: &AppState, update: BookUpdateLite) {
    let mut live = state.live.write().await;
    let mapping = match live.token_map.get(&update.asset_id) {
        Some(mapping) => {
            info!(
                "Order book update for token {} -> market '{}' ({}) as {:?} outcome",
                update.asset_id, mapping.label, mapping.slug, mapping.outcome
            );
            mapping.clone()
        }
        None => {
            warn!("Received order book update for unknown token: {}", update.asset_id);
            return;
        }
    };

    let entry = live
        .orderbooks
        .entry(mapping.label.clone())
        .or_insert_with(|| MarketOrderBook {
            label: mapping.label.clone(),
            slug: mapping.slug.clone(),
            event_title: mapping.event_title.clone(),
            window: mapping.window.clone(),
            up: BookSide::default(),
            down: BookSide::default(),
        });

    entry.slug = mapping.slug.clone();
    entry.window = mapping.window.clone();
    entry.event_title = mapping.event_title.clone();

    let outcome = match mapping.outcome {
        OutcomeSide::Up => {
            let depth = orderbook_depth();
            info!(
                "Merging order book for market '{}' ({}), UP side: {} bids, {} asks",
                entry.label, entry.slug, update.bids.len(), update.asks.len()
            );
            merge_book_side(&mut entry.up, &update, depth);
            BookOutcome::Up
        }
        OutcomeSide::Down => {
            let depth = orderbook_depth();
            info!(
                "Merging order book for market '{}' ({}), DOWN side: {} bids, {} asks",
                entry.label, entry.slug, update.bids.len(), update.asks.len()
            );
            merge_book_side(&mut entry.down, &update, depth);
            BookOutcome::Down
        }
    };

    let (up_bid, up_ask) = best_bid_ask(&entry.up);
    let (down_bid, down_ask) = best_bid_ask(&entry.down);
    let label = entry.label.clone();
    let slug = entry.slug.clone();

    let update_message = OrderBookUpdateMessage {
        message_type: "orderbook_update".to_string(),
        ts: now_ts(),
        label: entry.label.clone(),
        slug: entry.slug.clone(),
        event_title: entry.event_title.clone(),
        window: entry.window.clone(),
        outcome: match outcome {
            BookOutcome::Up => "up".to_string(),
            BookOutcome::Down => "down".to_string(),
        },
        deltas: OrderBookSideSnapshot {
            bids: update
                .bids
                .iter()
                .map(|level| OrderBookLevelSnapshot {
                    price: level.price,
                    size: level.size,
                })
                .collect(),
            asks: update
                .asks
                .iter()
                .map(|level| OrderBookLevelSnapshot {
                    price: level.price,
                    size: level.size,
                })
                .collect(),
        },
        book: match outcome {
            BookOutcome::Up => snapshot_side(&entry.up),
            BookOutcome::Down => snapshot_side(&entry.down),
        },
    };

    drop(live);
    let _ = state.book_broadcaster.send(update_message);
    apply_paper_logic(state, &label, &slug, up_bid, up_ask, down_bid, down_ask).await;
}

fn best_bid_ask(side: &BookSide) -> (Option<f64>, Option<f64>) {
    let bid = side.bids.first().map(|level| level.price);
    let ask = side.asks.first().map(|level| level.price);
    (bid, ask)
}

fn merge_book_side(side: &mut BookSide, update: &BookUpdateLite, depth: usize) {
    side.bids = merge_levels(&side.bids, &update.bids, true, depth);
    side.asks = merge_levels(&side.asks, &update.asks, false, depth);
}

fn merge_levels(
    existing: &[BookLevel],
    updates: &[BookLevel],
    is_bids: bool,
    depth: usize,
) -> Vec<BookLevel> {
    let mut map: HashMap<String, BookLevel> = existing
        .iter()
        .cloned()
        .map(|level| (level.key.clone(), level))
        .collect();

    for update in updates {
        if update.size <= 0.0 {
            map.remove(&update.key);
        } else {
            map.insert(update.key.clone(), update.clone());
        }
    }

    let mut levels: Vec<BookLevel> = map.into_values().collect();
    if is_bids {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    }
    levels.truncate(depth);
    levels
}

fn orderbook_depth() -> usize {
    env::var("ORDERBOOK_DEPTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10)
}

async fn apply_paper_logic(
    state: &AppState,
    label: &str,
    slug: &str,
    up_bid: Option<f64>,
    up_ask: Option<f64>,
    down_bid: Option<f64>,
    down_ask: Option<f64>,
) {
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut paper = state.paper.write().await;
    if paper.paused {
        return;
    }
    let per_budget = paper.per_market_budget;
    let entry = paper.markets.entry(label.to_string()).or_insert_with(|| PaperMarketState {
        label: label.to_string(),
        slug: slug.to_string(),
        cash: per_budget,
        realized_pnl: 0.0,
        step_index: 0,
        expected_side: None,
        done: false,
        open_positions: Vec::new(),
        sell_orders: Vec::new(),
        trades: Vec::new(),
    });

    if entry.slug != slug {
        info!(
            "Market rollover detected for '{}': {} -> {}",
            label, entry.slug, slug
        );
        let rollover_ts = now_ts();
        
        // Track the side we held in the previous market so we can buy the OPPOSITE in the new market
        let previous_side = entry.open_positions.first().map(|pos| pos.side);
        
        for position in entry.open_positions.drain(..) {
            let best_bid = match position.side {
                OutcomeSide::Up => up_bid,
                OutcomeSide::Down => down_bid,
            };
            let price = best_bid.unwrap_or(position.entry_price);
            let pnl = (price - position.entry_price) * position.size;
            entry.realized_pnl += pnl;
            let slippage = price - position.target_price;
            if let Some(order) = entry
                .sell_orders
                .iter_mut()
                .find(|order| order.id == position.order_id)
            {
                order.status = "rolled".to_string();
                order.filled_ts = Some(rollover_ts);
                order.price = price;
                order.slippage = slippage;
            }
            entry.trades.push(PaperTrade {
                ts: rollover_ts,
                label: entry.label.clone(),
                slug: entry.slug.clone(),
                action: "sell".to_string(),
                side: outcome_label(position.side).to_string(),
                price,
                target_price: position.target_price,
                slippage,
                size: position.size,
                notional: price * position.size,
                realized_pnl: pnl,
                step: position.step,
            });
        }
        
        entry.slug = slug.to_string();
        entry.step_index = 0;
        
        // CRITICAL: After rollover, buy the OPPOSITE side in the new market
        entry.expected_side = previous_side.map(opposite_side);
        
        if let Some(expected) = entry.expected_side {
            info!(
                "After rollover, will buy {:?} in new market '{}' (was {:?} in previous market)",
                expected, slug, previous_side
            );
        } else {
            info!("After rollover, no previous positions, will buy any side in entry band");
        }
        
        entry.done = false;
        entry.cash = (per_budget + entry.realized_pnl).max(0.0);
    }

    let mut sold_any = false;
    let mut remaining = Vec::new();
    for position in entry.open_positions.drain(..) {
        let best_bid = match position.side {
            OutcomeSide::Up => up_bid,
            OutcomeSide::Down => down_bid,
        };
        let filled = best_bid
            .map(|price| price_at_target(price, position.target_price))
            .unwrap_or(false);
        if filled {
            let price = best_bid.unwrap_or(position.target_price);
            let proceeds = price * position.size;
            entry.cash += proceeds;
            let pnl = (price - position.entry_price) * position.size;
            entry.realized_pnl += pnl;
            let slippage = price - position.target_price;
            if let Some(order) = entry
                .sell_orders
                .iter_mut()
                .find(|order| order.id == position.order_id)
            {
                order.status = "filled".to_string();
                order.filled_ts = Some(now_ts());
                order.price = price;
                order.slippage = slippage;
            }
            entry.trades.push(PaperTrade {
                ts: now_ts(),
                label: entry.label.clone(),
                slug: entry.slug.clone(),
                action: "sell".to_string(),
                side: outcome_label(position.side).to_string(),
                price,
                target_price: position.target_price,
                slippage,
                size: position.size,
                notional: proceeds,
                realized_pnl: pnl,
                step: position.step,
            });
            sold_any = true;
        } else {
            remaining.push(position);
        }
    }
    entry.open_positions = remaining;
    if sold_any && entry.open_positions.is_empty() {
        entry.done = true;
    }

    if entry.done {
        return;
    }

    let amounts = paper_step_amounts();
    if entry.step_index >= amounts.len() {
        return;
    }

    let next_amount = amounts[entry.step_index];
    if entry.cash < next_amount {
        return;
    }

    let candidate = match entry.expected_side {
        Some(side) => {
            match side {
                OutcomeSide::Up => {
                    if let Some(price) = up_ask {
                        if price_in_entry_band(price) {
                            info!(
                                "Market '{}' ({}): Found UP ask at {:.4} in entry band [0.55-0.58], will buy",
                                label, slug, price
                            );
                            Some((side, price))
                        } else {
                            info!(
                                "Market '{}' ({}): UP ask at {:.4} NOT in entry band [0.55-0.58], waiting",
                                label, slug, price
                            );
                            None
                        }
                    } else {
                        None
                    }
                }
                OutcomeSide::Down => {
                    if let Some(price) = down_ask {
                        if price_in_entry_band(price) {
                            info!(
                                "Market '{}' ({}): Found DOWN ask at {:.4} in entry band [0.55-0.58], will buy",
                                label, slug, price
                            );
                            Some((side, price))
                        } else {
                            info!(
                                "Market '{}' ({}): DOWN ask at {:.4} NOT in entry band [0.55-0.58], waiting",
                                label, slug, price
                            );
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
        None => {
            if let Some(price) = up_ask {
                if price_in_entry_band(price) {
                    info!(
                        "Market '{}' ({}): First entry - found UP ask at {:.4} in entry band, will buy",
                        label, slug, price
                    );
                    Some((OutcomeSide::Up, price))
                } else if let Some(price) = down_ask {
                    if price_in_entry_band(price) {
                        info!(
                            "Market '{}' ({}): First entry - found DOWN ask at {:.4} in entry band, will buy",
                            label, slug, price
                        );
                        Some((OutcomeSide::Down, price))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Some(price) = down_ask {
                if price_in_entry_band(price) {
                    info!(
                        "Market '{}' ({}): First entry - found DOWN ask at {:.4} in entry band, will buy",
                        label, slug, price
                    );
                    Some((OutcomeSide::Down, price))
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    let (side, price) = match candidate {
        Some(candidate) => candidate,
        None => return,
    };

    apply_paper_buy(entry, side, price, next_amount);
}

fn paper_step_amounts() -> Vec<f64> {
    vec![5.0, 14.34, 23.91, 39.86, 66.44]
}

fn paper_entry_price() -> f64 {
    0.55
}

fn paper_exit_price() -> f64 {
    0.7
}

fn price_at_target(price: f64, target: f64) -> bool {
    let tolerance = 0.0005;
    (price - target).abs() <= tolerance
}

fn price_in_entry_band(price: f64) -> bool {
    (0.55..=0.58).contains(&price)
}

fn opposite_side(side: OutcomeSide) -> OutcomeSide {
    match side {
        OutcomeSide::Up => OutcomeSide::Down,
        OutcomeSide::Down => OutcomeSide::Up,
    }
}

fn outcome_label(side: OutcomeSide) -> &'static str {
    match side {
        OutcomeSide::Up => "up",
        OutcomeSide::Down => "down",
    }
}

fn apply_paper_buy(entry: &mut PaperMarketState, side: OutcomeSide, price: f64, next_amount: f64) {
    let size = next_amount / price;
    let target_price = paper_entry_price();
    let slippage = price - target_price;
    entry.cash -= next_amount;
    let order_id = format!("{}-{}-{}", entry.slug, outcome_label(side), entry.step_index + 1);
    
    info!(
        "🟢 BUY: Market '{}' ({}) - {} {} shares at {:.4} (step {}, notional ${:.2}, target exit {:.2})",
        entry.label,
        entry.slug,
        outcome_label(side).to_uppercase(),
        size,
        price,
        entry.step_index + 1,
        next_amount,
        paper_exit_price()
    );
    
    entry.open_positions.push(PaperPosition {
        side,
        entry_price: price,
        size,
        notional: next_amount,
        target_price: paper_exit_price(),
        step: entry.step_index + 1,
        order_id: order_id.clone(),
    });
    entry.sell_orders.push(PaperSellOrder {
        id: order_id,
        ts: now_ts(),
        label: entry.label.clone(),
        slug: entry.slug.clone(),
        side: outcome_label(side).to_string(),
        price: paper_exit_price(),
        target_price: paper_exit_price(),
        slippage: 0.0,
        size,
        status: "active".to_string(),
        filled_ts: None,
    });
    entry.trades.push(PaperTrade {
        ts: now_ts(),
        label: entry.label.clone(),
        slug: entry.slug.clone(),
        action: "buy".to_string(),
        side: outcome_label(side).to_string(),
        price,
        target_price,
        slippage,
        size,
        notional: next_amount,
        realized_pnl: 0.0,
        step: entry.step_index + 1,
    });
    entry.step_index += 1;
    entry.expected_side = Some(opposite_side(side));
    
    info!(
        "  → Cash remaining: ${:.2}, Next expected side: {:?}",
        entry.cash,
        entry.expected_side
    );
}

async fn apply_price_update(state: &AppState, token_id: &str, hint: PriceHint) {
    let price = match hint.derive_yes() {
        Some(price) => price,
        None => return,
    };

    let mut live = state.live.write().await;
    let mapping = match live.token_map.get(token_id) {
        Some(mapping) => mapping.clone(),
        None => return,
    };

    let snapshot = match live.latest.get_mut(&mapping.label) {
        Some(snapshot) => snapshot,
        None => return,
    };

    snapshot.slug = mapping.slug.clone();
    snapshot.window = mapping.window.clone();
    snapshot.event_title = mapping.event_title.clone();

    match mapping.outcome {
        OutcomeSide::Up => {
            snapshot.price.up = Some(price);
            if snapshot.price.down.is_none() {
                snapshot.price.down = Some((1.0 - price).clamp(0.0, 1.0));
            }
        }
        OutcomeSide::Down => {
            snapshot.price.down = Some(price);
            if snapshot.price.up.is_none() {
                snapshot.price.up = Some((1.0 - price).clamp(0.0, 1.0));
            }
        }
    }

    snapshot.source = "clob".to_string();
    drop(live);
    broadcast_snapshot(state).await;
}

struct MarketMeta {
    label: String,
    slug: String,
    window: Option<WindowInfo>,
    title: Option<String>,
    up_token_id: Option<String>,
    down_token_id: Option<String>,
    hint: PriceHint,
}

async fn fetch_market_metadata(http: Client, market: MarketConfig, now_ts: i64) -> MarketMeta {
    let resolved = resolve_market(&market, now_ts);
    let window = resolved
        .window
        .clone()
        .or_else(|| parse_slug_window(&resolved.slug));
    let event_meta = fetch_event_metadata(&http, &resolved.slug).await;

    let up_token_id = market
        .yes_token_id
        .clone()
        .or(event_meta.up_token_id.clone());
    let down_token_id = market
        .no_token_id
        .clone()
        .or(event_meta.down_token_id.clone());

    MarketMeta {
        label: market.label,
        slug: resolved.slug,
        window,
        title: event_meta.title,
        up_token_id,
        down_token_id,
        hint: event_meta.price_hint,
    }
}

async fn refresh_market_metadata(state: &AppState) -> HashSet<String> {
    let now_ts = now_ts();
    let tasks = state
        .markets
        .iter()
        .cloned()
        .map(|market| fetch_market_metadata(state.http.clone(), market, now_ts));
    let markets = join_all(tasks).await;

    let existing_books = {
        let live = state.live.read().await;
        live.orderbooks.clone()
    };

    let mut latest = HashMap::new();
    let mut token_map = HashMap::new();
    let mut token_set = HashSet::new();
    let mut books = HashMap::new();
    let mut token_to_market = HashMap::new(); // For validation

    for meta in markets {
        let mut snapshot = MarketSnapshot {
            label: meta.label.clone(),
            slug: meta.slug.clone(),
            event_title: meta.title.clone(),
            window: meta.window.clone(),
            price: PriceInfo { up: None, down: None },
            source: "unavailable".to_string(),
        };

        let (up, down) = meta.hint.derive_pair();
        if up.is_some() || down.is_some() {
            snapshot.price.up = up;
            snapshot.price.down = down;
            snapshot.source = "gamma".to_string();
        }

        if let Some(id) = meta.up_token_id.clone() {
            // Check for conflicts
            if let Some(existing_market) = token_to_market.get(&id) {
                error!(
                    "Token ID {} is mapped to multiple markets: '{}' and '{}'!",
                    id, existing_market, meta.label
                );
            }
            token_to_market.insert(id.clone(), meta.label.clone());

            info!(
                "Mapping token {} to market '{}' (slug: {}) as UP outcome",
                id, meta.label, meta.slug
            );
            token_set.insert(id.clone());
            token_map.insert(
                id,
                TokenMapping {
                    label: meta.label.clone(),
                    slug: meta.slug.clone(),
                    window: meta.window.clone(),
                    event_title: meta.title.clone(),
                    outcome: OutcomeSide::Up,
                },
            );
        }

        if let Some(id) = meta.down_token_id.clone() {
            // Check for conflicts
            if let Some(existing_market) = token_to_market.get(&id) {
                error!(
                    "Token ID {} is mapped to multiple markets: '{}' and '{}'!",
                    id, existing_market, meta.label
                );
            }
            token_to_market.insert(id.clone(), meta.label.clone());

            info!(
                "Mapping token {} to market '{}' (slug: {}) as DOWN outcome",
                id, meta.label, meta.slug
            );
            token_set.insert(id.clone());
            token_map.insert(
                id,
                TokenMapping {
                    label: meta.label.clone(),
                    slug: meta.slug.clone(),
                    window: meta.window.clone(),
                    event_title: meta.title.clone(),
                    outcome: OutcomeSide::Down,
                },
            );
        }

        // Validate that up and down tokens are different
        if let (Some(up_id), Some(down_id)) = (&meta.up_token_id, &meta.down_token_id) {
            if up_id == down_id {
                error!(
                    "Market '{}' has the same token ID for both UP and DOWN outcomes: {}",
                    meta.label, up_id
                );
            }
        }

        latest.insert(meta.label.clone(), snapshot);

        let book_entry = existing_books
            .get(&meta.label)
            .cloned()
            .unwrap_or_else(|| MarketOrderBook {
                label: meta.label.clone(),
                slug: meta.slug.clone(),
                event_title: meta.title.clone(),
                window: meta.window.clone(),
                up: BookSide::default(),
                down: BookSide::default(),
            });
        books.insert(
            meta.label.clone(),
            MarketOrderBook {
                label: meta.label.clone(),
                slug: meta.slug.clone(),
                event_title: meta.title.clone(),
                window: meta.window.clone(),
                up: book_entry.up,
                down: book_entry.down,
            },
        );
    }

    let mut live = state.live.write().await;
    live.latest = latest;
    live.token_map = token_map;
    live.token_set = token_set.clone();
    live.orderbooks = books;
    drop(live);

    broadcast_snapshot(state).await;

    token_set
}

fn find_ws_token_id(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in ["asset_id", "assetId", "token_id", "tokenId", "id"] {
                if let Some(id) = map.get(key).and_then(value_to_string) {
                    return Some(id);
                }
            }
            if let Some(nested) = map.get("data") {
                if let Some(id) = find_ws_token_id(nested) {
                    return Some(id);
                }
            }
            if let Some(nested) = map.get("payload") {
                if let Some(id) = find_ws_token_id(nested) {
                    return Some(id);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(find_ws_token_id),
        _ => None,
    }
}

fn extract_ws_price_hint(value: &Value) -> Option<PriceHint> {
    match value {
        Value::Object(map) => {
            let mut hint = PriceHint {
                last_trade: map
                    .get("last_trade_price")
                    .or_else(|| map.get("lastTradePrice"))
                    .or_else(|| map.get("price"))
                    .and_then(value_to_f64),
                best_bid: map
                    .get("best_bid")
                    .or_else(|| map.get("bestBid"))
                    .or_else(|| map.get("bid"))
                    .and_then(value_to_f64),
                best_ask: map
                    .get("best_ask")
                    .or_else(|| map.get("bestAsk"))
                    .or_else(|| map.get("ask"))
                    .and_then(value_to_f64),
            };

            if hint.last_trade.is_none() && (map.get("bids").is_some() || map.get("asks").is_some()) {
                if let Some(mid) = extract_mid_price(value) {
                    hint.last_trade = Some(mid);
                }
            }

            if hint.last_trade.is_some() || hint.best_bid.is_some() || hint.best_ask.is_some() {
                return Some(hint);
            }

            for key in ["data", "payload", "message"] {
                if let Some(nested) = map.get(key) {
                    if let Some(nested_hint) = extract_ws_price_hint(nested) {
                        return Some(nested_hint);
                    }
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(extract_ws_price_hint),
        _ => None,
    }
}

async fn fetch_event_metadata(http: &Client, slug: &str) -> EventMetadata {
    let primary = format!("https://gamma-api.polymarket.com/events/slug/{slug}");
    if let Some(value) = fetch_event_payload(http, &primary, slug).await {
        return build_event_metadata(&value);
    }

    let fallback = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    if let Some(value) = fetch_event_payload(http, &fallback, slug).await {
        return build_event_metadata(&value);
    }

    EventMetadata::default()
}

async fn fetch_event_payload(http: &Client, url: &str, slug: &str) -> Option<Value> {
    let max_attempts = 3;
    for attempt in 1..=max_attempts {
        match http.get(url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return match resp.json().await {
                        Ok(value) => Some(value),
                        Err(err) => {
                            warn!("event metadata parse failed for {slug}: {err}");
                            None
                        }
                    };
                }

                let status = resp.status();
                if status.as_u16() == 429 || status.is_server_error() {
                    warn!(
                        "event metadata request returned {} for {} via {} (attempt {}/{})",
                        status,
                        slug,
                        url,
                        attempt,
                        max_attempts
                    );
                } else {
                    warn!(
                        "event metadata request returned {} for {} via {}",
                        status,
                        slug,
                        url
                    );
                    return None;
                }
            }
            Err(err) => {
                warn!(
                    "event metadata request failed for {slug} via {url} (attempt {}/{}): {err}",
                    attempt,
                    max_attempts
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
    }

    None
}

fn build_event_metadata(value: &Value) -> EventMetadata {
    let title = extract_event_title(value);
    let (mut up_token_id, mut down_token_id) = extract_token_ids(value);
    let price_hint = extract_price_hint(value);
    
    // Check if the market question is phrased negatively (asking ONLY about "down", "lower", etc.)
    // In such cases, "Yes" means down and "No" means up, so we need to swap the tokens
    // Examples: "Will BTC go lower?" -> Yes=Down, No=Up (needs swap)
    //           "BTC Up or Down?" -> Up=Up, Down=Down (no swap needed)
    let should_swap = if let Some(ref t) = title {
        let normalized = t.to_lowercase();
        
        // Check if it's asking about positive movement
        let asks_positive = normalized.contains("higher") 
            || normalized.contains("above") 
            || normalized.contains("over")
            || (normalized.contains("up") && !normalized.contains("up or down") && !normalized.contains("updown"))
            || normalized.contains("increase")
            || normalized.contains("rise")
            || normalized.contains("gain");
        
        // Check if the question is asking ONLY about negative movement (not "up or down")
        let asks_negative = (normalized.contains("lower") 
            || normalized.contains("below") 
            || normalized.contains("under")
            || (normalized.contains("down") && !normalized.contains("up or down") && !normalized.contains("updown"))
            || normalized.contains("decrease")
            || normalized.contains("fall")
            || normalized.contains("drop"))
            && !asks_positive;
        
        // Only swap if it asks ONLY about negative movement
        asks_negative
    } else {
        false
    };
    
    if should_swap {
        warn!(
            "Market title '{}' appears to ask about ONLY negative movement. Swapping token assignments: up<->down",
            title.as_ref().unwrap_or(&"".to_string())
        );
        std::mem::swap(&mut up_token_id, &mut down_token_id);
    }
    
    info!(
        "Extracted token IDs from event metadata: up={:?}, down={:?} (swapped={})",
        up_token_id, down_token_id, should_swap
    );
    EventMetadata {
        title,
        up_token_id,
        down_token_id,
        price_hint,
    }
}

fn extract_event_title(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(extract_event_title),
        Value::Object(map) => map
            .get("title")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                map.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            }),
        _ => None,
    }
}

fn extract_price_hint(value: &Value) -> PriceHint {
    match value {
        Value::Array(items) => items.iter().fold(PriceHint::default(), |acc, item| {
            merge_price_hints(acc, extract_price_hint(item))
        }),
        Value::Object(map) => {
            let mut hint = PriceHint {
                last_trade: map.get("lastTradePrice").and_then(value_to_f64),
                best_bid: map.get("bestBid").and_then(value_to_f64),
                best_ask: map.get("bestAsk").and_then(value_to_f64),
            };

            if let Some(markets) = map.get("markets").and_then(|v| v.as_array()) {
                for market in markets {
                    hint = merge_price_hints(hint, price_hint_from_market(market));
                    if hint.last_trade.is_some()
                        && hint.best_bid.is_some()
                        && hint.best_ask.is_some()
                    {
                        break;
                    }
                }
            }

            hint
        }
        _ => PriceHint::default(),
    }
}

fn price_hint_from_market(value: &Value) -> PriceHint {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return PriceHint::default(),
    };

    PriceHint {
        last_trade: obj.get("lastTradePrice").and_then(value_to_f64),
        best_bid: obj.get("bestBid").and_then(value_to_f64),
        best_ask: obj.get("bestAsk").and_then(value_to_f64),
    }
}

async fn fetch_prices(
    http: &Client,
    up_token_id: Option<&str>,
    down_token_id: Option<&str>,
    hints: PriceHint,
) -> (Option<f64>, Option<f64>, String) {
    let missing_ids = up_token_id.is_none() && down_token_id.is_none();

    let up = match up_token_id {
        Some(id) => fetch_book_price(http, id).await,
        None => None,
    };
    let down = match down_token_id {
        Some(id) => fetch_book_price(http, id).await,
        None => None,
    };

    if up.is_some() || down.is_some() {
        let down_final = match (up, down) {
            (Some(up_value), None) => Some((1.0 - up_value).clamp(0.0, 1.0)),
            (_, down_value) => down_value,
        };

        return (up, down_final, "clob".to_string());
    }

    let (hint_up, hint_down) = hints.derive_pair();
    let source = if hint_up.is_some() || hint_down.is_some() {
        "gamma".to_string()
    } else if missing_ids {
        "token-id-missing".to_string()
    } else {
        "unavailable".to_string()
    };

    (hint_up, hint_down, source)
}

async fn fetch_book_price(http: &Client, token_id: &str) -> Option<f64> {
    let url = format!("https://clob.polymarket.com/book?token_id={token_id}");
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let value: Value = resp.json().await.ok()?;
    let last_trade = value
        .get("last_trade_price")
        .or_else(|| value.get("lastTradePrice"))
        .and_then(value_to_f64);
    if last_trade.is_some() {
        return last_trade;
    }
    extract_mid_price(&value)
}

fn extract_mid_price(value: &Value) -> Option<f64> {
    let bids = extract_side_price(value, "bids");
    let asks = extract_side_price(value, "asks");

    match (bids, asks) {
        (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
        _ => None,
    }
}

fn extract_token_ids(value: &Value) -> (Option<String>, Option<String>) {
    match value {
        Value::Array(items) => items.iter().fold((None, None), |acc, item| {
            merge_token_pairs(acc, extract_token_ids(item))
        }),
        Value::Object(map) => {
            let mut result = direct_token_fields(map);

            if let Some(markets) = map.get("markets").and_then(|v| v.as_array()) {
                for market in markets {
                    result = merge_token_pairs(result, token_ids_from_market(market));
                    if result.0.is_some() && result.1.is_some() {
                        return result;
                    }
                }
            }

            if let Some(tokens) = map.get("tokens").and_then(|v| v.as_array()) {
                for token in tokens {
                    result = merge_token_pairs(result, token_id_from_token(token));
                    if result.0.is_some() && result.1.is_some() {
                        return result;
                    }
                }
            }

            result
        }
        _ => (None, None),
    }
}

fn direct_token_fields(map: &Map<String, Value>) -> (Option<String>, Option<String>) {
    let yes = map
        .get("yes_token_id")
        .or_else(|| map.get("yesTokenId"))
        .or_else(|| map.get("yesTokenID"))
        .and_then(value_to_string);
    let no = map
        .get("no_token_id")
        .or_else(|| map.get("noTokenId"))
        .or_else(|| map.get("noTokenID"))
        .and_then(value_to_string);
    (yes, no)
}

fn token_ids_from_market(value: &Value) -> (Option<String>, Option<String>) {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return (None, None),
    };

    let ids = parse_string_array(obj.get("clobTokenIds"));
    let outcomes = parse_string_array(obj.get("outcomes"));
    let mut result = pair_outcomes_with_ids(&outcomes, &ids);

    if result.0.is_none() || result.1.is_none() {
        if let Some(tokens) = obj.get("tokens").and_then(|v| v.as_array()) {
            for token in tokens {
                result = merge_token_pairs(result, token_id_from_token(token));
                if result.0.is_some() && result.1.is_some() {
                    break;
                }
            }
        }
    }

    if (result.0.is_none() || result.1.is_none()) && ids.len() >= 2 {
        warn!(
            "Token classification failed or incomplete, using positional fallback: ids[0]={} as UP, ids[1]={} as DOWN. This may be incorrect if Polymarket returns tokens in a different order!",
            ids[0], ids[1]
        );
        // Note: This assumes ids[0] is the "yes"/"up" token and ids[1] is the "no"/"down" token
        // This matches Polymarket's standard ordering but may not always be correct
        if result.0.is_none() {
            result.0 = Some(ids[0].clone());
        }
        if result.1.is_none() && ids.len() > 1 {
            result.1 = Some(ids[1].clone());
        }
    }

    result
}

fn token_id_from_token(value: &Value) -> (Option<String>, Option<String>) {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return (None, None),
    };

    let id = obj
        .get("token_id")
        .or_else(|| obj.get("tokenId"))
        .and_then(value_to_string);
    let label = obj
        .get("outcome")
        .or_else(|| obj.get("label"))
        .or_else(|| obj.get("ticker"))
        .or_else(|| obj.get("name"))
        .or_else(|| obj.get("title"))
        .or_else(|| obj.get("side"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    classify_with_id(label.as_deref(), id)
}

fn classify_with_id(label: Option<&str>, id: Option<String>) -> (Option<String>, Option<String>) {
    match (label, id) {
        (Some(text), Some(token_id)) => match classify_outcome(text) {
            OutcomeLabel::Up => (Some(token_id), None),
            OutcomeLabel::Down => (None, Some(token_id)),
            OutcomeLabel::Unknown => (None, None),
        },
        _ => (None, None),
    }
}

fn pair_outcomes_with_ids(outcomes: &[String], ids: &[String]) -> (Option<String>, Option<String>) {
    info!(
        "Pairing outcomes {:?} with token IDs {:?}",
        outcomes, ids
    );
    let mut result = (None, None);
    for (label, token_id) in outcomes.iter().zip(ids.iter()) {
        let classification = classify_outcome(label);
        info!(
            "Outcome '{}' + token '{}' => classified as {:?}",
            label, token_id, classification
        );
        let segment = match classification {
            OutcomeLabel::Up => (Some(token_id.clone()), None),
            OutcomeLabel::Down => (None, Some(token_id.clone())),
            OutcomeLabel::Unknown => (None, None),
        };
        result = merge_token_pairs(result, segment);
    }
    result
}

fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(value_to_string).collect::<Vec<_>>(),
        Some(Value::String(text)) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        Some(other) if other.is_null() => Vec::new(),
        Some(other) => other
            .as_str()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(num) => Some(num.to_string()),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn merge_token_pairs(
    mut base: (Option<String>, Option<String>),
    new: (Option<String>, Option<String>),
) -> (Option<String>, Option<String>) {
    if base.0.is_none() {
        base.0 = new.0;
    }
    if base.1.is_none() {
        base.1 = new.1;
    }
    base
}

fn merge_price_hints(mut base: PriceHint, new: PriceHint) -> PriceHint {
    if base.last_trade.is_none() {
        base.last_trade = new.last_trade;
    }
    if base.best_bid.is_none() {
        base.best_bid = new.best_bid;
    }
    if base.best_ask.is_none() {
        base.best_ask = new.best_ask;
    }
    base
}

#[derive(Debug, Clone, Copy)]
enum OutcomeLabel {
    Up,
    Down,
    Unknown,
}

fn classify_outcome(label: &str) -> OutcomeLabel {
    let normalized = label.trim().to_lowercase();
    
    // First check for exact matches (case-insensitive)
    if normalized == "up" || normalized == "yes" || normalized == "higher" || normalized == "above" {
        return OutcomeLabel::Up;
    }
    if normalized == "down" || normalized == "no" || normalized == "lower" || normalized == "below" {
        return OutcomeLabel::Down;
    }
    
    // Then check for partial matches
    if contains_any(
        &normalized,
        &["yes", "up", "higher", "above", "over", "bull", "increase", "rise"],
    ) {
        OutcomeLabel::Up
    } else if contains_any(
        &normalized,
        &["no", "down", "lower", "below", "under", "bear", "decrease", "fall"],
    ) {
        OutcomeLabel::Down
    } else {
        OutcomeLabel::Unknown
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|token| haystack.contains(token))
}

fn extract_side_price(value: &Value, side: &str) -> Option<f64> {
    let entries = value.get(side)?.as_array()?;
    let mut iter = entries.iter().filter_map(entry_price);
    let first = iter.next()?;
    Some(match side {
        "bids" => iter.fold(first, f64::max),
        "asks" => iter.fold(first, f64::min),
        _ => first,
    })
}

fn parse_price(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.parse().ok()
}

fn entry_price(value: &Value) -> Option<f64> {
    if let Some(list) = value.as_array() {
        return list.get(0).and_then(parse_price);
    }
    if let Some(obj) = value.as_object() {
        return obj.get("price").and_then(parse_price);
    }
    None
}

fn parse_slug_window(slug: &str) -> Option<WindowInfo> {
    let parts: Vec<&str> = slug.split('-').collect();
    if parts.len() < 4 {
        return None;
    }

    let window_part = parts.get(parts.len() - 2)?;
    let start_part = parts.get(parts.len() - 1)?;
    if !window_part.ends_with('m') {
        return None;
    }

    let window_minutes = window_part.trim_end_matches('m').parse::<u64>().ok()?;
    let end_ts = start_part.parse::<i64>().ok()?;
    let start_ts = end_ts - (window_minutes as i64 * 60);

    Some(WindowInfo {
        start_ts,
        end_ts,
        window_minutes,
    })
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn load_markets() -> Vec<MarketConfig> {
    let config_path = env::var("MARKETS_CONFIG").unwrap_or_else(|_| "config/markets.json".into());

    let content = match fs::read_to_string(&config_path) {
        Ok(content) => content,
        Err(err) => {
            error!("failed to read {config_path}: {err}");
            return default_markets();
        }
    };

    let parsed: MarketsConfigFile = match serde_json::from_str(&content) {
        Ok(parsed) => parsed,
        Err(err) => {
            error!("failed to parse {config_path}: {err}");
            return default_markets();
        }
    };

    if parsed.markets.is_empty() {
        return default_markets();
    }

    parsed.markets
}

fn default_markets() -> Vec<MarketConfig> {
    vec![
        MarketConfig {
            label: "BTC 15m Up/Down".to_string(),
            slug: None,
            slug_prefix: Some("btc-updown-15m".to_string()),
            window_minutes: Some(15),
            yes_token_id: None,
            no_token_id: None,
        },
        MarketConfig {
            label: "ETH 15m Up/Down".to_string(),
            slug: None,
            slug_prefix: Some("eth-updown-15m".to_string()),
            window_minutes: Some(15),
            yes_token_id: None,
            no_token_id: None,
        },
        MarketConfig {
            label: "SOL 15m Up/Down".to_string(),
            slug: None,
            slug_prefix: Some("sol-updown-15m".to_string()),
            window_minutes: Some(15),
            yes_token_id: None,
            no_token_id: None,
        },
        MarketConfig {
            label: "XRP 15m Up/Down".to_string(),
            slug: None,
            slug_prefix: Some("xrp-updown-15m".to_string()),
            window_minutes: Some(15),
            yes_token_id: None,
            no_token_id: None,
        },
    ]
}
