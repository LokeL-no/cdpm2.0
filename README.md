# CDPM 2.0

Rust backend + Svelte dashboard for Polymarket 15m up/down markets (BTC, ETH, SOL, XRP). The backend streams live snapshots over WebSocket and proxies Gamma/CLOB data.

## Project Layout

- Backend: Axum service in [backend/src/main.rs](backend/src/main.rs)
- Market config: [config/markets.json](config/markets.json)
- Frontend: Svelte app in [frontend/src/App.svelte](frontend/src/App.svelte)

## Requirements

- Rust toolchain (stable)
- Node.js 18+

## Run Backend

```bash
cargo run -p backend
```

Optional config override:

```bash
MARKETS_CONFIG=config/markets.json cargo run -p backend
```

Optional CLOB WebSocket overrides:

```bash
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws \
CLOB_WS_CHANNEL=market \
cargo run -p backend
```

Backend endpoints:

- `GET /api/snapshot`
- `GET /api/health`
- `WS /ws`

## Run Frontend

```bash
cd frontend
npm install
npm run dev
```

Vite proxy forwards `/api` and `/ws` to `http://localhost:3001`.

## Configure Markets

Edit [config/markets.json](config/markets.json):

- `slug_prefix`: base slug (e.g. `btc-updown-15m`)
- `window_minutes`: cadence for the recurring market (15 for Polymarket up/down)
- `yes_token_id` / `no_token_id` *(optional)*: overrides for CLOB token IDs

The backend now computes the active epoch automatically. Every snapshot aligns to the current 15‑minute window (epoch = market end) using `slug_prefix-<epoch>`. When the window expires it automatically rolls to the next epoch without editing the config file. Token IDs are discovered from Gamma metadata unless overridden.

## Notes

- Dynamic slugs follow the `slug_prefix-<epoch_end>` shape. Provide a full `slug` only if you want to pin a historical market manually.
- This is paper-trading ready, but no trading logic is wired yet.