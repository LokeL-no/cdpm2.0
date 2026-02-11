<script>
  import { onMount } from 'svelte';

  let markets = [];
  let status = 'connecting';
  let lastUpdate = null;
  let orderbooks = {};
  let bookStatus = 'connecting';
  let paperState = { markets: [] };
  let paperTimer = null;
  let fallbackTimer = null;

  const httpBase = import.meta.env.VITE_BACKEND_HTTP_URL || '';
  const wsBase = import.meta.env.VITE_BACKEND_WS_URL || '';

  const formatTime = (ts) => {
    if (!ts) return '--:--:--';
    return new Date(ts * 1000).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatPrice = (value) => {
    if (value === null || value === undefined) return '--';
    return value.toFixed(4);
  };

  const formatSize = (value) => {
    if (value === null || value === undefined) return '--';
    if (value === 0) return '0';
    if (value < 0.01) return value.toExponential(2);
    return value.toFixed(4);
  };

  const formatMoney = (value) => {
    if (value === null || value === undefined) return '--';
    return value.toFixed(2);
  };

  const formatShares = (value) => {
    if (value === null || value === undefined) return '--';
    return value.toFixed(4);
  };

  const formatSigned = (value) => {
    if (value === null || value === undefined) return '--';
    const sign = value > 0 ? '+' : '';
    return `${sign}${value.toFixed(4)}`;
  };

  const buildWsUrl = () => {
    if (wsBase) return wsBase;
    return '/ws';
  };

  const buildBookWsUrl = () => {
    if (wsBase) return `${wsBase}/orderbook`;
    return '/ws/orderbook';
  };

  const buildHttpUrl = (path) => {
    if (httpBase) return `${httpBase}${path}`;
    return path;
  };

  const scheduleFallback = () => {
    if (fallbackTimer) return;
    fallbackTimer = setInterval(async () => {
      try {
        const resp = await fetch(buildHttpUrl('/api/snapshot'));
        if (!resp.ok) return;
        const data = await resp.json();
        markets = data.markets || [];
        lastUpdate = data.ts;
        status = 'polling';
      } catch (err) {
        status = 'offline';
      }
    }, 5000);
  };

  const connectWs = () => {
    const socket = new WebSocket(buildWsUrl());

    socket.addEventListener('open', () => {
      status = 'live';
      if (fallbackTimer) {
        clearInterval(fallbackTimer);
        fallbackTimer = null;
      }
    });

    socket.addEventListener('message', (event) => {
      try {
        const data = JSON.parse(event.data);
        markets = data.markets || [];
        lastUpdate = data.ts;
      } catch (err) {
        status = 'degraded';
      }
    });

    socket.addEventListener('close', () => {
      status = 'reconnecting';
      scheduleFallback();
      setTimeout(connectWs, 2000);
    });

    socket.addEventListener('error', () => {
      status = 'degraded';
      scheduleFallback();
    });
  };

  const connectBookWs = () => {
    const socket = new WebSocket(buildBookWsUrl());

    socket.addEventListener('open', () => {
      bookStatus = 'live';
    });

    socket.addEventListener('message', (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.message_type === 'orderbook_update') {
          const label = data.label;
          if (!orderbooks[label]) {
            orderbooks[label] = { up: { bids: [], asks: [] }, down: { bids: [], asks: [] } };
          }
          orderbooks[label][data.outcome] = data.book || { bids: [], asks: [] };
          orderbooks = { ...orderbooks };
          return;
        }

        if (Array.isArray(data.markets)) {
          const next = {};
          for (const market of data.markets) {
            next[market.label] = {
              up: market.up || { bids: [], asks: [] },
              down: market.down || { bids: [], asks: [] }
            };
          }
          orderbooks = next;
          return;
        }
      } catch (err) {
        bookStatus = 'degraded';
      }
    });

    socket.addEventListener('close', () => {
      bookStatus = 'reconnecting';
      setTimeout(connectBookWs, 2000);
    });

    socket.addEventListener('error', () => {
      bookStatus = 'degraded';
    });
  };

  onMount(async () => {
    try {
      const resp = await fetch(buildHttpUrl('/api/snapshot'));
      if (resp.ok) {
        const data = await resp.json();
        markets = data.markets || [];
        lastUpdate = data.ts;
      }
    } catch (err) {
      status = 'offline';
    }

    connectWs();
    connectBookWs();

    const loadPaperState = async () => {
      try {
        const resp = await fetch(buildHttpUrl('/api/paper/state'));
        if (!resp.ok) return;
        paperState = await resp.json();
      } catch (err) {
        // ignore
      }
    };

    await loadPaperState();
    paperTimer = setInterval(loadPaperState, 2000);
  });

  const summaryBySymbol = () => {
    const totals = { BTC: 0, ETH: 0, SOL: 0, XRP: 0 };
    for (const market of paperState.markets || []) {
      const label = market.label || '';
      if (label.includes('BTC')) totals.BTC += market.realized_pnl || 0;
      if (label.includes('ETH')) totals.ETH += market.realized_pnl || 0;
      if (label.includes('SOL')) totals.SOL += market.realized_pnl || 0;
      if (label.includes('XRP')) totals.XRP += market.realized_pnl || 0;
    }
    return totals;
  };

  const countOpenPositions = (market) => (market.open_positions || []).length;
  const countActiveSells = (market) =>
    (market.sell_orders || []).filter((order) => order.status === 'active').length;
  const totalBuyCost = (market) =>
    (market.trades || []).reduce((sum, trade) => {
      if (trade.action === 'buy') return sum + (trade.notional || 0);
      return sum;
    }, 0);

  const togglePause = async () => {
    try {
      const resp = await fetch(buildHttpUrl('/api/paper/pause'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paused: !paperState.paused })
      });
      if (!resp.ok) return;
      const data = await resp.json();
      paperState = { ...paperState, paused: data.paused };
    } catch (err) {
      // ignore
    }
  };

  const resetPaper = async () => {
    try {
      const resp = await fetch(buildHttpUrl('/api/paper/reset'), { method: 'POST' });
      if (!resp.ok) return;
      paperState = await resp.json();
    } catch (err) {
      // ignore
    }
  };
</script>

<main class="scene">
  <section class="hero">
    <div>
      <p class="eyebrow">Polymarket 15m Up/Down</p>
      <h1>Live Market Ops</h1>
      <p class="subtitle">BTC, ETH, SOL, XRP monitoring with real-time CLOB updates.</p>
    </div>
    <div class="status">
      <div class={`dot ${status}`}></div>
      <div>
        <p class="status-label">{status}</p>
        <p class="status-time">Last update: {formatTime(lastUpdate)}</p>
        <p class="status-time">Orderbook: {bookStatus}</p>
      </div>
    </div>
  </section>

  <section class="grid">
    {#if markets.length === 0}
      <div class="empty">Waiting for market data...</div>
    {:else}
      {#each markets as market}
        <article class="card">
          <header>
            <div>
              <h2>{market.label}</h2>
              <p class="slug">{market.slug}</p>
            </div>
            <span class="source">{market.source}</span>
          </header>
          <div class="window">
            <div>
              <p class="label">Window</p>
              <p class="value">
                {market.window
                  ? `${formatTime(market.window.start_ts)} -> ${formatTime(market.window.end_ts)}`
                  : 'n/a'}
              </p>
            </div>
            <div>
              <p class="label">Event</p>
              <p class="value">{market.event_title || 'Unknown'}</p>
            </div>
          </div>
          <div class="prices">
            <div class="price yes">
              <span class="pill side up">UP</span>
              <p class="value">{formatPrice(market.price?.up)}</p>
            </div>
            <div class="price no">
              <span class="pill side down">DOWN</span>
              <p class="value">{formatPrice(market.price?.down)}</p>
            </div>
          </div>
          <div class="orderbook">
            <div class="book">
              <p class="label">UP Book</p>
              <div class="levels">
                {#each (orderbooks[market.label]?.up?.bids || []).slice(0, 5) as level}
                  <div class="row bid">
                    <span>{formatPrice(level.price)}</span>
                    <span>{formatSize(level.size)}</span>
                  </div>
                {/each}
                {#each (orderbooks[market.label]?.up?.asks || []).slice(0, 5) as level}
                  <div class="row ask">
                    <span>{formatPrice(level.price)}</span>
                    <span>{formatSize(level.size)}</span>
                  </div>
                {/each}
              </div>
            </div>
            <div class="book">
              <p class="label">DOWN Book</p>
              <div class="levels">
                {#each (orderbooks[market.label]?.down?.bids || []).slice(0, 5) as level}
                  <div class="row bid">
                    <span>{formatPrice(level.price)}</span>
                    <span>{formatSize(level.size)}</span>
                  </div>
                {/each}
                {#each (orderbooks[market.label]?.down?.asks || []).slice(0, 5) as level}
                  <div class="row ask">
                    <span>{formatPrice(level.price)}</span>
                    <span>{formatSize(level.size)}</span>
                  </div>
                {/each}
              </div>
            </div>
          </div>
        </article>
      {/each}
    {/if}
  </section>

  <section class="paper">
    <header class="paper-header">
      <h2>Paper Trades</h2>
      <p class="subtitle">Budget 200 per market. Live P/L and fills.</p>
    </header>
    <div class="summary-card">
      <h3>Strategy Summary</h3>
      <div class="summary-actions">
        <button class="action" on:click={togglePause}>
          {paperState.paused ? 'Resume' : 'Pause'}
        </button>
        <button class="action secondary" on:click={resetPaper}>Reset</button>
      </div>
      <div class="summary-grid">
        {#each Object.entries(summaryBySymbol()) as [symbol, pnl]}
          <div class="summary-row">
            <span class="label">{symbol}</span>
            <span class={`value ${pnl >= 0 ? 'pos' : 'neg'}`}>${formatPrice(pnl)}</span>
          </div>
        {/each}
      </div>
    </div>
    {#if paperState.markets?.length}
      <div class="paper-grid">
        {#each paperState.markets as market}
          <article class="paper-card">
            <div class="paper-meta">
              <div>
                <h3>{market.label}</h3>
                <p class="slug">{market.slug}</p>
              </div>
              <span class={`status-pill ${market.status}`}>{market.status}</span>
            </div>
            <div class="paper-stats wide">
              <div>
                <p class="label">Cash</p>
                <p class="value">${formatMoney(market.cash)}</p>
              </div>
              <div>
                <p class="label">P/L</p>
                <p class={`value ${market.realized_pnl >= 0 ? 'pos' : 'neg'}`}>
                  ${formatPrice(market.realized_pnl)}
                </p>
              </div>
              <div>
                <p class="label">Step</p>
                <p class="value">{market.step_index}</p>
              </div>
              <div>
                <p class="label">Active Trades</p>
                <p class="value">{countOpenPositions(market)}</p>
              </div>
              <div>
                <p class="label">Active Sells</p>
                <p class="value">{countActiveSells(market)}</p>
              </div>
            </div>
            <div class="locked-profit">
              <div>
                <p class="label">Locked Cost</p>
                <p class="value">${formatMoney(totalBuyCost(market))}</p>
              </div>
              <div>
                <p class="label">Locked P/L</p>
                <p class={`value ${market.realized_pnl >= 0 ? 'pos' : 'neg'}`}>
                  ${formatMoney(market.realized_pnl)}
                </p>
              </div>
            </div>
            <div class="paper-trades">
              <p class="label">Trades</p>
              {#if market.trades?.length}
                <div class="table">
                  <div class="table-header">
                    <span>Type</span>
                    <span>Side</span>
                    <span class="num">Price</span>
                    <span class="num">Target</span>
                    <span class="num">Slippage</span>
                    <span class="num">Cost</span>
                    <span class="num">Shares</span>
                  </div>
                  {#each market.trades.slice(-6).reverse() as trade}
                    <div class="table-row">
                      <span class={`pill action ${trade.action}`}>{trade.action.toUpperCase()}</span>
                      <span class={`pill side ${trade.side}`}>{trade.side.toUpperCase()}</span>
                      <span class="num">{formatPrice(trade.price)}</span>
                      <span class="num">{formatPrice(trade.target_price)}</span>
                      <span class="num">{formatSigned(trade.slippage)}</span>
                      <span class="num">${formatMoney(trade.notional)}</span>
                      <span class="num">{formatShares(trade.size)} sh</span>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="empty">No paper trades yet.</p>
              {/if}
            </div>
            <div class="paper-trades">
              <p class="label">Sell Orders</p>
              {#if market.sell_orders?.length}
                <div class="table">
                  <div class="table-header">
                    <span>Status</span>
                    <span>Side</span>
                    <span class="num">Price</span>
                    <span class="num">Target</span>
                    <span class="num">Slippage</span>
                    <span class="num">Shares</span>
                    <span class="num">Value</span>
                  </div>
                  {#each market.sell_orders.slice(-6).reverse() as order}
                    <div class="table-row">
                      <span class={`pill status ${order.status}`}>{order.status.toUpperCase()}</span>
                      <span class={`pill side ${order.side}`}>{order.side.toUpperCase()}</span>
                      <span class="num">{formatPrice(order.price)}</span>
                      <span class="num">{formatPrice(order.target_price)}</span>
                      <span class="num">{formatSigned(order.slippage)}</span>
                      <span class="num">{formatShares(order.size)} sh</span>
                      <span class="num">${formatMoney(order.price * order.size)}</span>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="empty">No sell orders yet.</p>
              {/if}
            </div>
          </article>
        {/each}
      </div>
    {:else}
      <div class="empty">Waiting for paper trade data...</div>
    {/if}
  </section>
</main>
