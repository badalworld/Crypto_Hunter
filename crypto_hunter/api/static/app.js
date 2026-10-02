/* Crypto Hunter dashboard – all data arrives over the /ws stream (no polling, no mock data). */
(() => {
  const $ = (s) => document.querySelector(s);
  const token = new URLSearchParams(location.search).get("token") || localStorage.getItem("ch_token") || "";
  if (token) localStorage.setItem("ch_token", token);
  const H = token ? { Authorization: `Bearer ${token}` } : {};

  const S = { status: null, account: null, positions: [], watchlist: [], metrics: null, trades: [], events: [],
              signals: [], equity: [], projection: [], config: null, schema: null };

  // ------------------------------------------------------------ helpers
  const fmt = (v, d = 2) => (v == null || isNaN(v)) ? "–" : Number(v).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
  const px = (v) => { if (v == null) return "–"; v = Number(v); const d = v >= 1000 ? 1 : v >= 1 ? 4 : v >= 0.01 ? 6 : 8; return v.toFixed(d).replace(/0+$/, "").replace(/\.$/, ""); };
  const pct = (v, d = 1, sign = true) => (v == null || isNaN(v)) ? "–" : `${sign && v > 0 ? "+" : ""}${Number(v).toFixed(d)}%`;
  const cls = (v) => v > 0 ? "pos" : v < 0 ? "neg" : "flat";
  const big = (v) => v >= 1e9 ? (v / 1e9).toFixed(2) + "B" : v >= 1e6 ? (v / 1e6).toFixed(1) + "M" : v >= 1e3 ? (v / 1e3).toFixed(0) + "K" : fmt(v, 0);
  const ago = (s) => { s = Math.max(0, s | 0); if (s < 60) return s + "s"; if (s < 3600) return (s / 60 | 0) + "m"; if (s < 86400) return (s / 3600 | 0) + "h " + ((s % 3600) / 60 | 0) + "m"; return (s / 86400 | 0) + "d " + ((s % 86400) / 3600 | 0) + "h"; };
  const t2 = (ts) => new Date(ts * 1000).toLocaleTimeString([], { hour12: false });
  const dt = (ts) => new Date(ts * 1000).toLocaleString([], { hour12: false, month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const toast = (m, k = "") => { const el = document.createElement("div"); el.className = "toast " + k; el.textContent = m; $("#toasts").appendChild(el); setTimeout(() => el.remove(), 5000); };
  async function api(path, method = "GET", body) {
    const r = await fetch(path, { method, headers: { "Content-Type": "application/json", ...H }, body: body ? JSON.stringify(body) : undefined });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.detail || r.statusText);
    return j;
  }

  // ------------------------------------------------------------ websocket
  let ws, retry = 1000;
  function connect() {
    ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws${token ? "?token=" + encodeURIComponent(token) : ""}`);
    ws.onopen = () => { retry = 1000; setPill("#pill-ws", "on"); };
    ws.onclose = () => { setPill("#pill-ws", "off"); setTimeout(connect, retry); retry = Math.min(retry * 1.7, 15000); };
    ws.onmessage = (e) => handle(JSON.parse(e.data));
    setInterval(() => ws.readyState === 1 && ws.send("ping"), 25000);
  }
  function handle(m) {
    switch (m.type) {
      case "snapshot":
        Object.assign(S, { status: m.status, account: m.account, positions: m.positions, watchlist: m.watchlist, metrics: m.metrics });
        if (m.signals) S.signals = m.signals;
        renderAll(); break;
      case "bootstrap":
        S.trades = m.trades; S.events = m.events.reverse(); S.equity = m.equity; S.projection = m.projection; S.config = m.config;
        renderTrades(); renderFeed(); drawChart(); if (S.schema) buildForm(); break;
      case "event": S.events.push(m.event); if (S.events.length > 300) S.events.shift(); renderFeed(true); break;
      case "trade": S.trades.unshift(m.trade); renderTrades(true); toast(`${m.trade.symbol} ${m.trade.side} closed [${m.trade.reason}] ${m.trade.pnl >= 0 ? "+" : ""}${fmt(m.trade.pnl, 3)} USDT`, m.trade.pnl >= 0 ? "ok" : "err"); break;
      case "position": { const i = S.positions.findIndex((p) => p.symbol === m.position.symbol && p.side === m.position.side); if (i >= 0) S.positions[i] = m.position; else S.positions.push(m.position); renderPositions(true); break; }
      case "watchlist": S.watchlist = m.watchlist; renderWatch(); break;
      case "signal": S.signals.unshift(m.signal); S.signals = S.signals.slice(0, 50); renderSignals(); break;
    }
  }

  // ------------------------------------------------------------ render
  function setPill(sel, state, text) { const el = $(sel); el.classList.remove("on", "off", "warn"); if (state) el.classList.add(state); if (text != null) el.querySelector("b") ? (el.querySelector("b").textContent = text) : (el.textContent = text); }
  function renderAll() { renderStatus(); renderKpis(); renderPositions(); renderWatch(); renderLimits(); renderSignals(); }

  function renderStatus() {
    const st = S.status; if (!st) return;
    const map = { RUNNING: "on", STARTING: "warn", PAUSED: "warn", ERROR: "off", STOPPED: "off" };
    setPill("#pill-state", map[st.state] || "", st.state + (st.state_reason ? " · " + st.state_reason : ""));
    setPill("#pill-md", st.ws_public ? "on" : st.running ? "off" : "");
    setPill("#pill-pr", st.ws_private ? "on" : st.running ? "off" : "");
    $("#pill-lat").textContent = (st.rest_latency_ms || 0).toFixed(0) + " ms";
    $("#btn-start").classList.toggle("hidden", st.running);
    $("#btn-stop").classList.toggle("hidden", !st.running);
    $("#btn-start").disabled = !st.has_credentials;
    $("#cred-status").textContent = st.has_credentials ? `Key ${st.masked_key} · ${st.account_ok ? "verified" : "unverified"} · ${st.position_mode} mode` : "No credentials saved";
    $("#cred-status").classList.toggle("ok", !!st.has_credentials && st.account_ok);
  }

  function renderKpis() {
    const a = S.account, m = S.metrics; if (!a || !m) return;
    $("#k-equity").textContent = fmt(a.equity); $("#k-equity-sub").textContent = `${a.currency} · session ${pct(m.session_return_pct)}`;
    $("#k-avail").textContent = fmt(a.available);
    const up = S.positions.reduce((s, p) => s + (p.unrealized_pnl || 0), 0) || a.unrealized;
    $("#k-upnl").textContent = (up >= 0 ? "+" : "") + fmt(up, 3); $("#k-upnl").className = "val mono " + cls(up); $("#k-upnl-sub").textContent = `${S.positions.length} open`;
    const d = m.daily_pnl_equity || m.daily_pnl; $("#k-daily").textContent = (d >= 0 ? "+" : "") + fmt(d, 2); $("#k-daily").className = "val mono " + cls(d);
    $("#k-daily-sub").textContent = `closed ${m.daily_pnl >= 0 ? "+" : ""}${fmt(m.daily_pnl, 2)}`;
    $("#k-wr").textContent = pct(m.win_rate, 0, false); $("#k-wr-sub").textContent = `${m.wins}W / ${m.losses}L · PF ${m.profit_factor == null ? "∞" : fmt(m.profit_factor, 2)}`;
    $("#k-roi").textContent = pct(m.avg_roi); $("#k-roi").className = "val mono " + cls(m.avg_roi); $("#k-roi-sub").textContent = `win ${pct(m.avg_win_roi, 0)} / loss ${pct(m.avg_loss_roi, 0)}`;
    $("#k-dd").textContent = pct(-m.max_drawdown_pct, 1); $("#k-dd").className = "val mono " + (m.max_drawdown_pct > 10 ? "neg" : "flat");
    $("#k-target-amt").textContent = fmt(m.target_equity, 0); $("#k-target").textContent = pct(m.target_progress_pct, 1, false);
    $("#k-target-bar").style.width = Math.min(100, m.target_progress_pct) + "%";
    $("#k-target-sub").textContent = `needs ${fmt(m.required_daily_growth_pct, 1)}%/day · from ${fmt(m.session_start_equity)}`;
    $("#tr-pf").textContent = `${m.trades} trades · net ${m.total_pnl >= 0 ? "+" : ""}${fmt(m.total_pnl, 3)} USDT`;
  }

  function renderPositions(flash) {
    const tb = $("#tbl-pos tbody"); const ps = S.positions.slice().sort((a, b) => b.opened_at - a.opened_at);
    $("#pos-count").textContent = ps.length ? `(${ps.length})` : ""; $("#pos-empty").classList.toggle("hidden", ps.length > 0);
    tb.innerHTML = ps.map((p) => {
      const tp = S.config?.exits?.tp_roi || 200; const w = Math.min(100, Math.abs(p.roi) / tp * 100);
      return `<tr class="${flash ? "flash" : ""}">
        <td><b>${esc(p.symbol)}</b></td><td><span class="tag ${p.side}">${p.side.toUpperCase()}</span></td>
        <td class="mono">${p.vol} <span class="muted">(${fmt(p.margin)}$ ×${p.leverage})</span></td>
        <td class="mono">${px(p.entry_price)}</td><td class="mono">${px(p.mark_price)}</td>
        <td class="mono ${cls(p.unrealized_pnl)}">${p.unrealized_pnl >= 0 ? "+" : ""}${fmt(p.unrealized_pnl, 3)}</td>
        <td class="mono ${cls(p.roi)}">${pct(p.roi)}<div class="roi-bar"><span class="${p.roi < 0 ? "neg" : ""}" style="width:${w}%"></span></div></td>
        <td class="mono">${pct(p.peak_roi)}</td><td class="mono">${px(p.stop_price)}</td>
        <td class="mono ${cls(p.stop_roi_effective)}">${pct(p.stop_roi_effective, 0)}</td><td class="mono">${px(p.tp_price)}</td>
        <td><span class="tag ${p.trailing_active ? "on" : "off"}">${p.trailing_active ? "LOCKED " + pct(p.stop_roi, 0, false) : "ATR SL"}</span></td>
        <td class="muted">${ago(p.age_sec)}</td>
        <td><button class="btn tiny danger ghost" data-close="${esc(p.symbol)}|${p.side}">close</button></td></tr>`; }).join("");
  }

  function renderWatch() {
    const tb = $("#tbl-watch tbody"); $("#wl-count").textContent = S.watchlist.length ? `(${S.watchlist.length})` : "";
    const pos = new Set(S.positions.map((p) => p.symbol));
    tb.innerHTML = S.watchlist.map((w) => `<tr><td class="muted">${w.rank}</td><td><b>${esc(w.symbol)}</b>${pos.has(w.symbol) ? ' <span class="tag on">pos</span>' : ""}</td>
      <td class="mono">${px(w.last)}</td><td class="mono">${fmt(w.atr_pct, 2)}</td><td class="mono">${big(w.amount24)}</td><td class="mono ${cls(w.change24_pct)}">${pct(w.change24_pct)}</td></tr>`).join("");
  }

  function renderTrades(flash) {
    const tb = $("#tbl-trades tbody"); $("#tr-empty").classList.toggle("hidden", S.trades.length > 0);
    tb.innerHTML = S.trades.slice(0, 300).map((t, i) => `<tr class="${flash && i === 0 ? "flash" : ""}"><td class="muted">${dt(t.closed_at)}</td><td><b>${esc(t.symbol)}</b></td>
      <td><span class="tag ${t.side}">${t.side.toUpperCase()}</span></td><td class="mono">${px(t.entry_price)}</td><td class="mono">${px(t.exit_price)}</td>
      <td class="mono ${cls(t.pnl)}">${t.pnl >= 0 ? "+" : ""}${fmt(t.pnl, 3)}</td><td class="mono ${cls(t.roi)}">${pct(t.roi)}</td><td class="mono">${pct(t.peak_roi)}</td>
      <td><span class="tag ${esc(t.reason)}">${esc(t.reason)}</span></td><td class="muted">${ago(t.closed_at - t.opened_at)}</td></tr>`).join("");
  }

  function renderFeed(append) {
    const ul = $("#feed"); const kinds = { entry: "ok", exit: "ok", trail: "info", signal: "ok", signal_rejected: "rej" };
    const items = S.events.slice(-150).reverse();
    ul.innerHTML = items.map((e) => `<li class="${kinds[e.kind] || e.level}"><time>${t2(e.ts)}</time><b class="muted">${esc(e.kind)}</b> ${esc(e.message)}</li>`).join("");
  }

  function renderSignals() {
    $("#signals").innerHTML = S.signals.map((s) => `<li class="${s.passed ? "ok" : "rej"}"><time>${t2(s.ts)}</time><b>${esc(s.symbol)}</b> <span class="tag ${s.side}">${s.side.toUpperCase()}</span>
      mag ${fmt(s.magnitude, 2)} · ATR% ${fmt(s.atr_pct, 2)} · ${s.passed ? "<b class='pos'>PASSED</b>" : "rejected"}
      <div class="chk">${Object.entries(s.checks || {}).map(([k, v]) => `<span class="${v.ok ? "" : "bad"}" title="${esc(v.detail)}">${k}</span>`).join("")}</div></li>`).join("") || '<li class="rej">No AO divergences detected yet</li>';
  }

  function renderLimits() {
    const rl = S.status?.rate_limits || {}; const keys = Object.keys(rl).sort();
    $("#limits").innerHTML = keys.length ? `<div class="lim">${keys.map((k) => `<span class="mono">${esc(k)}</span><span class="mono muted">${rl[k].used}/${rl[k].budget} per ${rl[k].period}s</span><div class="b"><span style="width:${rl[k].pct}%"></span></div>`).join("")}</div>` : '<div class="empty">No API calls yet</div>';
  }

  // ------------------------------------------------------------ chart
  const cv = $("#equity-chart");
  function drawChart() {
    const ctx = cv.getContext("2d"); const dpr = window.devicePixelRatio || 1;
    const W = cv.clientWidth, Hh = 260; cv.width = W * dpr; cv.height = Hh * dpr; ctx.scale(dpr, dpr); ctx.clearRect(0, 0, W, Hh);
    const eq = S.equity, pj = S.projection, target = S.metrics?.target_equity || S.config?.risk?.target_equity_usdt || 10000;
    if (!eq.length && !pj.length) { ctx.fillStyle = "#8b93ad"; ctx.font = "13px Inter"; ctx.fillText("Equity snapshots appear once the bot is running", 20, 40); return; }
    const now = Date.now() / 1000;
    const pts = eq.concat(S.account && S.account.ts ? [{ ts: now, equity: S.account.equity }] : []);
    const xs = pts.map((p) => p.ts).concat(pj.map((p) => p.ts)); const x0 = Math.min(...xs), x1 = Math.max(...xs, now + 3600);
    const ysAll = pts.map((p) => p.equity).concat(pj.map((p) => p.equity)); let y0 = Math.min(...ysAll), y1 = Math.max(...ysAll);
    y1 = Math.max(y1, Math.min(target, y1 * 1.5)); const pad = (y1 - y0) * 0.08 || 1; y0 -= pad; y1 += pad;
    const L = 56, R = 12, T = 12, B = 24; const X = (t) => L + (t - x0) / (x1 - x0) * (W - L - R), Y = (v) => T + (1 - (v - y0) / (y1 - y0)) * (Hh - T - B);
    ctx.strokeStyle = "rgba(255,255,255,.07)"; ctx.fillStyle = "#8b93ad"; ctx.font = "11px JetBrains Mono"; ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) { const v = y0 + (y1 - y0) * i / 4, y = Y(v); ctx.beginPath(); ctx.moveTo(L, y); ctx.lineTo(W - R, y); ctx.stroke(); ctx.fillText(v >= 1000 ? (v / 1000).toFixed(1) + "k" : v.toFixed(1), 4, y + 4); }
    for (let i = 0; i <= 6; i++) { const t = x0 + (x1 - x0) * i / 6; ctx.fillText(new Date(t * 1000).toLocaleDateString([], { month: "short", day: "numeric" }) + " " + new Date(t * 1000).getHours() + "h", X(t) - 20, Hh - 6); }
    if (target >= y0 && target <= y1) { ctx.setLineDash([6, 6]); ctx.strokeStyle = "#fbbf24"; ctx.beginPath(); ctx.moveTo(L, Y(target)); ctx.lineTo(W - R, Y(target)); ctx.stroke(); ctx.setLineDash([]); }
    if (pj.length) { ctx.strokeStyle = "rgba(167,139,250,.9)"; ctx.lineWidth = 1.5; ctx.setLineDash([3, 4]); ctx.beginPath(); pj.forEach((p, i) => i ? ctx.lineTo(X(p.ts), Y(p.equity)) : ctx.moveTo(X(p.ts), Y(p.equity))); ctx.stroke(); ctx.setLineDash([]); }
    if (pts.length) {
      const g = ctx.createLinearGradient(0, T, 0, Hh); g.addColorStop(0, "rgba(110,231,255,.35)"); g.addColorStop(1, "rgba(110,231,255,0)");
      ctx.beginPath(); pts.forEach((p, i) => i ? ctx.lineTo(X(p.ts), Y(p.equity)) : ctx.moveTo(X(p.ts), Y(p.equity)));
      ctx.lineTo(X(pts[pts.length - 1].ts), Hh - B); ctx.lineTo(X(pts[0].ts), Hh - B); ctx.closePath(); ctx.fillStyle = g; ctx.fill();
      ctx.beginPath(); pts.forEach((p, i) => i ? ctx.lineTo(X(p.ts), Y(p.equity)) : ctx.moveTo(X(p.ts), Y(p.equity)));
      ctx.strokeStyle = "#6ee7ff"; ctx.lineWidth = 2; ctx.shadowColor = "rgba(110,231,255,.6)"; ctx.shadowBlur = 8; ctx.stroke(); ctx.shadowBlur = 0;
      const last = pts[pts.length - 1]; ctx.fillStyle = "#6ee7ff"; ctx.beginPath(); ctx.arc(X(last.ts), Y(last.equity), 4, 0, 7); ctx.fill();
    }
  }
  window.addEventListener("resize", drawChart);
  setInterval(() => { if (S.account && S.status?.running) { const l = S.equity[S.equity.length - 1]; if (!l || Date.now() / 1000 - l.ts > 30) { S.equity.push({ ts: Date.now() / 1000, equity: S.account.equity }); if (S.equity.length > 5000) S.equity.shift(); } } drawChart(); }, 5000);

  // ------------------------------------------------------------ settings form
  const GROUPS = { risk: "Position sizing & risk", exits: "Take-profit & stepped trailing stop (ROI on margin)", filters: "Fake-signal filters", scanner: "Asset selection (scanner)", signal: "Signal (AO divergence)", execution: "Execution & API" };
  function buildForm() {
    const f = $("#cfg-form"); const defs = S.schema.$defs || {}; const cfg = S.config;
    f.innerHTML = Object.entries(GROUPS).map(([g, title]) => {
      const ref = S.schema.properties[g].$ref.split("/").pop(); const props = defs[ref].properties;
      return `<section class="set-group"><h3>${title}</h3>${Object.entries(props).map(([k, p]) => {
        const v = cfg[g][k]; const id = `${g}.${k}`; const desc = p.description ? `<span class="desc">${esc(p.description)}</span>` : "";
        if (p.type === "boolean") return `<label class="check"><input type="checkbox" data-k="${id}" ${v ? "checked" : ""}><b>${k}</b>${desc}</label>`;
        if (p.enum) return `<label><b>${k}</b><select data-k="${id}">${p.enum.map((o) => `<option ${o === v ? "selected" : ""}>${o}</option>`).join("")}</select>${desc}</label>`;
        if (p.type === "array") return `<label><b>${k}</b><input data-k="${id}" data-arr="1" value="${esc((v || []).join(","))}" placeholder="comma separated">${desc}</label>`;
        const num = p.type === "integer" || p.type === "number"; return `<label><b>${k}</b><input data-k="${id}" ${num ? `type="number" step="any"` : ""} value="${esc(v)}">${desc}</label>`;
      }).join("")}</section>`; }).join("");
    f.querySelectorAll("[data-k]").forEach((el) => { if (el.type === "number" || el.tagName === "SELECT" || el.type === "checkbox") return; });
  }
  function collectPatch() {
    const patch = {};
    $("#cfg-form").querySelectorAll("[data-k]").forEach((el) => {
      const [g, k] = el.dataset.k.split("."); const cur = S.config[g][k]; let v;
      if (el.type === "checkbox") v = el.checked; else if (el.dataset.arr) v = el.value.split(",").map((s) => s.trim()).filter(Boolean); else if (el.type === "number") v = el.value === "" ? cur : Number(el.value); else v = el.value;
      if (JSON.stringify(v) !== JSON.stringify(cur)) { (patch[g] ||= {})[k] = v; }
    });
    return patch;
  }

  // ------------------------------------------------------------ actions
  $("#btn-start").onclick = async () => { try { await api("/api/bot/start", "POST"); toast("Bot started", "ok"); } catch (e) { toast(e.message, "err"); } };
  $("#btn-stop").onclick = async () => { if (!confirm("Stop the bot? Open positions keep their exchange-side TP/SL but trailing stops will not update.")) return; try { await api("/api/bot/stop", "POST"); } catch (e) { toast(e.message, "err"); } };
  $("#btn-close-all").onclick = async () => { if (!confirm("Market-close ALL managed positions?")) return; try { const r = await api("/api/positions/close_all", "POST"); toast(`Closing ${r.closed} positions`); } catch (e) { toast(e.message, "err"); } };
  $("#tbl-pos").addEventListener("click", async (e) => { const b = e.target.closest("[data-close]"); if (!b) return; const [s, side] = b.dataset.close.split("|"); if (!confirm(`Market-close ${side} ${s}?`)) return; try { await api(`/api/positions/${s}/${side}/close`, "POST"); } catch (err) { toast(err.message, "err"); } });
  const openDrawer = (o) => { $("#drawer").classList.toggle("hidden", !o); $("#drawer-bg").classList.toggle("hidden", !o); };
  $("#btn-settings").onclick = async () => { openDrawer(true); if (!S.schema) { const r = await api("/api/config"); S.schema = r.schema; S.config = r.config; buildForm(); } };
  $("#btn-close-drawer").onclick = $("#drawer-bg").onclick = () => openDrawer(false);
  $("#btn-save-creds").onclick = async () => {
    const m = $("#cred-msg"); m.className = "msg"; m.textContent = "Verifying with MEXC…";
    try { const r = await api("/api/credentials", "POST", { api_key: $("#in-key").value, api_secret: $("#in-secret").value }); m.className = "msg ok"; m.textContent = `Verified · equity ${fmt(r.equity)} USDT`; $("#in-key").value = $("#in-secret").value = ""; }
    catch (e) { m.className = "msg err"; m.textContent = e.message; }
  };
  $("#btn-del-creds").onclick = async () => { if (!confirm("Remove stored API credentials? The bot will stop.")) return; await api("/api/credentials", "DELETE"); toast("Credentials removed"); };
  $("#btn-save-cfg").onclick = async () => {
    const m = $("#cfg-msg"); const patch = collectPatch(); if (!Object.keys(patch).length) { m.className = "msg"; m.textContent = "No changes"; return; }
    try { const r = await api("/api/config", "PUT", { patch }); S.config = r.config; buildForm(); m.className = "msg ok"; m.textContent = "Applied"; toast("Configuration applied", "ok"); } catch (e) { m.className = "msg err"; m.textContent = e.message; }
  };
  document.querySelectorAll(".tab").forEach((t) => t.onclick = () => { document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === t)); document.querySelectorAll(".tabpane").forEach((p) => p.classList.toggle("hidden", p.id !== "pane-" + t.dataset.tab)); });
  setInterval(() => { if (S.positions.length) renderPositions(); }, 1000);

  connect();
})();
