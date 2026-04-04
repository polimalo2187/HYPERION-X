const state = {
  token: null,
  session: null,
  telegram: null,
  isAdmin: false,
};

const elements = {
  statusBanner: document.getElementById('statusBanner'),
  refreshButton: document.getElementById('refreshButton'),
  userPlanBadge: document.getElementById('userPlanBadge'),
  userIdentity: document.getElementById('userIdentity'),
  userTradingStatus: document.getElementById('userTradingStatus'),
  userReadiness: document.getElementById('userReadiness'),
  walletBadge: document.getElementById('walletBadge'),
  walletConfigured: document.getElementById('walletConfigured'),
  privateKeyConfigured: document.getElementById('privateKeyConfigured'),
  planActive: document.getElementById('planActive'),
  planExpiresAt: document.getElementById('planExpiresAt'),
  dashboardStats: document.getElementById('dashboardStats'),
  lastOpen: document.getElementById('lastOpen'),
  lastClose: document.getElementById('lastClose'),
  operationsCount: document.getElementById('operationsCount'),
  operationsList: document.getElementById('operationsList'),
  referralStats: document.getElementById('referralStats'),
  adminVisualStats: document.getElementById('adminVisualStats'),
  adminTradeStats: document.getElementById('adminTradeStats'),
  adminTabButton: document.getElementById('adminTabButton'),
};

function setStatus(message, variant = 'info') {
  elements.statusBanner.className = `status-banner ${variant}`;
  elements.statusBanner.textContent = message;
}

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function pretty(value) {
  if (!value) return 'Sin datos';
  try {
    return JSON.stringify(value, null, 2);
  } catch (error) {
    return String(value);
  }
}

function badgeClassForStatus(value) {
  const normalized = String(value || '').toLowerCase();
  if (['ready', 'active', 'premium', 'true', 'configured'].includes(normalized)) return 'success';
  if (['trial', 'warning'].includes(normalized)) return 'warning';
  if (['inactive', 'false', 'not_ready', 'none'].includes(normalized)) return 'danger';
  return 'neutral';
}

function buildKpiCard(label, value, subtext = '') {
  const article = document.createElement('article');
  article.className = 'kpi-card';
  article.innerHTML = `
    <span class="kpi-label">${label}</span>
    <div class="kpi-value">${value}</div>
    <div class="kpi-subtext">${subtext}</div>
  `;
  return article;
}

function renderDashboard(data) {
  elements.userPlanBadge.textContent = data.plan || 'none';
  elements.userPlanBadge.className = `badge ${badgeClassForStatus(data.plan_active ? data.plan : 'none')}`;
  elements.userIdentity.textContent = data.username ? `@${data.username}` : `ID ${data.user_id}`;
  elements.userTradingStatus.textContent = data.trading_status || 'inactive';
  elements.userReadiness.textContent = data.status_summary || 'not_ready';
  elements.walletBadge.textContent = data.wallet_configured ? 'wallet ok' : 'wallet pendiente';
  elements.walletBadge.className = `badge ${badgeClassForStatus(data.wallet_configured ? 'configured' : 'false')}`;
  elements.walletConfigured.textContent = data.wallet_configured ? 'Configurada' : 'Falta configurar';
  elements.privateKeyConfigured.textContent = data.private_key_configured ? 'Configurada' : 'Falta configurar';
  elements.planActive.textContent = data.plan_active ? 'Sí' : 'No';
  elements.planExpiresAt.textContent = formatDate(data.plan_expires_at);

  elements.dashboardStats.innerHTML = '';
  elements.dashboardStats.append(
    buildKpiCard('Plan', data.plan || 'none', data.plan_active ? 'Activo' : 'Sin acceso activo'),
    buildKpiCard('Trading status', data.trading_status || 'inactive', data.wallet_configured && data.private_key_configured ? 'Configuración mínima cargada' : 'Configuración incompleta'),
    buildKpiCard('Wallet', data.wallet || 'No configurada', data.wallet_configured ? 'Lista para operar' : 'Pendiente en el perfil'),
    buildKpiCard('Balance exchange', data.exchange_balance !== undefined ? data.exchange_balance : 'No consultado', 'Se solicita al backend cuando aplica'),
    buildKpiCard('Trial usado', data.trial_used ? 'Sí' : 'No', 'Control del clon actual'),
    buildKpiCard('Términos', data.terms_accepted ? 'Aceptados' : 'Pendientes', 'Estado actual del usuario')
  );
}

function renderOperations(data) {
  elements.lastOpen.textContent = pretty(data.last_open);
  elements.lastClose.textContent = pretty(data.last_close);
  elements.operationsCount.textContent = String(data.count || 0);
  if (!Array.isArray(data.trades) || data.trades.length === 0) {
    elements.operationsList.className = 'list-stack empty-state';
    elements.operationsList.textContent = 'Sin operaciones recientes.';
    return;
  }

  elements.operationsList.className = 'list-stack';
  elements.operationsList.innerHTML = '';
  data.trades.forEach((trade) => {
    const item = document.createElement('article');
    item.className = 'list-item';
    const profit = Number(trade.profit || 0);
    const profitLabel = Number.isFinite(profit) ? profit.toFixed(4) : String(trade.profit || '0');
    const badgeClass = profit > 0 ? 'success' : (profit < 0 ? 'danger' : 'neutral');
    item.innerHTML = `
      <div class="list-item-header">
        <div>
          <div class="list-item-title">${trade.symbol || 'Trade'} · ${trade.side || 'N/A'}</div>
          <div class="list-item-meta">${formatDate(trade.timestamp)}</div>
        </div>
        <span class="badge ${badgeClass}">${profitLabel}</span>
      </div>
      <div class="list-item-grid">
        <div><span class="metric-label">Entry</span><strong>${trade.entry_price ?? '—'}</strong></div>
        <div><span class="metric-label">Exit</span><strong>${trade.exit_price ?? '—'}</strong></div>
        <div><span class="metric-label">Qty</span><strong>${trade.qty ?? '—'}</strong></div>
        <div><span class="metric-label">Best score</span><strong>${trade.best_score ?? '—'}</strong></div>
      </div>
    `;
    elements.operationsList.appendChild(item);
  });
}

function renderReferrals(data) {
  elements.referralStats.innerHTML = '';
  elements.referralStats.append(
    buildKpiCard('Usuario', data.user_id || '—', 'ID del usuario autenticado'),
    buildKpiCard('Referidos válidos', data.referral_valid_count || 0, 'Contados sobre este clon')
  );
}

function renderAdmin(data) {
  const visual = data.visual || {};
  const tradeStats = data.trade_stats_30d || {};

  elements.adminVisualStats.innerHTML = '';
  elements.adminVisualStats.append(
    buildKpiCard('Usuarios totales', visual.total_users || 0, 'Registrados en esta base'),
    buildKpiCard('Free/Trial vencido', visual.free_old || 0, 'Usuarios sin premium activo'),
    buildKpiCard('Premium activo', visual.premium_active || 0, 'Estado actual'),
    buildKpiCard('Premium vencido', visual.premium_expired || 0, 'Caducados en la base')
  );

  elements.adminTradeStats.innerHTML = '';
  elements.adminTradeStats.append(
    buildKpiCard('Total trades', tradeStats.total || 0, 'Últimos 30 días'),
    buildKpiCard('Wins', tradeStats.wins || 0, `Win rate ${tradeStats.win_rate ?? 0}%`),
    buildKpiCard('Losses', tradeStats.losses || 0, `Win rate decisivo ${tradeStats.win_rate_decisive ?? 0}%`),
    buildKpiCard('Profit factor', tradeStats.profit_factor ?? 0, `PnL total ${tradeStats.pnl_total ?? 0}`),
    buildKpiCard('Gross profit', tradeStats.gross_profit ?? 0, 'Suma de resultados positivos'),
    buildKpiCard('Gross loss', tradeStats.gross_loss ?? 0, 'Suma absoluta de pérdidas')
  );
}

async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.token) {
    headers.set('Authorization', `Bearer ${state.token}`);
  }
  if (options.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  const response = await fetch(path, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || `Error HTTP ${response.status}`);
  }

  return response.json();
}

async function authenticate() {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  if (!tg) {
    throw new Error('Telegram WebApp no está disponible. Abre esta URL desde el botón del bot.');
  }

  state.telegram = tg;
  tg.ready();
  tg.expand();
  tg.setHeaderColor('#0c111b');
  tg.setBackgroundColor('#0c111b');

  if (!tg.initData) {
    throw new Error('initData ausente. Debes abrir la MiniApp desde Telegram.');
  }

  const payload = await apiFetch('/api/v1/auth/telegram', {
    method: 'POST',
    body: JSON.stringify({ init_data: tg.initData }),
  });

  state.token = payload.access_token;
  state.session = payload.user;
}

async function loadData() {
  setStatus('Sincronizando datos del backend...', 'info');
  const [dashboard, operations, referrals] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/operations?limit=20'),
    apiFetch('/api/v1/referrals'),
  ]);

  renderDashboard(dashboard);
  renderOperations(operations);
  renderReferrals(referrals);

  state.isAdmin = !!state.session && Number(state.session.user_id) > 0 && (state.session.user_id === referrals.user_id) && !!window.Telegram?.WebApp;
  if (state.session && (state.session.user_id || state.session.username)) {
    setStatus('MiniApp conectada correctamente al backend.', 'success');
  }

  try {
    const admin = await apiFetch('/api/v1/admin/overview');
    renderAdmin(admin);
    elements.adminTabButton.classList.remove('hidden');
    state.isAdmin = true;
  } catch (error) {
    elements.adminTabButton.classList.add('hidden');
    state.isAdmin = false;
  }
}

function bindTabs() {
  const buttons = Array.from(document.querySelectorAll('.tab'));
  const panels = Array.from(document.querySelectorAll('.panel'));
  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      const target = button.dataset.tab;
      buttons.forEach((item) => item.classList.toggle('is-active', item === button));
      panels.forEach((panel) => panel.classList.toggle('is-active', panel.dataset.panel === target));
    });
  });
}

async function bootstrap() {
  bindTabs();
  elements.refreshButton.addEventListener('click', async () => {
    elements.refreshButton.disabled = true;
    try {
      await loadData();
    } catch (error) {
      setStatus(error.message || 'No se pudieron refrescar los datos.', 'error');
    } finally {
      elements.refreshButton.disabled = false;
    }
  });

  try {
    await authenticate();
    await loadData();
  } catch (error) {
    console.error(error);
    setStatus(error.message || 'No se pudo iniciar la MiniApp.', 'error');
  }
}

bootstrap();
