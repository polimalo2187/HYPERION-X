const state = {
  token: null,
  telegram: null,
  session: null,
  isAdmin: false,
};

const elements = {
  statusBanner: document.getElementById('statusBanner'),
  refreshButton: document.getElementById('refreshButton'),
  headerConnectionBadge: document.getElementById('headerConnectionBadge'),
  heroSignalState: document.getElementById('heroSignalState'),
  heroPlan: document.getElementById('heroPlan'),
  heroTrading: document.getElementById('heroTrading'),
  heroUser: document.getElementById('heroUser'),
  heroUserSubtext: document.getElementById('heroUserSubtext'),
  heroWallet: document.getElementById('heroWallet'),
  heroWalletSubtext: document.getElementById('heroWalletSubtext'),
  heroKey: document.getElementById('heroKey'),
  heroKeySubtext: document.getElementById('heroKeySubtext'),
  heroAccess: document.getElementById('heroAccess'),
  heroAccessSubtext: document.getElementById('heroAccessSubtext'),
  userPlanBadge: document.getElementById('userPlanBadge'),
  userStatusBadge: document.getElementById('userStatusBadge'),
  walletBadge: document.getElementById('walletBadge'),
  userIdentity: document.getElementById('userIdentity'),
  userIdentitySubtext: document.getElementById('userIdentitySubtext'),
  userTradingStatus: document.getElementById('userTradingStatus'),
  userReadiness: document.getElementById('userReadiness'),
  walletConfigured: document.getElementById('walletConfigured'),
  privateKeyConfigured: document.getElementById('privateKeyConfigured'),
  dashboardStats: document.getElementById('dashboardStats'),
  readinessList: document.getElementById('readinessList'),
  insightAccount: document.getElementById('insightAccount'),
  insightReadiness: document.getElementById('insightReadiness'),
  insightAuth: document.getElementById('insightAuth'),
  insightExpiry: document.getElementById('insightExpiry'),
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

function pillClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (['ready', 'active', 'premium', 'configured', 'connected', 'ok', 'sí', 'yes', 'true'].includes(normalized)) return 'success';
  if (['trial', 'warning', 'partial', 'preview'].includes(normalized)) return 'warning';
  if (['inactive', 'false', 'not_ready', 'none', 'missing', 'error', 'offline', 'no', 'blocked'].includes(normalized)) return 'danger';
  if (['info', 'loading'].includes(normalized)) return 'info';
  return 'neutral';
}

function setPill(element, label, valueForClass = label) {
  if (!element) return;
  element.textContent = label;
  element.className = `status-pill ${pillClass(valueForClass)}`;
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
  } catch {
    return String(value);
  }
}

function truncateMiddle(value, max = 18) {
  const text = String(value || '');
  if (!text) return '—';
  if (text.length <= max) return text;
  const side = Math.floor((max - 3) / 2);
  return `${text.slice(0, side)}...${text.slice(-side)}`;
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

function buildReadinessItem(label, description, status) {
  const article = document.createElement('article');
  article.className = `readiness-item ${pillClass(status)}`;
  article.innerHTML = `
    <span class="readiness-dot"></span>
    <div class="readiness-copy">
      <strong>${label}</strong>
      <span>${description}</span>
    </div>
    <span class="status-pill ${pillClass(status)}">${status}</span>
  `;
  return article;
}

function renderDashboard(data) {
  const usernameText = data.username ? `@${data.username}` : `ID ${data.user_id || '—'}`;
  const readinessText = data.status_summary || 'not_ready';
  const tradingText = data.trading_status || 'inactive';
  const planText = data.plan || 'none';
  const accessText = data.plan_active ? 'Activo' : 'Bloqueado';
  const walletText = data.wallet_configured ? truncateMiddle(data.wallet || 'Configurada', 16) : 'Pendiente';
  const authText = state.token ? 'Autenticado' : 'Sin auth';

  setPill(elements.headerConnectionBadge, 'Conectado', 'connected');
  setPill(elements.userPlanBadge, planText, data.plan_active ? planText : 'none');
  setPill(elements.userStatusBadge, readinessText, readinessText);
  setPill(elements.walletBadge, data.wallet_configured ? 'Wallet ok' : 'Pendiente', data.wallet_configured ? 'configured' : 'missing');

  elements.heroSignalState.textContent = data.plan_active ? 'Sesión activa' : 'Acceso limitado';
  elements.heroPlan.textContent = planText;
  elements.heroTrading.textContent = tradingText;
  elements.heroUser.textContent = usernameText;
  elements.heroUserSubtext.textContent = data.plan_active ? 'Usuario autenticado correctamente' : 'Sin plan operativo activo';
  elements.heroWallet.textContent = walletText;
  elements.heroWalletSubtext.textContent = data.wallet_configured ? 'Wallet cargada en backend' : 'Falta configurar wallet';
  elements.heroKey.textContent = data.private_key_configured ? 'Cargada' : 'Falta';
  elements.heroKeySubtext.textContent = data.private_key_configured ? 'Llave presente en sistema' : 'Llave privada pendiente';
  elements.heroAccess.textContent = accessText;
  elements.heroAccessSubtext.textContent = formatDate(data.plan_expires_at);

  elements.userIdentity.textContent = usernameText;
  elements.userIdentitySubtext.textContent = data.user_id ? `Telegram ID ${data.user_id}` : 'Usuario no resuelto';
  elements.userReadiness.textContent = readinessText;
  elements.userTradingStatus.textContent = `Trading ${tradingText}`;
  elements.walletConfigured.textContent = data.wallet_configured ? 'Wallet configurada' : 'Wallet pendiente';
  elements.privateKeyConfigured.textContent = data.private_key_configured ? 'Private key cargada' : 'Private key faltante';

  elements.insightAccount.textContent = usernameText;
  elements.insightReadiness.textContent = readinessText;
  elements.insightAuth.textContent = authText;
  elements.insightExpiry.textContent = formatDate(data.plan_expires_at);

  elements.dashboardStats.innerHTML = '';
  elements.dashboardStats.append(
    buildKpiCard('Plan activo', planText, data.plan_active ? 'El acceso está habilitado.' : 'No hay acceso activo.'),
    buildKpiCard('Trading status', tradingText, 'Estado operativo reportado por backend.'),
    buildKpiCard('Balance exchange', data.exchange_balance !== undefined ? data.exchange_balance : 'No consultado', 'Consulta opcional al backend.'),
    buildKpiCard('Términos', data.terms_accepted ? 'Aceptados' : 'Pendientes', 'Estado actual del usuario.'),
    buildKpiCard('Trial usado', data.trial_used ? 'Sí' : 'No', 'Controlado por la base del clon.'),
    buildKpiCard('Vencimiento', formatDate(data.plan_expires_at), 'Fecha registrada para este usuario.')
  );

  elements.readinessList.innerHTML = '';
  elements.readinessList.append(
    buildReadinessItem('Wallet', data.wallet_configured ? 'La wallet ya está cargada en el sistema.' : 'Todavía falta configurar la wallet.', data.wallet_configured ? 'configured' : 'missing'),
    buildReadinessItem('Private key', data.private_key_configured ? 'La private key existe en la configuración.' : 'Aún falta la private key.', data.private_key_configured ? 'configured' : 'missing'),
    buildReadinessItem('Plan', data.plan_active ? `El usuario tiene acceso ${planText}.` : 'No hay plan activo en este momento.', data.plan_active ? 'active' : 'inactive'),
    buildReadinessItem('Trading', tradingText === 'active' ? 'El bot reporta trading activo.' : 'El bot no está en estado activo.', tradingText)
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
        <span class="status-pill ${badgeClass}">${profitLabel}</span>
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
    buildKpiCard('Usuario', data.user_id || '—', 'ID autenticado contra el backend.'),
    buildKpiCard('Referidos válidos', data.referral_valid_count || 0, 'Contados sobre este clon.')
  );
}

function renderAdmin(data) {
  const visual = data.visual || {};
  const tradeStats = data.trade_stats_30d || {};

  elements.adminVisualStats.innerHTML = '';
  elements.adminVisualStats.append(
    buildKpiCard('Usuarios totales', visual.total_users || 0, 'Registrados en esta base.'),
    buildKpiCard('Free / trial vencido', visual.free_old || 0, 'Usuarios sin premium activo.'),
    buildKpiCard('Premium activo', visual.premium_active || 0, 'Estado actual en la base.'),
    buildKpiCard('Premium vencido', visual.premium_expired || 0, 'Usuarios caducados.')
  );

  elements.adminTradeStats.innerHTML = '';
  elements.adminTradeStats.append(
    buildKpiCard('Total trades', tradeStats.total || 0, 'Últimos 30 días.'),
    buildKpiCard('Wins', tradeStats.wins || 0, `Win rate ${tradeStats.win_rate ?? 0}%`),
    buildKpiCard('Losses', tradeStats.losses || 0, `Decisivas ${tradeStats.win_rate_decisive ?? 0}%`),
    buildKpiCard('Profit factor', tradeStats.profit_factor ?? 0, `PnL total ${tradeStats.pnl_total ?? 0}`),
    buildKpiCard('Gross profit', tradeStats.gross_profit ?? 0, 'Suma de cierres positivos.'),
    buildKpiCard('Gross loss', tradeStats.gross_loss ?? 0, 'Pérdidas acumuladas.')
  );
}

async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.token) headers.set('Authorization', `Bearer ${state.token}`);
  if (options.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');

  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || `Error HTTP ${response.status}`);
  }
  return response.json();
}

function setDirectBrowserMode() {
  setPill(elements.headerConnectionBadge, 'Preview', 'preview');
  setStatus('Abriste la URL directa. El diseño se puede ver, pero la sesión real solo llega al abrir la MiniApp desde Telegram.', 'warning');
  elements.heroSignalState.textContent = 'Abrir desde Telegram';
  elements.heroPlan.textContent = 'Preview';
  elements.heroTrading.textContent = 'Preview';
  elements.heroUser.textContent = 'Modo visual';
  elements.heroUserSubtext.textContent = 'La autenticación real llega desde Telegram WebApp';
  elements.heroWallet.textContent = '—';
  elements.heroWalletSubtext.textContent = 'Sin sesión';
  elements.heroKey.textContent = '—';
  elements.heroKeySubtext.textContent = 'Sin sesión';
  elements.heroAccess.textContent = 'Bloqueado';
  elements.heroAccessSubtext.textContent = 'Necesita initData';
  setPill(elements.userPlanBadge, 'Preview', 'preview');
  setPill(elements.userStatusBadge, 'Sin auth', 'blocked');
  setPill(elements.walletBadge, 'Sin auth', 'blocked');
  elements.userIdentity.textContent = 'Modo visual';
  elements.userIdentitySubtext.textContent = 'Abre la MiniApp desde Telegram';
  elements.userReadiness.textContent = 'Pendiente';
  elements.userTradingStatus.textContent = 'Sin sesión';
  elements.walletConfigured.textContent = 'No disponible';
  elements.privateKeyConfigured.textContent = 'No disponible';
  elements.insightAccount.textContent = 'Sin sesión';
  elements.insightReadiness.textContent = 'Preview';
  elements.insightAuth.textContent = 'Pendiente';
  elements.insightExpiry.textContent = '—';
  elements.refreshButton.disabled = true;
}

async function authenticate() {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  if (!tg) {
    setDirectBrowserMode();
    throw new Error('Telegram WebApp no está disponible.');
  }

  state.telegram = tg;
  tg.ready();
  tg.expand();
  tg.setHeaderColor('#09111f');
  tg.setBackgroundColor('#060b16');

  if (!tg.initData) {
    setDirectBrowserMode();
    throw new Error('Abre esta MiniApp desde Telegram para autenticar la sesión.');
  }

  const payload = await apiFetch('/api/v1/auth/telegram', {
    method: 'POST',
    body: JSON.stringify({ init_data: tg.initData }),
  });

  state.token = payload.access_token;
  state.session = payload.user;
}

async function loadData() {
  setStatus('Sincronizando panel con el backend...', 'info');
  const [dashboard, operations, referrals] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/operations?limit=20'),
    apiFetch('/api/v1/referrals'),
  ]);

  renderDashboard(dashboard);
  renderOperations(operations);
  renderReferrals(referrals);

  setStatus('MiniApp conectada correctamente al backend.', 'success');

  try {
    const admin = await apiFetch('/api/v1/admin/overview');
    renderAdmin(admin);
    elements.adminTabButton.classList.remove('hidden');
    state.isAdmin = true;
  } catch {
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
    if (!state.token) return;
    await loadData();
  } catch (error) {
    console.error(error);
    if (state.token) {
      setStatus(error.message || 'No se pudo iniciar la MiniApp.', 'error');
    }
  }
}

bootstrap();
