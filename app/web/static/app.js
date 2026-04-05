const state = {
  token: null,
  telegram: null,
  isAdmin: false,
  dashboard: null,
  control: null,
  performance: null,
  operations: null,
  referrals: null,
  admin: null,
  adminSelectedUser: null,
};

const $ = (id) => document.getElementById(id);

const elements = {
  statusBanner: $('statusBanner'),
  refreshButton: $('refreshButton'),
  connectionBadge: $('connectionBadge'),
  adminTabButton: $('adminTabButton'),
  heroSessionPill: $('heroSessionPill'),
  heroPlanPill: $('heroPlanPill'),
  heroTradingPill: $('heroTradingPill'),
  heroUser: $('heroUser'),
  heroUserSubtext: $('heroUserSubtext'),
  heroWallet: $('heroWallet'),
  heroWalletSubtext: $('heroWalletSubtext'),
  heroKey: $('heroKey'),
  heroKeySubtext: $('heroKeySubtext'),
  heroAccess: $('heroAccess'),
  heroAccessSubtext: $('heroAccessSubtext'),
  userPlanBadge: $('userPlanBadge'),
  userReadinessBadge: $('userReadinessBadge'),
  walletBadge: $('walletBadge'),
  userIdentity: $('userIdentity'),
  userIdentitySubtext: $('userIdentitySubtext'),
  userReadiness: $('userReadiness'),
  userTradingStatus: $('userTradingStatus'),
  walletConfigured: $('walletConfigured'),
  privateKeyConfigured: $('privateKeyConfigured'),
  dashboardStats: $('dashboardStats'),
  readinessList: $('readinessList'),
  performanceGrid: $('performanceGrid'),
  referralStats: $('referralStats'),
  controlReadinessBox: $('controlReadinessBox'),
  controlStats: $('controlStats'),
  termsStatusText: $('termsStatusText'),
  walletInput: $('walletInput'),
  privateKeyInput: $('privateKeyInput'),
  configurationForm: $('configurationForm'),
  saveConfigurationButton: $('saveConfigurationButton'),
  clearConfigurationButton: $('clearConfigurationButton'),
  acceptTermsButton: $('acceptTermsButton'),
  activateTradingButton: $('activateTradingButton'),
  pauseTradingButton: $('pauseTradingButton'),
  lastOpen: $('lastOpen'),
  lastClose: $('lastClose'),
  operationsCount: $('operationsCount'),
  operationsList: $('operationsList'),
  adminVisualStats: $('adminVisualStats'),
  adminSecurityStats: $('adminSecurityStats'),
  adminTradeStats: $('adminTradeStats'),
  adminSearchForm: $('adminSearchForm'),
  adminSearchInput: $('adminSearchInput'),
  adminSearchButton: $('adminSearchButton'),
  adminSearchResults: $('adminSearchResults'),
  adminUserDetail: $('adminUserDetail'),
  adminUserTitle: $('adminUserTitle'),
  adminUserSubtitle: $('adminUserSubtitle'),
  adminActivatePremiumButton: $('adminActivatePremiumButton'),
  adminUserStats: $('adminUserStats'),
};

function setStatus(message, variant = 'info') {
  elements.statusBanner.className = `status-banner ${variant}`;
  elements.statusBanner.textContent = message;
}

function pillClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (['ready', 'active', 'premium', 'configured', 'connected', 'ok', 'sí', 'yes', 'true', 'activo'].includes(normalized)) return 'success';
  if (['trial', 'warning', 'partial', 'preview', 'pendiente'].includes(normalized)) return 'warning';
  if (['inactive', 'false', 'not_ready', 'none', 'missing', 'error', 'offline', 'no', 'blocked', 'bloqueado'].includes(normalized)) return 'danger';
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

function formatNumber(value, digits = 2) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return '0';
  return numeric.toFixed(digits);
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

function buildPerformanceCard(windowLabel, stats) {
  const article = document.createElement('article');
  article.className = 'performance-card';
  article.innerHTML = `
    <span class="kpi-label">Ventana</span>
    <div class="performance-value">${windowLabel}</div>
    <div class="performance-subtext">PF ${stats.profit_factor === Infinity ? '∞' : formatNumber(stats.profit_factor, 2)} · Win ${formatNumber(stats.win_rate, 2)}%</div>
    <div class="performance-metrics">
      <div class="performance-mini"><span class="mini-label">Trades</span><strong>${stats.total}</strong></div>
      <div class="performance-mini"><span class="mini-label">PnL</span><strong>${formatNumber(stats.pnl_total, 4)}</strong></div>
      <div class="performance-mini"><span class="mini-label">Wins</span><strong>${stats.wins}</strong></div>
      <div class="performance-mini"><span class="mini-label">Losses</span><strong>${stats.losses}</strong></div>
    </div>
  `;
  return article;
}

function buildControlSummary(control) {
  const blockers = Array.isArray(control.activation_blockers) && control.activation_blockers.length
    ? control.activation_blockers.map((item) => `• ${item}`).join('\n')
    : '• Sin bloqueadores';
  return [
    `Wallet: ${control.wallet_configured ? truncateMiddle(control.wallet_masked, 24) : 'pendiente'}`,
    `Private key: ${control.private_key_configured ? 'configurada' : 'pendiente'}`,
    `Términos: ${control.terms_accepted ? 'aceptados' : 'pendientes'}`,
    `Plan: ${control.plan || 'none'} (${control.plan_active ? 'activo' : 'inactivo'})`,
    `Trading: ${control.trading_status || 'inactive'}`,
    '',
    'Bloqueadores:',
    blockers,
  ].join('\n');
}

function renderDashboard(data) {
  state.dashboard = data;
  const usernameText = data.username ? `@${data.username}` : `ID ${data.user_id || '—'}`;
  const readinessText = data.status_summary || 'not_ready';
  const tradingText = data.trading_status || 'inactive';
  const planText = data.plan || 'none';
  const accessText = data.plan_active ? 'Activo' : 'Bloqueado';
  const walletText = data.wallet_configured ? truncateMiddle(data.wallet || 'Configurada', 18) : 'Pendiente';

  setPill(elements.connectionBadge, 'Conectado', 'connected');
  setPill(elements.heroSessionPill, data.plan_active ? 'Sesión activa' : 'Acceso limitado', data.plan_active ? 'active' : 'blocked');
  setPill(elements.heroPlanPill, `Plan ${planText}`, data.plan_active ? planText : 'none');
  setPill(elements.heroTradingPill, `Trading ${tradingText}`, tradingText);
  setPill(elements.userPlanBadge, planText, data.plan_active ? planText : 'none');
  setPill(elements.userReadinessBadge, readinessText, readinessText);
  setPill(elements.walletBadge, data.wallet_configured ? 'Wallet ok' : 'Pendiente', data.wallet_configured ? 'configured' : 'missing');

  elements.heroUser.textContent = usernameText;
  elements.heroUserSubtext.textContent = data.plan_active ? 'Usuario autenticado y con sesión válida' : 'Sin acceso operativo activo';
  elements.heroWallet.textContent = walletText;
  elements.heroWalletSubtext.textContent = data.wallet_configured ? 'Configurada en backend' : 'Todavía falta configurarla';
  elements.heroKey.textContent = data.private_key_configured ? 'Configurada' : 'Pendiente';
  elements.heroKeySubtext.textContent = data.private_key_configured ? 'Llave privada presente en sistema' : 'Llave privada faltante';
  elements.heroAccess.textContent = accessText;
  elements.heroAccessSubtext.textContent = formatDate(data.plan_expires_at);

  elements.userIdentity.textContent = usernameText;
  elements.userIdentitySubtext.textContent = data.user_id ? `Telegram ID ${data.user_id}` : 'Usuario no resuelto';
  elements.userReadiness.textContent = readinessText;
  elements.userTradingStatus.textContent = `Trading ${tradingText}`;
  elements.walletConfigured.textContent = data.wallet_configured ? 'Wallet configurada' : 'Wallet pendiente';
  elements.privateKeyConfigured.textContent = data.private_key_configured ? 'Private key cargada' : 'Private key faltante';

  elements.dashboardStats.innerHTML = '';
  elements.dashboardStats.append(
    buildKpiCard('Plan', planText, data.plan_active ? 'Acceso operativo habilitado.' : 'Acceso inactivo.'),
    buildKpiCard('Trading', tradingText, 'Estado reportado por la base.'),
    buildKpiCard('Términos', data.terms_accepted ? 'Aceptados' : 'Pendientes', 'Bloquean la activación si faltan.'),
    buildKpiCard('Wallet', data.wallet_configured ? 'Sí' : 'No', 'Configuración sensible del usuario.'),
    buildKpiCard('Private key', data.private_key_configured ? 'Sí' : 'No', 'No se vuelve a exponer por API.'),
    buildKpiCard('Vencimiento', formatDate(data.plan_expires_at), 'Fecha actual del plan.')
  );

  elements.readinessList.innerHTML = '';
  elements.readinessList.append(
    buildReadinessItem('Wallet', data.wallet_configured ? 'La wallet ya está guardada.' : 'Todavía no existe wallet guardada.', data.wallet_configured ? 'configured' : 'missing'),
    buildReadinessItem('Private key', data.private_key_configured ? 'La private key ya existe.' : 'Todavía falta private key.', data.private_key_configured ? 'configured' : 'missing'),
    buildReadinessItem('Términos', data.terms_accepted ? 'Los términos ya fueron aceptados.' : 'Debes aceptar términos para activar trading.', data.terms_accepted ? 'active' : 'warning'),
    buildReadinessItem('Plan', data.plan_active ? `El plan ${planText} está activo.` : 'No hay acceso operativo vigente.', data.plan_active ? 'active' : 'inactive')
  );
}

function renderControl(control) {
  state.control = control;
  elements.controlReadinessBox.textContent = buildControlSummary(control);
  elements.termsStatusText.textContent = control.terms_accepted ? 'Aceptados' : 'Pendientes';
  elements.acceptTermsButton.disabled = control.terms_accepted;
  elements.activateTradingButton.disabled = !control.activation_ready;
  elements.pauseTradingButton.disabled = control.trading_status !== 'active';

  if (!elements.walletInput.value && control.wallet) {
    elements.walletInput.value = control.wallet;
  }

  elements.controlStats.innerHTML = '';
  elements.controlStats.append(
    buildKpiCard('Wallet', control.wallet_configured ? truncateMiddle(control.wallet_masked, 18) : 'Pendiente', 'Dirección actual del usuario.'),
    buildKpiCard('Private key', control.private_key_configured ? 'Configurada' : 'Pendiente', control.security_posture === 'encrypted_at_rest' ? 'Cifrada en reposo.' : 'Nunca se reexpone por API.'),
    buildKpiCard('Seguridad', control.security_posture === 'encrypted_at_rest' ? 'Cifrada' : (control.security_posture === 'legacy_plaintext' ? 'Legacy' : 'Sin key'), control.security_posture === 'legacy_plaintext' ? 'Requiere rotación para endurecer.' : 'Estado del almacenamiento sensible.'),
    buildKpiCard('Términos', control.terms_accepted ? 'Aceptados' : 'Pendientes', 'Bloquean activación.'),
    buildKpiCard('Trading', control.trading_status || 'inactive', 'Se puede pausar desde aquí.'),
    buildKpiCard('Plan', control.plan || 'none', control.plan_active ? 'Plan vigente.' : 'Sin acceso vigente.'),
    buildKpiCard('Activación', control.activation_ready ? 'Lista' : 'Bloqueada', (control.activation_blockers || []).join(', ') || 'Sin bloqueadores.')
  );
}

function renderPerformance(performance) {
  state.performance = performance;
  elements.performanceGrid.innerHTML = '';
  elements.performanceGrid.append(
    buildPerformanceCard('24h', performance['24h'] || {}),
    buildPerformanceCard('7d', performance['7d'] || {}),
    buildPerformanceCard('30d', performance['30d'] || {})
  );
}

function renderOperations(data) {
  state.operations = data;
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
        <div><span class="metric-label">Score</span><strong>${trade.best_score ?? '—'}</strong></div>
      </div>
    `;

    elements.operationsList.appendChild(item);
  });
}

function renderReferrals(data) {
  state.referrals = data;
  elements.referralStats.innerHTML = '';
  elements.referralStats.append(
    buildKpiCard('Usuario', data.user_id || '—', 'ID autenticado contra backend.'),
    buildKpiCard('Referidos válidos', data.referral_valid_count || 0, 'Contados en esta base.')
  );
}

function renderAdmin(data) {
  state.admin = data;
  const visual = data.visual || {};
  const tradeStats = data.trade_stats_30d || {};
  const security = data.security || {};

  elements.adminVisualStats.innerHTML = '';
  elements.adminVisualStats.append(
    buildKpiCard('Usuarios', visual.total_users || 0, 'Registrados en esta DB.'),
    buildKpiCard('Trial vencido', visual.free_old || 0, 'Sin premium activo.'),
    buildKpiCard('Premium activo', visual.premium_active || 0, 'Estado actual.'),
    buildKpiCard('Premium vencido', visual.premium_expired || 0, 'Caducados.')
  );

  elements.adminSecurityStats.innerHTML = '';
  elements.adminSecurityStats.append(
    buildKpiCard('Keys cifradas', security.encrypted_private_keys || 0, 'Protegidas en reposo.'),
    buildKpiCard('Legacy plaintext', security.legacy_plaintext_private_keys || 0, 'Deuda crítica a rotar.'),
    buildKpiCard('Wallets', security.wallets_configured || 0, 'Usuarios con wallet configurada.')
  );

  elements.adminTradeStats.innerHTML = '';
  elements.adminTradeStats.append(
    buildKpiCard('Trades 30d', tradeStats.total || 0, 'Global del clon.'),
    buildKpiCard('Wins', tradeStats.wins || 0, `Win rate ${tradeStats.win_rate ?? 0}%`),
    buildKpiCard('Losses', tradeStats.losses || 0, `Decisivas ${tradeStats.win_rate_decisive ?? 0}%`),
    buildKpiCard('Profit factor', tradeStats.profit_factor === Infinity ? '∞' : tradeStats.profit_factor ?? 0, `PnL ${tradeStats.pnl_total ?? 0}`),
    buildKpiCard('Gross profit', tradeStats.gross_profit ?? 0, 'Cierres positivos.'),
    buildKpiCard('Gross loss', tradeStats.gross_loss ?? 0, 'Pérdidas acumuladas.')
  );
}

function buildAdminSearchResultItem(user) {
  const item = document.createElement('article');
  item.className = 'list-item';
  item.innerHTML = `
    <div class="list-item-header">
      <div>
        <div class="list-item-title">${user.username ? `@${user.username}` : `ID ${user.user_id}`}</div>
        <div class="list-item-meta">Plan ${user.plan || 'none'} · Trading ${user.trading_status || 'inactive'} · Key ${user.private_key_storage || 'not_configured'}</div>
      </div>
      <button class="secondary-button" type="button" data-admin-user-id="${user.user_id}">Cargar</button>
    </div>
  `;
  const button = item.querySelector('button');
  button.addEventListener('click', async () => {
    await loadAdminUserDetail(user.user_id);
  });
  return item;
}

function renderAdminSelectedUser(user) {
  state.adminSelectedUser = user;
  elements.adminUserDetail.classList.remove('hidden');
  elements.adminUserTitle.textContent = user.username ? `@${user.username}` : `Usuario ${user.user_id}`;
  elements.adminUserSubtitle.textContent = `ID ${user.user_id} · Plan ${user.plan || 'none'} · Trading ${user.trading_status || 'inactive'}`;
  elements.adminUserStats.innerHTML = '';
  elements.adminUserStats.append(
    buildKpiCard('Wallet', user.wallet_configured ? truncateMiddle(user.wallet || '—', 18) : 'Pendiente', 'Estado de wallet.'),
    buildKpiCard('Private key', user.private_key_configured ? 'Configurada' : 'Pendiente', user.private_key_storage || 'not_configured'),
    buildKpiCard('Plan', user.plan || 'none', user.plan_active ? `Vence ${formatDate(user.plan_expires_at)}` : 'Sin acceso vigente.'),
    buildKpiCard('Términos', user.terms_accepted ? 'Aceptados' : 'Pendientes', user.terms_timestamp ? `TS ${formatDate(user.terms_timestamp)}` : 'Sin aceptación registrada.'),
    buildKpiCard('Trading', user.trading_status || 'inactive', user.last_open_at ? `Última apertura ${formatDate(user.last_open_at)}` : 'Sin aperturas registradas.'),
    buildKpiCard('Referidos válidos', user.referral_valid_count || 0, user.private_key_version ? `Cipher ${user.private_key_version}` : 'Sin versión de cifrado.')
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
  if (response.status === 204) return null;
  return response.json();
}

function setPreviewMode() {
  setPill(elements.connectionBadge, 'Preview', 'preview');
  setStatus('Abriste la URL directa. La vista carga, pero la sesión real solo llega al abrir la MiniApp desde Telegram.', 'warning');
  setPill(elements.heroSessionPill, 'Preview', 'preview');
  setPill(elements.heroPlanPill, 'Plan preview', 'preview');
  setPill(elements.heroTradingPill, 'Trading preview', 'preview');
  elements.heroUser.textContent = 'Modo visual';
  elements.heroUserSubtext.textContent = 'Abre la MiniApp desde Telegram para autenticar';
  elements.heroWallet.textContent = '—';
  elements.heroWalletSubtext.textContent = 'Sin sesión';
  elements.heroKey.textContent = '—';
  elements.heroKeySubtext.textContent = 'Sin sesión';
  elements.heroAccess.textContent = 'Bloqueado';
  elements.heroAccessSubtext.textContent = 'Necesita initData';
  setPill(elements.userPlanBadge, 'Preview', 'preview');
  setPill(elements.userReadinessBadge, 'Sin auth', 'blocked');
  setPill(elements.walletBadge, 'Sin auth', 'blocked');
  elements.userIdentity.textContent = 'Modo visual';
  elements.userIdentitySubtext.textContent = 'Usa el botón desde Telegram';
  elements.userReadiness.textContent = 'Pendiente';
  elements.userTradingStatus.textContent = 'Sin sesión';
  elements.walletConfigured.textContent = 'No disponible';
  elements.privateKeyConfigured.textContent = 'No disponible';
  elements.refreshButton.disabled = true;
}

async function authenticate() {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  if (!tg) {
    setPreviewMode();
    throw new Error('Telegram WebApp no está disponible.');
  }

  state.telegram = tg;
  tg.ready();
  tg.expand();
  tg.setHeaderColor('#09111f');
  tg.setBackgroundColor('#050913');

  if (!tg.initData) {
    setPreviewMode();
    throw new Error('Abre esta MiniApp desde Telegram para autenticar la sesión.');
  }

  const payload = await apiFetch('/api/v1/auth/telegram', {
    method: 'POST',
    body: JSON.stringify({ init_data: tg.initData }),
  });

  state.token = payload.access_token;
}

async function loadData() {
  setStatus('Sincronizando datos con el backend...', 'info');
  const [dashboard, control, performance, operations, referrals] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/control'),
    apiFetch('/api/v1/performance'),
    apiFetch('/api/v1/operations?limit=20'),
    apiFetch('/api/v1/referrals'),
  ]);

  renderDashboard(dashboard);
  renderControl(control);
  renderPerformance(performance);
  renderOperations(operations);
  renderReferrals(referrals);

  try {
    const admin = await apiFetch('/api/v1/admin/overview');
    renderAdmin(admin);
    elements.adminTabButton.classList.remove('hidden');
    state.isAdmin = true;
  } catch {
    elements.adminTabButton.classList.add('hidden');
    state.isAdmin = false;
    document.querySelectorAll('[data-panel="admin"]').forEach((panel) => panel.classList.remove('is-active'));
  }

  setStatus('MiniApp sincronizada correctamente.', 'success');
}

function bindTabs() {
  const buttons = Array.from(document.querySelectorAll('.tab'));
  const panels = Array.from(document.querySelectorAll('.panel'));
  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      const target = button.dataset.tab;
      if (target === 'admin' && !state.isAdmin) return;
      buttons.forEach((item) => item.classList.toggle('is-active', item === button));
      panels.forEach((panel) => panel.classList.toggle('is-active', panel.dataset.panel === target));
    });
  });
}

async function saveConfiguration() {
  const wallet = elements.walletInput.value.trim();
  const privateKey = elements.privateKeyInput.value.trim();
  const payload = {};
  if (wallet) payload.wallet = wallet;
  if (privateKey) payload.private_key = privateKey;

  if (!payload.wallet && !payload.private_key) {
    setStatus('No hay cambios para guardar.', 'warning');
    return;
  }

  elements.saveConfigurationButton.disabled = true;
  try {
    const control = await apiFetch('/api/v1/control/configuration', {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    renderControl(control);
    elements.privateKeyInput.value = '';
    setStatus('Configuración crítica actualizada correctamente.', 'success');
    await refreshSummaryOnly();
  } catch (error) {
    setStatus(error.message || 'No se pudo guardar la configuración.', 'error');
  } finally {
    elements.saveConfigurationButton.disabled = false;
  }
}

async function acceptTermsAction() {
  elements.acceptTermsButton.disabled = true;
  try {
    const control = await apiFetch('/api/v1/control/terms/accept', { method: 'POST' });
    renderControl(control);
    setStatus('Términos aceptados correctamente.', 'success');
    await refreshSummaryOnly();
  } catch (error) {
    setStatus(error.message || 'No se pudieron aceptar los términos.', 'error');
    elements.acceptTermsButton.disabled = false;
  }
}

async function activateTradingAction() {
  elements.activateTradingButton.disabled = true;
  try {
    const payload = await apiFetch('/api/v1/control/trading/activate', { method: 'POST' });
    renderControl(payload.control);
    setStatus(payload.message || 'Trading activado.', 'success');
    await refreshSummaryOnly();
  } catch (error) {
    setStatus(error.message || 'No se pudo activar el trading.', 'error');
  } finally {
    elements.activateTradingButton.disabled = false;
  }
}

async function pauseTradingAction() {
  elements.pauseTradingButton.disabled = true;
  try {
    const payload = await apiFetch('/api/v1/control/trading/pause', { method: 'POST' });
    renderControl(payload.control);
    setStatus(payload.message || 'Trading pausado.', 'warning');
    await refreshSummaryOnly();
  } catch (error) {
    setStatus(error.message || 'No se pudo pausar el trading.', 'error');
  } finally {
    elements.pauseTradingButton.disabled = false;
  }
}

async function refreshSummaryOnly() {
  const [dashboard, control, performance] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/control'),
    apiFetch('/api/v1/performance'),
  ]);
  renderDashboard(dashboard);
  renderControl(control);
  renderPerformance(performance);
}
async function loadAdminUserDetail(userId) {
  const detail = await apiFetch(`/api/v1/admin/users/${userId}`);
  renderAdminSelectedUser(detail);
}

async function searchAdminUsers() {
  const query = elements.adminSearchInput.value.trim();
  if (!query) {
    setStatus('Escribe un Telegram ID o username para buscar.', 'warning');
    return;
  }
  elements.adminSearchButton.disabled = true;
  try {
    const payload = await apiFetch(`/api/v1/admin/users/search?q=${encodeURIComponent(query)}&limit=10`);
    const items = Array.isArray(payload.items) ? payload.items : [];
    if (!items.length) {
      elements.adminSearchResults.className = 'list-stack empty-state';
      elements.adminSearchResults.textContent = 'No se encontraron usuarios con ese criterio.';
      elements.adminUserDetail.classList.add('hidden');
      state.adminSelectedUser = null;
      setStatus('Búsqueda admin sin resultados.', 'warning');
      return;
    }

    elements.adminSearchResults.className = 'list-stack';
    elements.adminSearchResults.innerHTML = '';
    items.forEach((user) => elements.adminSearchResults.appendChild(buildAdminSearchResultItem(user)));
    setStatus(`Se encontraron ${items.length} usuarios.`, 'success');
  } catch (error) {
    setStatus(error.message || 'No se pudo buscar el usuario.', 'error');
  } finally {
    elements.adminSearchButton.disabled = false;
  }
}

async function activatePremiumForSelectedUser() {
  const selected = state.adminSelectedUser;
  if (!selected || !selected.user_id) {
    setStatus('Primero carga un usuario admin.', 'warning');
    return;
  }
  elements.adminActivatePremiumButton.disabled = true;
  try {
    const payload = await apiFetch(`/api/v1/admin/users/${selected.user_id}/plan/premium`, { method: 'POST' });
    renderAdminSelectedUser(payload.user);
    await loadData();
    setStatus(payload.message || 'Premium activado.', 'success');
  } catch (error) {
    setStatus(error.message || 'No se pudo activar premium.', 'error');
  } finally {
    elements.adminActivatePremiumButton.disabled = false;
  }
}


function bindActions() {
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

  elements.configurationForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    await saveConfiguration();
  });

  elements.clearConfigurationButton.addEventListener('click', () => {
    elements.walletInput.value = '';
    elements.privateKeyInput.value = '';
  });

  elements.acceptTermsButton.addEventListener('click', acceptTermsAction);
  elements.activateTradingButton.addEventListener('click', activateTradingAction);
  elements.pauseTradingButton.addEventListener('click', pauseTradingAction);

  if (elements.adminSearchForm) {
    elements.adminSearchForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      await searchAdminUsers();
    });
  }

  if (elements.adminActivatePremiumButton) {
    elements.adminActivatePremiumButton.addEventListener('click', activatePremiumForSelectedUser);
  }
}

async function bootstrap() {
  bindTabs();
  bindActions();

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
