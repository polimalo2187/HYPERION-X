const state = {
  token: null,
  telegram: null,
  isAdmin: false,
  dashboard: null,
  control: null,
  performance: null,
  operations: null,
  referrals: null,
  systemRuntime: null,
  admin: null,
  adminSelectedUser: null,
};

const $ = (id) => document.getElementById(id);

const elements = {
  statusBanner: $('statusBanner'),
  refreshButton: $('refreshButton'),
  connectionBadge: $('connectionBadge'),
  adminTabButton: $('adminTabButton'),
  systemTabButton: $('systemTabButton'),
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
  performanceExecutiveGrid: $('performanceExecutiveGrid'),
  referralStats: $('referralStats'),
  accessStatsGrid: $('accessStatsGrid'),
  userActivityList: $('userActivityList'),
  summarySystemStatusPanel: $('summarySystemStatusPanel'),
  summarySystemOverviewPanel: $('summarySystemOverviewPanel'),
  systemHealthGrid: $('systemHealthGrid'),
  systemRuntimeGrid: $('systemRuntimeGrid'),
  systemRuntimeNotes: $('systemRuntimeNotes'),
  systemHealthGridPanel: $('systemHealthGridPanel'),
  backendHealthCard: $('backendHealthCard'),
  systemRuntimeGridPanel: $('systemRuntimeGridPanel'),
  systemActivityList: $('systemActivityList'),
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
  activeTradeBadge: $('activeTradeBadge'),
  activeTradeSummary: $('activeTradeSummary'),
  latestOpenSummary: $('latestOpenSummary'),
  latestCloseSummary: $('latestCloseSummary'),
  operationsSummaryGrid: $('operationsSummaryGrid'),
  timelineCount: $('timelineCount'),
  operationsTimelineGrid: $('operationsTimelineGrid'),
  operationsTimelineList: $('operationsTimelineList'),
  operationsCount: $('operationsCount'),
  operationsList: $('operationsList'),
  adminVisualStats: $('adminVisualStats'),
  adminSecurityStats: $('adminSecurityStats'),
  adminTradeStats: $('adminTradeStats'),
  adminRecentActions: $('adminRecentActions'),
  adminSearchForm: $('adminSearchForm'),
  adminSearchInput: $('adminSearchInput'),
  adminSearchButton: $('adminSearchButton'),
  adminSearchResults: $('adminSearchResults'),
  adminUserDetail: $('adminUserDetail'),
  adminUserTitle: $('adminUserTitle'),
  adminUserSubtitle: $('adminUserSubtitle'),
  adminPlanSelect: $('adminPlanSelect'),
  adminPlanDaysInput: $('adminPlanDaysInput'),
  adminGrantPlanButton: $('adminGrantPlanButton'),
  adminPlanGrantHint: $('adminPlanGrantHint'),
  adminPlanPreviewBox: $('adminPlanPreviewBox'),
  adminActionReasonInput: $('adminActionReasonInput'),
  adminActivateTradingButton: $('adminActivateTradingButton'),
  adminPauseTradingButton: $('adminPauseTradingButton'),
  adminMigrateKeyButton: $('adminMigrateKeyButton'),
  adminResetStatsButton: $('adminResetStatsButton'),
  adminBulkMigrateButton: $('adminBulkMigrateButton'),
  adminBulkMigrationStatus: $('adminBulkMigrationStatus'),
  adminUserStats: $('adminUserStats'),
  adminUserPerformance: $('adminUserPerformance'),
  adminUserActionHistory: $('adminUserActionHistory'),
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

function setSummarySystemVisibility() {
  if (elements.summarySystemStatusPanel) elements.summarySystemStatusPanel.classList.add('hidden');
  if (elements.summarySystemOverviewPanel) elements.summarySystemOverviewPanel.classList.add('hidden');
}

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function planLabel(plan) {
  const normalized = String(plan || '').toLowerCase();
  if (normalized === 'trial') return 'PRUEBA';
  if (normalized === 'premium') return 'PREMIUM';
  if (!normalized || normalized === 'none') return 'SIN PLAN';
  return normalized.toUpperCase();
}

function accessLabel(data) {
  return data.access_label || (data.plan_active ? 'Activo' : 'Sin acceso');
}

function accessDetail(data) {
  if (data.access_detail) return data.access_detail;
  if (data.plan_active && data.plan_days_remaining > 0) return `${data.plan_days_remaining} día(s) restantes`;
  return data.plan_active ? 'Acceso vigente' : 'Sin acceso vigente';
}

function renderEventSummary(container, title, summary, emptyCopy) {
  if (!container) return;
  if (!summary || (!summary.title && !summary.detail)) {
    container.className = 'list-stack empty-state';
    container.textContent = emptyCopy;
    return;
  }
  container.className = 'event-summary';
  container.innerHTML = '';
  const item = document.createElement('article');
  item.className = 'list-item';
  item.innerHTML = `
    <div class="list-item-title">${summary.title || title}</div>
    <div class="list-item-meta">${summary.detail || emptyCopy}</div>
  `;
  container.appendChild(item);
}


function renderLiveTradeSummary(container, summary, emptyCopy) {
  if (!container) return;
  if (!summary) {
    container.className = 'list-stack empty-state';
    container.textContent = emptyCopy;
    return;
  }
  container.className = 'event-summary';
  container.innerHTML = '';
  const item = document.createElement('article');
  item.className = 'list-item';
  item.innerHTML = `
    <div class="list-item-title">${summary.title || 'Operación activa'}</div>
    <div class="list-item-meta">${summary.detail || emptyCopy}</div>
    <div class="list-item-meta subtle">${summary.started_at ? `Abierta ${formatDate(summary.started_at)}` : 'Sin timestamp de apertura.'}</div>
  `;
  container.appendChild(item);
}

function formatBlockers(list) {
  const rows = Array.isArray(list) ? list.filter(Boolean) : [];
  return rows.length ? rows.map((item) => `• ${item}`).join('\n') : '• Sin bloqueadores';
}

function formatActivityDetail(item) {
  return item?.detail || 'Sin detalle adicional.';
}

function formatFreshness(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value < 0) return '—';
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m`;
  return `${Math.floor(value / 3600)}h`;
}

function runtimeStatusLabel(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'healthy') return 'Sano';
  if (normalized === 'warning' || normalized === 'stale') return 'Atención';
  if (normalized === 'degraded') return 'Degradado';
  if (normalized === 'online') return 'Online';
  if (normalized === 'offline') return 'Offline';
  if (normalized === 'error') return 'Error';
  return normalized.toUpperCase() || '—';
}

function buildSystemComponentCard(label, component = {}) {
  const status = component.status || 'offline';
  const freshness = component.freshness_seconds;
  const article = document.createElement('article');
  article.className = `kpi-card runtime-card ${pillClass(status)}`;
  article.innerHTML = `
    <div class="runtime-card-head">
      <span class="kpi-label">${label}</span>
      <span class="status-pill ${pillClass(status)}">${runtimeStatusLabel(status)}</span>
    </div>
    <div class="kpi-value">${component.last_seen_at ? formatDate(component.last_seen_at) : 'Sin registro reciente'}</div>
    <div class="kpi-subtext">${component.message || 'Sin actualización reciente'} · Actualización ${formatFreshness(freshness)}</div>
  `;
  return article;
}

function systemStatusCopy(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'healthy') return 'La plataforma opera con normalidad.';
  if (normalized === 'warning' || normalized === 'stale') return 'La plataforma sigue operativa, pero hay una atención temporal en curso.';
  if (normalized === 'degraded') return 'Hay una incidencia temporal en algunos servicios. Algunas funciones pueden tardar más de lo normal.';
  return 'La plataforma está sincronizando su estado operativo.';
}

function publicIssueSummary(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'healthy') return { label: 'Operativo', description: 'Todos los servicios principales responden correctamente.', tone: 'active' };
  if (normalized === 'warning' || normalized === 'stale') return { label: 'Atención', description: 'Se detectó una condición temporal que no bloquea el uso general.', tone: 'warning' };
  if (normalized === 'degraded') return { label: 'Incidencia', description: 'Hay una degradación temporal en la plataforma.', tone: 'blocked' };
  return { label: 'Sincronizando', description: 'La plataforma está resolviendo su estado actual.', tone: 'warning' };
}

function renderPublicSystemOverview(payload) {
  const publicSummary = payload.public_summary || {};
  const checkedAt = payload.checked_at || publicSummary.last_update_at;
  const summary = publicIssueSummary(payload.overall_status);
  const activityLabel = publicSummary.recent_activity_label || 'Sin actividad reciente';
  const activityDetail = publicSummary.recent_activity_detail || 'Todavía no hay actividad reciente para mostrar.';
  const syncLabel = publicSummary.connection_label || 'Sincronizada';
  const syncDetail = checkedAt ? `Última actualización ${formatDate(checkedAt)}` : 'Sin actualización reciente.';

  const healthCards = [
    buildKpiCard('Estado del sistema', runtimeStatusLabel(payload.overall_status), systemStatusCopy(payload.overall_status)),
    buildKpiCard('Conectividad', syncLabel, syncDetail),
    buildKpiCard('Actividad reciente', activityLabel, activityDetail),
  ];

  const overviewCards = [
    buildKpiCard('Operaciones activas', publicSummary.active_trades ?? 0, 'Posiciones activas registradas en el sistema.'),
    buildKpiCard('Estado de ejecución', publicSummary.execution_label || 'Monitoreado', publicSummary.execution_detail || 'Visión resumida del estado operativo.'),
    buildKpiCard('Última lectura', checkedAt ? formatDate(checkedAt) : '—', 'Hora de la última sincronización visual.'),
  ];

  if (elements.systemHealthGrid) {
    elements.systemHealthGrid.innerHTML = '';
    healthCards.forEach((card) => elements.systemHealthGrid.appendChild(card));
  }

  if (elements.systemRuntimeGrid) {
    elements.systemRuntimeGrid.innerHTML = '';
    overviewCards.forEach((card) => elements.systemRuntimeGrid.appendChild(card));
  }

  if (elements.accessStatsGrid) elements.accessStatsGrid.innerHTML = '';
  if (elements.performanceExecutiveGrid) elements.performanceExecutiveGrid.innerHTML = '';
  if (elements.systemRuntimeNotes) {
    elements.systemRuntimeNotes.innerHTML = '';
    elements.systemRuntimeNotes.className = 'readiness-list compact-list';
    elements.systemRuntimeNotes.appendChild(buildReadinessItem(summary.label, summary.description, summary.tone));
    if (publicSummary.plan_notice) {
      elements.systemRuntimeNotes.appendChild(buildReadinessItem('Acceso', publicSummary.plan_notice, 'active'));
    }
  }
}

function renderTechnicalSystemPanel(payload) {
  const components = payload.components || {};
  const runtime = payload.runtime || {};
  const cards = [
    buildSystemComponentCard('Backend web', { status: payload.backend?.status || 'online', last_seen_at: payload.backend?.checked_at, message: payload.backend?.message, freshness_seconds: 0 }),
    buildSystemComponentCard('Canal Telegram', components.telegram_bot || {}),
    buildSystemComponentCard('Motor de trading', components.trading_loop || {}),
    buildSystemComponentCard('Scanner de mercado', components.scanner || {}),
  ];

  if (elements.systemHealthGridPanel) {
    elements.systemHealthGridPanel.innerHTML = '';
    cards.forEach((card) => elements.systemHealthGridPanel.appendChild(card.cloneNode(true)));
  }

  const runtimeCards = [
    buildKpiCard('Estado global', runtimeStatusLabel(payload.overall_status), payload.issues?.length ? `${payload.issues.length} incidencia(s) detectadas.` : 'Sin alertas críticas ahora mismo.'),
    buildKpiCard('Usuarios con plan', runtime.users_with_active_plan || 0, 'Acceso vigente en esta base.'),
    buildKpiCard('Trading activo', runtime.users_trading_active || 0, 'Usuarios con trading activo.'),
    buildKpiCard('Operaciones activas', runtime.active_trades || 0, 'Operaciones abiertas registradas.'),
    buildKpiCard('Última actividad del scanner', runtime.scanner_last_event || '—', runtime.scanner_last_symbol ? `Símbolo ${runtime.scanner_last_symbol}` : 'Sin símbolo reciente.'),
    buildKpiCard('Última actividad del manager', runtime.latest_trade_manager?.manager_heartbeat_at ? formatDate(runtime.latest_trade_manager.manager_heartbeat_at) : 'Sin registro', runtime.latest_trade_manager?.symbol ? `${runtime.latest_trade_manager.symbol} · user ${runtime.latest_trade_manager.user_id}` : 'Sin trade activo reciente.'),
  ];

  if (elements.systemRuntimeGridPanel) {
    elements.systemRuntimeGridPanel.innerHTML = '';
    runtimeCards.forEach((card) => elements.systemRuntimeGridPanel.appendChild(card));
  }

  if (elements.backendHealthCard) {
    elements.backendHealthCard.innerHTML = '';
    elements.backendHealthCard.append(
      buildKpiCard('Backend', 'ONLINE', payload.backend?.message || 'El backend respondió.'),
      buildKpiCard('Chequeado', payload.backend?.checked_at ? formatDate(payload.backend.checked_at) : '—', 'Timestamp de esta respuesta.')
    );
  }

  if (elements.systemActivityList) {
    elements.systemActivityList.className = 'list-stack';
    elements.systemActivityList.innerHTML = '';
    elements.systemActivityList.append(
      buildActivityItem('Última apertura', runtime.latest_open, 'No hay aperturas registradas todavía.'),
      buildActivityItem('Último cierre', runtime.latest_close, 'No hay cierres registrados todavía.'),
    );
  }
}

function buildActivityItem(title, item, emptyCopy) {
  const article = document.createElement('article');
  article.className = 'list-item';
  if (!item) {
    article.innerHTML = `<div class="list-item-title">${title}</div><div class="list-item-meta">${emptyCopy}</div>`;
    return article;
  }
  article.innerHTML = `
    <div class="list-item-header">
      <div>
        <div class="list-item-title">${title}</div>
        <div class="list-item-meta">${item.username ? '@' + item.username : 'ID ' + (item.user_id ?? '—')} · ${formatDate(item.at)}</div>
      </div>
      <span class="status-pill neutral">${item.symbol || '—'}</span>
    </div>
    <div class="list-item-meta">${item.event || 'Sin payload resumido.'}</div>
  `;
  return article;
}


function adminActionLabel(action) {
  const normalized = String(action || '').toLowerCase();
  const labels = {
    grant_manual_plan_days: 'Extensión manual de plan',
    activate_user_trading: 'Reanudar trading',
    pause_user_trading: 'Pausar trading',
    migrate_user_private_key: 'Migrar private key',
    reset_user_stats: 'Resetear rendimiento',
    bulk_migrate_legacy_keys: 'Migración masiva de keys legacy',
    activate_premium_fixed_30d: 'Activación fija Premium 30d',
  };
  return labels[normalized] || normalized.replace(/_/g, ' ').toUpperCase();
}

function getAdminActionReason() {
  return elements.adminActionReasonInput ? elements.adminActionReasonInput.value.trim() : '';
}

function renderAdminActionHistory(container, items, emptyText) {
  if (!container) return;
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    container.className = 'list-stack empty-state';
    container.textContent = emptyText;
    return;
  }

  container.className = 'admin-history-list';
  container.innerHTML = '';

  rows.forEach((row) => {
    const article = document.createElement('article');
    article.className = 'admin-history-item';
    const actor = row.actor_username ? `@${row.actor_username}` : (row.actor_user_id ? `ID ${row.actor_user_id}` : 'Admin');
    const target = row.target_username ? `@${row.target_username}` : (row.target_user_id ? `ID ${row.target_user_id}` : 'Global');
    const statusClass = row.status === 'rejected' ? 'danger' : 'info';
    const metadata = row.metadata || {};
    const extra = [];
    if (metadata.plan) extra.push(`Plan ${planLabel(metadata.plan)}`);
    if (metadata.days) extra.push(`${metadata.days} día(s)`);
    if (metadata.migrated_count) extra.push(`${metadata.migrated_count} migradas`);
    article.innerHTML = `
      <div class="admin-history-head">
        <div>
          <div class="admin-history-title">${adminActionLabel(row.action)}</div>
          <div class="admin-history-meta">${formatDate(row.created_at)} · ${actor} → ${target}${extra.length ? ` · ${extra.join(' · ')}` : ''}</div>
        </div>
        <span class="status-pill ${statusClass}">${row.status === 'rejected' ? 'RECHAZADA' : 'REGISTRADA'}</span>
      </div>
      <div class="admin-history-body">
        <p class="admin-history-message"><strong>Resultado:</strong> ${row.message || 'Sin mensaje adicional.'}</p>
        ${row.reason ? `<p class="admin-history-reason"><strong>Motivo:</strong> ${row.reason}</p>` : ''}
      </div>
    `;
    container.appendChild(article);
  });
}

async function requestStrongConfirmation(title, lines = []) {
  const promptText = `${title}

${lines.filter(Boolean).join('\n')}

Escribe CONFIRMAR para continuar.`;
  const value = window.prompt(promptText, '');
  if (value === null) {
    setStatus('Acción cancelada.', 'warning');
    return false;
  }
  if (value.trim().toUpperCase() !== 'CONFIRMAR') {
    setStatus('Confirmación inválida. Escribe exactamente CONFIRMAR.', 'warning');
    return false;
  }
  return true;
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

function buildAdminPerformanceCard(windowLabel, stats = {}) {
  return buildPerformanceCard(windowLabel, {
    total: stats.total || 0,
    pnl_total: stats.pnl_total || 0,
    wins: stats.wins || 0,
    losses: stats.losses || 0,
    profit_factor: stats.profit_factor === Infinity ? Infinity : Number(stats.profit_factor || 0),
    win_rate: Number(stats.win_rate || 0),
  });
}

function buildControlSummary(control) {
  const blockers = formatBlockers(control.activation_blockers_copy || control.activation_blockers);
  return [
    `Wallet: ${control.wallet_configured ? truncateMiddle(control.wallet_masked, 24) : 'pendiente'}`,
    `Private key: ${control.private_key_configured ? 'configurada' : 'pendiente'}`,
    `Términos: ${control.terms_accepted ? 'aceptados' : 'pendientes'}`,
    `Plan: ${planLabel(control.plan)} (${control.plan_active ? 'activo' : 'inactivo'})`,
    `Días restantes: ${control.plan_days_remaining ?? 0}`,
    `Trading: ${control.trading_status || 'inactive'}`,
    `Preparación: ${control.readiness_completed ?? 0}/${control.readiness_total ?? 5}`,
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
  const planText = planLabel(data.plan);
  const accessText = accessLabel(data);
  const walletText = data.wallet_configured ? truncateMiddle(data.wallet || 'Configurada', 18) : 'Pendiente';

  setPill(elements.connectionBadge, 'Conectado', 'connected');
  setPill(elements.heroSessionPill, data.plan_active ? 'Sesión activa' : 'Acceso limitado', data.plan_active ? 'active' : 'blocked');
  setPill(elements.heroPlanPill, `Plan ${planText}`, data.plan_active ? data.plan : 'none');
  setPill(elements.heroTradingPill, `Trading ${tradingText}`, tradingText);
  setPill(elements.userPlanBadge, planText, data.plan_active ? data.plan : 'none');
  setPill(elements.userReadinessBadge, readinessText, readinessText);
  setPill(elements.walletBadge, data.wallet_configured ? 'Wallet ok' : 'Pendiente', data.wallet_configured ? 'configured' : 'missing');

  elements.heroUser.textContent = usernameText;
  elements.heroUserSubtext.textContent = data.plan_active ? `Usuario autenticado · preparación ${data.readiness_completed || 0}/${data.readiness_total || 5}` : 'Sin acceso operativo activo';
  elements.heroWallet.textContent = walletText;
  elements.heroWalletSubtext.textContent = data.wallet_configured ? 'Configurada en backend' : 'Todavía falta configurarla';
  elements.heroKey.textContent = data.private_key_configured ? 'Configurada' : 'Pendiente';
  elements.heroKeySubtext.textContent = data.private_key_configured ? 'Llave privada presente en sistema' : 'Llave privada faltante';
  elements.heroAccess.textContent = accessText;
  elements.heroAccessSubtext.textContent = `${accessDetail(data)}${data.plan_expires_at ? ' · vence ' + formatDate(data.plan_expires_at) : ''}`;

  elements.userIdentity.textContent = usernameText;
  elements.userIdentitySubtext.textContent = data.user_id ? `Telegram ID ${data.user_id}` : 'Usuario no resuelto';
  elements.userReadiness.textContent = readinessText;
  elements.userTradingStatus.textContent = `Trading ${tradingText}`;
  elements.walletConfigured.textContent = data.wallet_configured ? 'Wallet configurada' : 'Wallet pendiente';
  elements.privateKeyConfigured.textContent = data.private_key_configured ? 'Private key cargada' : 'Private key faltante';

  elements.dashboardStats.innerHTML = '';
  elements.dashboardStats.append(
    buildKpiCard('Plan', planText, data.plan_active ? 'Acceso operativo habilitado.' : 'Acceso inactivo.'),
    buildKpiCard('Días restantes', data.plan_days_remaining ?? 0, data.plan_active ? 'Se calculan sobre el vencimiento actual.' : 'Sin días vigentes.'),
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

  if (elements.accessStatsGrid) {
    elements.accessStatsGrid.innerHTML = '';
    elements.accessStatsGrid.append(
      buildKpiCard('Estado de acceso', accessText, accessDetail(data)),
      buildKpiCard('Días restantes', data.plan_days_remaining ?? 0, data.plan_active ? 'Continuidad actual del plan.' : 'No hay días vigentes.'),
      buildKpiCard('Vencimiento', formatDate(data.plan_expires_at), data.plan_active ? 'Fecha actual del acceso.' : 'No existe acceso operativo vigente.'),
    );
  }
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
    buildKpiCard('Plan', planLabel(control.plan), control.plan_active ? `Plan vigente · ${control.plan_days_remaining || 0} día(s) restantes.` : 'Sin acceso vigente.'),
    buildKpiCard('Preparación', `${control.readiness_completed || 0}/${control.readiness_total || 5}`, 'Lectura global de requisitos cumplidos.'),
    buildKpiCard('Activación', control.activation_ready ? 'Lista' : 'Bloqueada', (control.activation_blockers_copy || []).join(' ') || 'Sin bloqueadores.')
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

  if (elements.performanceExecutiveGrid) {
    const executive = performance.executive || {};
    const track = performance.track_record || {};
    elements.performanceExecutiveGrid.innerHTML = '';
    elements.performanceExecutiveGrid.append(
      buildKpiCard('Mejor ventana', executive.best_window || 'Sin muestra', executive.best_window ? `PnL ${formatNumber(executive.best_window_pnl || 0, 4)}` : 'Todavía no hay una ventana con trades suficientes.'),
      buildKpiCard('Lectura actual', executive.edge_label || 'Sin lectura', executive.edge_detail || 'Sin lectura ejecutiva disponible.'),
      buildKpiCard('Trades 30d', executive.trades_30d || 0, `Cadencia ${formatNumber(executive.cadence_30d || 0, 2)} trades/día · Decisivos ${executive.decisive_30d || 0}`),
      buildKpiCard('Expectativa', formatNumber(executive.expectancy || track.expectancy || 0, 4), `Avg win ${formatNumber(executive.avg_win || track.avg_win || 0, 4)} · Avg loss ${formatNumber(executive.avg_loss || track.avg_loss || 0, 4)}`),
      buildKpiCard('Racha actual', executive.streak_label || 'Sin racha', `Mejor win x${executive.streak_best_win || 0} · Mejor loss x${executive.streak_best_loss || 0}`),
      buildKpiCard('Forma reciente', executive.recent_form_compact || track.recent_form_compact || '—', executive.dominant_symbol ? `${executive.dominant_symbol} · ${executive.dominant_symbol_count || 0} trade(s)` : 'Sin símbolo dominante todavía.'),
      buildKpiCard('Track record', track.total || 0, `Win ${formatNumber(track.win_rate || 0, 2)}% · PF ${track.profit_factor === Infinity ? '∞' : formatNumber(track.profit_factor || 0, 2)}`),
      buildKpiCard('Extremos', `${formatNumber(track.best_trade || 0, 4)} / ${formatNumber(track.worst_trade || 0, 4)}`, track.last_trade_at ? `Último cierre ${formatDate(track.last_trade_at)}` : 'Sin cierres históricos todavía.')
    );
  }
}

function renderOperations(data) {
  state.operations = data;
  const summary = data.summary || {};
  const timeline = Array.isArray(data.activity) ? data.activity : [];
  const timelineSummary = data.timeline_summary || {};

  setPill(elements.activeTradeBadge, data.active_trade_summary ? 'LIVE' : 'IDLE', data.active_trade_summary ? 'active' : 'neutral');
  renderLiveTradeSummary(elements.activeTradeSummary, data.active_trade_summary, 'No hay una operación activa registrada ahora mismo.');

  if (elements.operationsSummaryGrid) {
    elements.operationsSummaryGrid.innerHTML = '';
    elements.operationsSummaryGrid.append(
      buildKpiCard('Wins visibles', summary.wins || 0, 'Trades ganadores en la lista visible.'),
      buildKpiCard('Losses visibles', summary.losses || 0, 'Trades perdedores en la lista visible.'),
      buildKpiCard('Neto visible', formatNumber(summary.net_visible || 0, 4), 'Suma del PnL en la muestra actual.'),
      buildKpiCard('Expectativa visible', formatNumber(summary.avg_trade_visible || 0, 4), 'Promedio de PnL por trade en la muestra visible.'),
      buildKpiCard('Racha visible', summary.current_streak_count ? `${String(summary.current_streak_type || '').toUpperCase()} x${summary.current_streak_count}` : 'Sin racha', 'Cuenta consecutiva desde el trade más reciente.'),
      buildKpiCard('Forma reciente', summary.recent_form_visible || '—', summary.dominant_symbol ? `${summary.dominant_symbol} · ${summary.dominant_symbol_count || 0} trade(s)` : 'Sin símbolo dominante visible.'),
      buildKpiCard('Símbolo dominante', summary.dominant_symbol || '—', summary.dominant_symbol ? `PnL ${formatNumber(summary.dominant_symbol_pnl || 0, 4)}` : 'La muestra visible todavía es muy corta.'),
      buildKpiCard('Mejor / peor', `${formatNumber(summary.best_trade_pnl || 0, 4)} / ${formatNumber(summary.worst_trade_pnl || 0, 4)}`, 'Extremos dentro de la lista visible.'),
    );
  }

  if (elements.operationsTimelineGrid) {
    elements.operationsTimelineGrid.innerHTML = '';
    elements.operationsTimelineGrid.append(
      buildKpiCard('Eventos visibles', timelineSummary.total_visible_events || 0, 'Actividad reciente registrada para esta cuenta.'),
      buildKpiCard('Trading', timelineSummary.trading_events || 0, 'Aperturas y cierres visibles en el feed.'),
      buildKpiCard('Cuenta', timelineSummary.account_events || 0, 'Cambios de wallet, key, términos o acceso.'),
      buildKpiCard('Control', timelineSummary.control_events || 0, timelineSummary.live_trade ? 'Hay una operación viva registrada.' : 'Sin operación activa ahora mismo.'),
    );
  }

  renderEventSummary(elements.latestOpenSummary, 'Última apertura', data.last_open_summary, 'Sin aperturas registradas.');
  renderEventSummary(elements.latestCloseSummary, 'Último cierre', data.last_close_summary, 'Sin cierres registrados.');

  if (elements.userActivityList) {
    if (!timeline.length) {
      elements.userActivityList.className = 'list-stack empty-state';
      elements.userActivityList.textContent = 'Sin actividad reciente.';
    } else {
      elements.userActivityList.className = 'list-stack';
      elements.userActivityList.innerHTML = '';
      timeline.slice(0, 6).forEach((item) => {
        const article = document.createElement('article');
        article.className = 'list-item';
        article.innerHTML = `
          <div class="list-item-header">
            <div>
              <div class="list-item-title">${item.title || 'Actividad'}</div>
              <div class="list-item-meta">${item.at ? formatDate(item.at) : 'Sin timestamp'}</div>
            </div>
            <span class="status-pill ${pillClass(item.tone || 'neutral')}">${item.badge || 'INFO'}</span>
          </div>
          <div class="list-item-meta">${formatActivityDetail(item)}</div>
        `;
        elements.userActivityList.appendChild(article);
      });
    }
  }

  if (elements.timelineCount) {
    elements.timelineCount.textContent = String(timeline.length || 0);
  }

  if (elements.operationsTimelineList) {
    if (!timeline.length) {
      elements.operationsTimelineList.className = 'list-stack empty-state';
      elements.operationsTimelineList.textContent = 'Sin actividad reciente.';
    } else {
      elements.operationsTimelineList.className = 'list-stack';
      elements.operationsTimelineList.innerHTML = '';
      timeline.forEach((item) => {
        const article = document.createElement('article');
        article.className = 'list-item';
        article.innerHTML = `
          <div class="list-item-header">
            <div>
              <div class="list-item-title">${item.title || 'Actividad'}</div>
              <div class="list-item-meta">${item.at ? formatDate(item.at) : 'Sin timestamp'} · ${item.family || 'info'}</div>
            </div>
            <span class="status-pill ${pillClass(item.tone || 'neutral')}">${item.badge || 'INFO'}</span>
          </div>
          <div class="list-item-meta">${formatActivityDetail(item)}</div>
        `;
        elements.operationsTimelineList.appendChild(article);
      });
    }
  }

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
    const badgeClass = trade.result_tone || (profit > 0 ? 'success' : (profit < 0 ? 'danger' : 'neutral'));

    item.innerHTML = `
      <div class="list-item-header">
        <div>
          <div class="list-item-title">${trade.symbol || 'Trade'} · ${trade.side || 'N/A'}</div>
          <div class="list-item-meta">${formatDate(trade.timestamp)} · ${trade.result_label || 'Trade'}</div>
        </div>
        <span class="status-pill ${badgeClass}">${profitLabel}</span>
      </div>
      <div class="list-item-grid">
        <div><span class="metric-label">Entry</span><strong>${trade.entry_price ?? '—'}</strong></div>
        <div><span class="metric-label">Exit</span><strong>${trade.exit_price ?? '—'}</strong></div>
        <div><span class="metric-label">Qty</span><strong>${trade.qty ?? '—'}</strong></div>
        <div><span class="metric-label">Score</span><strong>${trade.best_score ?? '—'}</strong></div>
        <div><span class="metric-label">Razón</span><strong>${trade.exit_reason || trade.close_source || '—'}</strong></div>
        <div><span class="metric-label">Fuente</span><strong>${trade.pnl_source || '—'}</strong></div>
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

function renderSystemRuntime(payload) {
  state.systemRuntime = payload;
  renderPublicSystemOverview(payload);
  setSummarySystemVisibility();

  const hasTechnicalDetails = Boolean(payload.components && Object.keys(payload.components).length);

  if (elements.systemTabButton) {
    elements.systemTabButton.classList.toggle('hidden', !hasTechnicalDetails);
  }

  if (!hasTechnicalDetails) {
    if (elements.systemHealthGridPanel) elements.systemHealthGridPanel.innerHTML = '';
    if (elements.systemRuntimeGridPanel) elements.systemRuntimeGridPanel.innerHTML = '';
    if (elements.backendHealthCard) elements.backendHealthCard.innerHTML = '';
    if (elements.systemActivityList) {
      elements.systemActivityList.className = 'list-stack empty-state';
      elements.systemActivityList.textContent = 'Los detalles técnicos solo están disponibles para administración.';
    }
    return;
  }

  renderTechnicalSystemPanel(payload);
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
    buildKpiCard('Trades 30d', tradeStats.total || 0, 'Global del sistema.'),
    buildKpiCard('Wins', tradeStats.wins || 0, `Win rate ${tradeStats.win_rate ?? 0}%`),
    buildKpiCard('Losses', tradeStats.losses || 0, `Decisivas ${tradeStats.win_rate_decisive ?? 0}%`),
    buildKpiCard('Profit factor', tradeStats.profit_factor === Infinity ? '∞' : tradeStats.profit_factor ?? 0, `PnL ${tradeStats.pnl_total ?? 0}`),
    buildKpiCard('Gross profit', tradeStats.gross_profit ?? 0, 'Cierres positivos.'),
    buildKpiCard('Gross loss', tradeStats.gross_loss ?? 0, 'Pérdidas acumuladas.')
  );

  renderAdminActionHistory(
    elements.adminRecentActions,
    data.recent_actions || [],
    'Sin acciones administrativas registradas todavía.',
  );

  if (elements.adminBulkMigrationStatus) {
    elements.adminBulkMigrationStatus.textContent = `Legacy pendientes: ${security.legacy_plaintext_private_keys || 0} · Keys cifradas: ${security.encrypted_private_keys || 0}`;
  }
}

function formatDateTimeOrDash(value) {
  return value ? formatDate(value) : '—';
}

function renderAdminPlanPreview(preview) {
  if (!elements.adminPlanPreviewBox) return;
  if (!preview || !preview.ok) {
    elements.adminPlanPreviewBox.textContent = 'Sin previsualización todavía.';
    return;
  }

  const planCopy = `Plan destino: ${planLabel(preview.target_plan || preview.new_plan)}`;
  const baseCopy = preview.base_type === 'current_expiry'
    ? 'La extensión se sumará desde el vencimiento actual.'
    : 'La extensión empezará desde hoy porque no hay acceso vigente.';

  elements.adminPlanPreviewBox.innerHTML = `
    <strong>Previsualización antes de aplicar</strong><br />
    ${planCopy}. ${baseCopy}
    <div class="preview-grid">
      <div class="preview-item">
        <span class="preview-label">Plan actual</span>
        <strong>${planLabel(preview.previous_plan)}</strong>
      </div>
      <div class="preview-item">
        <span class="preview-label">Plan resultante</span>
        <strong>${planLabel(preview.new_plan)}</strong>
      </div>
      <div class="preview-item">
        <span class="preview-label">Días vigentes</span>
        <strong>${preview.previous_days_remaining || 0}</strong>
      </div>
      <div class="preview-item">
        <span class="preview-label">Días resultantes</span>
        <strong>${preview.new_days_remaining || 0}</strong>
      </div>
      <div class="preview-item">
        <span class="preview-label">Vencimiento actual</span>
        <strong>${formatDateTimeOrDash(preview.previous_expires_at)}</strong>
      </div>
      <div class="preview-item">
        <span class="preview-label">Nuevo vencimiento</span>
        <strong>${formatDateTimeOrDash(preview.new_expires_at)}</strong>
      </div>
    </div>
  `;
}

function updateAdminPlanGrantHint(user = state.adminSelectedUser) {
  if (!elements.adminPlanGrantHint) return;
  const targetPlan = elements.adminPlanSelect ? elements.adminPlanSelect.value : 'premium';
  if (!user || !user.user_id) {
    elements.adminPlanGrantHint.textContent = 'Carga un usuario para aplicar días exactos sin simular una compra automática.';
    return;
  }

  const planText = planLabel(targetPlan);
  let baseMessage = user.plan_active
    ? `El usuario tiene acceso vigente hasta ${formatDate(user.plan_expires_at)}. Los días nuevos se sumarán desde ese vencimiento.`
    : 'El usuario no tiene acceso vigente. Los días nuevos se aplicarán desde hoy.';

  if (targetPlan === 'trial' && user.plan === 'premium' && user.plan_active) {
    baseMessage = 'No se puede aplicar PRUEBA mientras el usuario tenga PREMIUM activo.';
  }

  elements.adminPlanGrantHint.textContent = `${baseMessage} Plan seleccionado: ${planText}. Esta acción es manual y no cuenta como compra automática.`;
}

async function refreshAdminPlanPreview() {
  const selected = state.adminSelectedUser;
  if (!selected || !selected.user_id || !elements.adminPlanDaysInput) {
    renderAdminPlanPreview(null);
    return;
  }
  const targetPlan = elements.adminPlanSelect ? elements.adminPlanSelect.value : 'premium';
  const rawDays = elements.adminPlanDaysInput.value.trim();
  const days = Number.parseInt(rawDays, 10);
  if (!Number.isInteger(days) || days <= 0) {
    renderAdminPlanPreview(null);
    return;
  }
  try {
    const payload = await apiFetch(`/api/v1/admin/users/${selected.user_id}/plan/manual-days-preview?plan=${encodeURIComponent(targetPlan)}&days=${days}`);
    renderAdminPlanPreview(payload.preview || null);
  } catch (error) {
    elements.adminPlanPreviewBox.textContent = error.message || 'No se pudo calcular la previsualización.';
  }
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
  const remainingDays = Number(user.plan_days_remaining || 0);
  const expiryCopy = user.plan_active && user.plan_expires_at ? ` · Vence ${formatDate(user.plan_expires_at)} · ${remainingDays} día(s) restantes` : '';
  elements.adminUserSubtitle.textContent = `ID ${user.user_id} · Plan ${user.plan || 'none'}${expiryCopy} · Trading ${user.trading_status || 'inactive'}`;
  updateAdminPlanGrantHint(user);
  if (elements.adminPlanSelect && !elements.adminPlanSelect.value) {
    elements.adminPlanSelect.value = 'premium';
  }
  if (elements.adminPlanDaysInput && !elements.adminPlanDaysInput.value) {
    elements.adminPlanDaysInput.value = '7';
  }
  void refreshAdminPlanPreview();
  elements.adminUserStats.innerHTML = '';
  elements.adminUserStats.append(
    buildKpiCard('Wallet', user.wallet_configured ? truncateMiddle(user.wallet || '—', 18) : 'Pendiente', 'Estado de wallet.'),
    buildKpiCard('Private key', user.private_key_configured ? 'Configurada' : 'Pendiente', user.private_key_storage || 'not_configured'),
    buildKpiCard('Plan', user.plan || 'none', user.plan_active ? `Vence ${formatDate(user.plan_expires_at)} · ${user.plan_days_remaining || 0} día(s)` : 'Sin acceso vigente.'),
    buildKpiCard('Términos', user.terms_accepted ? 'Aceptados' : 'Pendientes', user.terms_timestamp ? `TS ${formatDate(user.terms_timestamp)}` : 'Sin aceptación registrada.'),
    buildKpiCard('Trading', user.trading_status || 'inactive', user.last_open_at ? `Última apertura ${formatDate(user.last_open_at)}` : 'Sin aperturas registradas.'),
    buildKpiCard('Referidos válidos', user.referral_valid_count || 0, user.private_key_version ? `Cipher ${user.private_key_version}` : 'Sin versión de cifrado.')
  );

  if (elements.adminUserPerformance) {
    const perf = user.performance || {};
    elements.adminUserPerformance.innerHTML = '';
    elements.adminUserPerformance.append(
      buildAdminPerformanceCard('24h', perf['24h'] || {}),
      buildAdminPerformanceCard('7d', perf['7d'] || {}),
      buildAdminPerformanceCard('30d', perf['30d'] || {}),
    );
  }

  renderAdminActionHistory(
    elements.adminUserActionHistory,
    user.recent_admin_actions || [],
    'Sin acciones administrativas sobre este usuario todavía.',
  );

  if (elements.adminActivateTradingButton) {
    elements.adminActivateTradingButton.disabled = !user.terms_accepted || !user.wallet_configured || !user.private_key_configured;
  }
  if (elements.adminPauseTradingButton) {
    elements.adminPauseTradingButton.disabled = user.trading_status !== 'active';
  }
  if (elements.adminMigrateKeyButton) {
    elements.adminMigrateKeyButton.disabled = !user.private_key_configured || user.private_key_storage === 'encrypted';
  }
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
  if (elements.systemTabButton) elements.systemTabButton.classList.add('hidden');
  if (elements.adminTabButton) elements.adminTabButton.classList.add('hidden');
  setSummarySystemVisibility(false);
  if (elements.accessStatsGrid) elements.accessStatsGrid.innerHTML = '';
  if (elements.performanceExecutiveGrid) elements.performanceExecutiveGrid.innerHTML = '';
  if (elements.systemRuntimeNotes) {
    elements.systemRuntimeNotes.className = 'readiness-list compact-list';
    elements.systemRuntimeNotes.innerHTML = '';
    elements.systemRuntimeNotes.appendChild(buildReadinessItem('Sin lectura del sistema', 'El estado operativo solo se resuelve con sesión real desde Telegram.', 'blocked'));
  }
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
  state.isAdmin = Boolean(payload.is_admin);
  if (elements.adminTabButton) elements.adminTabButton.classList.toggle('hidden', !state.isAdmin);
  if (elements.systemTabButton) elements.systemTabButton.classList.toggle('hidden', !state.isAdmin);
}

async function loadData() {
  setStatus('Sincronizando datos con el backend...', 'info');
  const [dashboard, control, performance, operations, referrals, systemRuntime] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/control'),
    apiFetch('/api/v1/performance'),
    apiFetch('/api/v1/operations?limit=20'),
    apiFetch('/api/v1/referrals'),
    apiFetch('/api/v1/system/runtime'),
  ]);

  renderDashboard(dashboard);
  renderControl(control);
  renderPerformance(performance);
  renderOperations(operations);
  renderReferrals(referrals);
  renderSystemRuntime(systemRuntime);

  if (state.isAdmin) {
    try {
      const admin = await apiFetch('/api/v1/admin/overview');
      renderAdmin(admin);
      if (elements.adminTabButton) elements.adminTabButton.classList.remove('hidden');
      if (elements.systemTabButton) elements.systemTabButton.classList.remove('hidden');
      setSummarySystemVisibility();
    } catch {
      if (elements.adminTabButton) elements.adminTabButton.classList.add('hidden');
      if (elements.systemTabButton) elements.systemTabButton.classList.add('hidden');
      state.isAdmin = false;
      setSummarySystemVisibility(false);
      document.querySelectorAll('[data-panel="admin"]').forEach((panel) => panel.classList.remove('is-active'));
      document.querySelectorAll('[data-panel="system"]').forEach((panel) => panel.classList.remove('is-active'));
    }
  } else {
    if (elements.adminTabButton) elements.adminTabButton.classList.add('hidden');
    if (elements.systemTabButton) elements.systemTabButton.classList.add('hidden');
    setSummarySystemVisibility(false);
    document.querySelectorAll('[data-panel="admin"]').forEach((panel) => panel.classList.remove('is-active'));
    document.querySelectorAll('[data-panel="system"]').forEach((panel) => panel.classList.remove('is-active'));
  }

  setStatus('MiniApp sincronizada correctamente.', 'success');
}

function bindTabs() {
  const buttons = Array.from(document.querySelectorAll('.tab'));
  const panels = Array.from(document.querySelectorAll('.panel'));
  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      const target = button.dataset.tab;
      if ((target === 'admin' || target === 'system') && !state.isAdmin) return;
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
  const [dashboard, control, performance, systemRuntime] = await Promise.all([
    apiFetch('/api/v1/dashboard?include_balance=false'),
    apiFetch('/api/v1/control'),
    apiFetch('/api/v1/performance'),
    apiFetch('/api/v1/system/runtime'),
  ]);
  renderDashboard(dashboard);
  renderControl(control);
  renderPerformance(performance);
  renderSystemRuntime(systemRuntime);
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

async function grantManualPremiumDaysForSelectedUser() {
  const selected = state.adminSelectedUser;
  if (!selected || !selected.user_id) {
    setStatus('Primero carga un usuario admin.', 'warning');
    return;
  }

  const targetPlan = elements.adminPlanSelect ? elements.adminPlanSelect.value : 'premium';
  const rawDays = elements.adminPlanDaysInput ? elements.adminPlanDaysInput.value.trim() : '';
  const days = Number.parseInt(rawDays, 10);
  if (!Number.isInteger(days) || days <= 0) {
    setStatus('Introduce una cantidad válida de días mayor que cero.', 'warning');
    return;
  }

  const reason = getAdminActionReason();
  let preview = null;
  try {
    const previewPayload = await apiFetch(`/api/v1/admin/users/${selected.user_id}/plan/manual-days-preview?plan=${encodeURIComponent(targetPlan)}&days=${days}`);
    preview = previewPayload.preview || null;
    renderAdminPlanPreview(preview);
  } catch (error) {
    setStatus(error.message || 'No se pudo calcular la previsualización.', 'error');
    return;
  }

  const confirmed = await requestStrongConfirmation(
    'Confirmación reforzada · Extensión manual de plan',
    [
      `Usuario: ${selected.username ? '@' + selected.username : 'ID ' + selected.user_id}`,
      `Plan actual: ${planLabel(preview?.previous_plan)}`,
      `Plan resultante: ${planLabel(preview?.new_plan || targetPlan)}`,
      `Días a agregar: ${days}`,
      `Nuevo vencimiento: ${formatDateTimeOrDash(preview?.new_expires_at)}`,
      reason ? `Motivo: ${reason}` : 'Motivo: sin nota administrativa',
    ],
  );
  if (!confirmed) return;

  elements.adminGrantPlanButton.disabled = true;
  try {
    const payload = await apiFetch(`/api/v1/admin/users/${selected.user_id}/plan/manual-days`, {
      method: 'POST',
      body: JSON.stringify({ plan: targetPlan, days, reason }),
    });
    renderAdminSelectedUser(payload.user);
    await loadData();
    setStatus(payload.message || `${planLabel(targetPlan)} actualizado por ${days} días.`, 'success');
  } catch (error) {
    setStatus(error.message || 'No se pudo aplicar la extensión manual.', 'error');
  } finally {
    elements.adminGrantPlanButton.disabled = false;
  }
}

async function runAdminSelectedAction(button, path, fallbackMessage, variant = 'success', confirmationTitle = 'Confirmación reforzada', confirmationDetails = []) {
  const selected = state.adminSelectedUser;
  if (!selected || !selected.user_id) {
    setStatus('Primero carga un usuario admin.', 'warning');
    return;
  }

  const reason = getAdminActionReason();
  const confirmed = await requestStrongConfirmation(confirmationTitle, [
    `Usuario: ${selected.username ? '@' + selected.username : 'ID ' + selected.user_id}`,
    ...confirmationDetails,
    reason ? `Motivo: ${reason}` : 'Motivo: sin nota administrativa',
  ]);
  if (!confirmed) return;

  if (button) button.disabled = true;
  try {
    const payload = await apiFetch(path(selected.user_id), {
      method: 'POST',
      body: JSON.stringify({ reason }),
    });
    if (payload.user) {
      renderAdminSelectedUser(payload.user);
    }
    await loadData();
    setStatus(payload.message || fallbackMessage, variant);
  } catch (error) {
    setStatus(error.message || fallbackMessage, 'error');
  } finally {
    if (button) button.disabled = false;
  }
}

async function activateTradingForSelectedUser() {
  await runAdminSelectedAction(
    elements.adminActivateTradingButton,
    (userId) => `/api/v1/admin/users/${userId}/trading/activate`,
    'No se pudo activar trading para el usuario.',
    'success',
    'Confirmación reforzada · Reanudar trading',
    ['Se intentará reactivar el motor del usuario.'],
  );
}

async function pauseTradingForSelectedUser() {
  await runAdminSelectedAction(
    elements.adminPauseTradingButton,
    (userId) => `/api/v1/admin/users/${userId}/trading/pause`,
    'No se pudo pausar trading para el usuario.',
    'warning',
    'Confirmación reforzada · Pausar trading',
    ['Esta acción detiene la operativa del usuario hasta nuevo cambio manual.'],
  );
}

async function migrateKeyForSelectedUser() {
  await runAdminSelectedAction(
    elements.adminMigrateKeyButton,
    (userId) => `/api/v1/admin/users/${userId}/security/migrate-key`,
    'No se pudo migrar la private key del usuario.',
    'success',
    'Confirmación reforzada · Migrar private key legacy',
    ['Se intentará convertir la key almacenada a cifrado en reposo.'],
  );
}

async function resetStatsForSelectedUser() {
  await runAdminSelectedAction(
    elements.adminResetStatsButton,
    (userId) => `/api/v1/admin/users/${userId}/stats/reset`,
    'No se pudo resetear el rendimiento del usuario.',
    'warning',
    'Confirmación reforzada · Resetear rendimiento',
    ['Esta acción reinicia el punto de partida de métricas del usuario.'],
  );
}

async function bulkMigrateLegacyKeys() {
  if (!elements.adminBulkMigrateButton) return;
  const reason = getAdminActionReason();
  const confirmed = await requestStrongConfirmation(
    'Confirmación reforzada · Migración masiva de keys legacy',
    [
      'Se migrarán hasta 25 registros legacy en esta ejecución.',
      reason ? `Motivo: ${reason}` : 'Motivo: sin nota administrativa',
    ],
  );
  if (!confirmed) return;

  elements.adminBulkMigrateButton.disabled = true;
  try {
    const payload = await apiFetch('/api/v1/admin/security/migrate-legacy-keys?limit=25', {
      method: 'POST',
      body: JSON.stringify({ reason }),
    });
    if (elements.adminBulkMigrationStatus) {
      elements.adminBulkMigrationStatus.textContent = payload.message || `Migradas ${payload.migrated_count || 0} keys.`;
    }
    await loadData();
    setStatus(payload.message || 'Migración legacy completada.', 'success');
  } catch (error) {
    setStatus(error.message || 'No se pudo ejecutar la migración legacy.', 'error');
  } finally {
    elements.adminBulkMigrateButton.disabled = false;
  }
}

function bindActions() {
  elements.refreshButton.addEventListener('click', async () => {
    elements.refreshButton.disabled = true;
  if (elements.systemTabButton) elements.systemTabButton.classList.add('hidden');
  if (elements.adminTabButton) elements.adminTabButton.classList.add('hidden');
  setSummarySystemVisibility(false);
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

  if (elements.adminGrantPlanButton) {
    elements.adminGrantPlanButton.addEventListener('click', grantManualPremiumDaysForSelectedUser);
  }
  if (elements.adminPlanSelect) {
    elements.adminPlanSelect.addEventListener('change', () => {
      updateAdminPlanGrantHint();
      void refreshAdminPlanPreview();
    });
  }
  if (elements.adminPlanDaysInput) {
    elements.adminPlanDaysInput.addEventListener('input', () => { void refreshAdminPlanPreview(); });
    elements.adminPlanDaysInput.addEventListener('change', () => { void refreshAdminPlanPreview(); });
  }
  if (elements.adminActivateTradingButton) {
    elements.adminActivateTradingButton.addEventListener('click', activateTradingForSelectedUser);
  }
  if (elements.adminPauseTradingButton) {
    elements.adminPauseTradingButton.addEventListener('click', pauseTradingForSelectedUser);
  }
  if (elements.adminMigrateKeyButton) {
    elements.adminMigrateKeyButton.addEventListener('click', migrateKeyForSelectedUser);
  }
  if (elements.adminResetStatsButton) {
    elements.adminResetStatsButton.addEventListener('click', resetStatsForSelectedUser);
  }
  if (elements.adminBulkMigrateButton) {
    elements.adminBulkMigrateButton.addEventListener('click', bulkMigrateLegacyKeys);
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
