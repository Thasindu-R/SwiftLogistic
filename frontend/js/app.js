/**
 * SwiftTrack Frontend – Unified Portal (Admin / Client / Driver)
 * Communicates with the API Gateway (port 8000).
 */

const API_BASE =
  window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.protocol}//${window.location.hostname}:8000`;
const WS_BASE = API_BASE.replace("http", "ws");

const AUTH_KEYS = {
  token: "swifttrack_token",
  user: "swifttrack_user",
  sidebarCollapsed: "swifttrack_sidebar_collapsed",
};

function readAuthFrom(storage) {
  try {
    const t = storage.getItem(AUTH_KEYS.token);
    const u = JSON.parse(storage.getItem(AUTH_KEYS.user) || "null");
    return { token: t || null, user: u || null };
  } catch {
    return { token: null, user: null };
  }
}

function loadAuth() {
  const local = readAuthFrom(localStorage);
  if (local.token && local.user) return { ...local, storage: localStorage };
  const session = readAuthFrom(sessionStorage);
  if (session.token && session.user)
    return { ...session, storage: sessionStorage };
  return { token: null, user: null, storage: localStorage };
}

function clearAuth() {
  [localStorage, sessionStorage].forEach((s) => {
    try {
      s.removeItem(AUTH_KEYS.token);
      s.removeItem(AUTH_KEYS.user);
    } catch {}
  });
}

function getActiveAuthStorage() {
  if (localStorage.getItem(AUTH_KEYS.token)) return localStorage;
  if (sessionStorage.getItem(AUTH_KEYS.token)) return sessionStorage;
  return localStorage;
}

const bootAuth = loadAuth();
let token = bootAuth.token;
let currentUser = bootAuth.user;
let trackingWs = null;
let _debounceTimers = {};

let _authStorage = bootAuth.storage;

/* ══════════════════════════════════════════════════════════ */
/*  TOASTS + NOTIFICATIONS                                   */
/* ══════════════════════════════════════════════════════════ */
const _notifications = [];

function toast(message, type = "info", opts = {}) {
  const container = $("toast-container");
  if (!container) return;

  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.setAttribute("role", "status");
  const title =
    opts.title ||
    (type === "success"
      ? "Success"
      : type === "error"
        ? "Error"
        : type === "warning"
          ? "Warning"
          : "Info");
  el.innerHTML = `
    <div class="row">
      <div>
        <div class="ttl">${escapeHtml(title)}</div>
        <div class="msg"></div>
      </div>
      <button class="x" aria-label="Close" type="button">×</button>
    </div>
  `;
  el.querySelector(".msg").textContent = String(message || "");

  const close = () => {
    el.classList.add("hide");
    setTimeout(() => el.remove(), 180);
  };
  el.querySelector(".x").onclick = close;
  container.appendChild(el);

  const duration =
    typeof opts.duration === "number"
      ? opts.duration
      : type === "error"
        ? 5500
        : type === "warning"
          ? 4500
          : 3200;
  if (duration > 0) setTimeout(close, duration);
}

function pushNotification({
  title,
  message,
  type = "info",
  timestamp = new Date().toISOString(),
} = {}) {
  _notifications.unshift({
    id: `${Date.now()}_${Math.random().toString(16).slice(2)}`,
    title: title || "Update",
    message: message || "",
    type,
    timestamp,
    read: false,
  });
  _notifications.splice(30);
  renderNotifications();
}

function renderNotifications() {
  const list = $("notification-list");
  const badge = $("notif-count");
  if (!list || !badge) return;

  const unread = _notifications.filter((n) => !n.read).length;
  badge.textContent = String(unread);
  badge.style.display = unread > 0 ? "inline-flex" : "none";

  if (!_notifications.length) {
    list.innerHTML = `<div class="no-data-message"><p>No notifications yet.</p></div>`;
    return;
  }

  list.innerHTML = _notifications
    .map((n) => {
      const when = n.timestamp ? new Date(n.timestamp).toLocaleString() : "";
      return `
        <div class="notif-item ${n.read ? "read" : "unread"}">
          <div class="nt">
            <div class="title">${escapeHtml(n.title)}</div>
            <div class="time">${escapeHtml(when)}</div>
          </div>
          <div class="msg">${escapeHtml(n.message)}</div>
        </div>
      `;
    })
    .join("");
}

function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

function toggleNotifications(force) {
  const panel = $("notification-panel");
  const overlay = $("notif-overlay");
  if (!panel || !overlay) return;
  const show =
    typeof force === "boolean" ? force : panel.style.display !== "block";
  panel.style.display = show ? "block" : "none";
  overlay.style.display = show ? "block" : "none";

  if (show) {
    _notifications.forEach((n) => (n.read = true));
    renderNotifications();
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  HELPERS                                                  */
/* ══════════════════════════════════════════════════════════ */
function headers() {
  const h = { "Content-Type": "application/json" };
  if (token) h["Authorization"] = `Bearer ${token}`;
  return h;
}

async function api(method, path, body = null) {
  const opts = { method, headers: headers() };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // Auto-logout on expired / invalid token
    if (res.status === 401 && token) {
      clearAuth();
      token = null;
      currentUser = null;
      showLogin();
      toast("Session expired — please sign in again.", "warning");
    }
    throw new Error(data.detail || JSON.stringify(data));
  }
  return data;
}

function $(id) {
  return document.getElementById(id);
}

function debounce(fn, ms) {
  return function () {
    clearTimeout(_debounceTimers[fn.name]);
    _debounceTimers[fn.name] = setTimeout(fn, ms);
  };
}

function shortId(id) {
  return id ? id.substring(0, 8) + "..." : "";
}
function fmtDate(d) {
  return d ? new Date(d).toLocaleString() : "-";
}

function showModal(id) {
  $(id).style.display = "flex";
}
function hideModal(id) {
  $(id).style.display = "none";
}

/* ══════════════════════════════════════════════════════════ */
/*  AUTH                                                     */
/* ══════════════════════════════════════════════════════════ */
function showLogin() {
  const loginForm = $("login-form");
  const registerForm = $("register-form");
  if (loginForm) loginForm.style.display = "block";
  if (registerForm) registerForm.style.display = "none";
  const err = $("auth-error");
  if (err) err.textContent = "";
}
function showRegister() {
  const loginForm = $("login-form");
  const registerForm = $("register-form");
  if (loginForm) loginForm.style.display = "none";
  if (registerForm) registerForm.style.display = "block";
  const err = $("auth-error");
  if (err) err.textContent = "";
}

async function login() {
  const btn = $("login-btn");
  const err = $("auth-error");
  if (err) err.textContent = "";
  if (btn) {
    btn.classList.add("is-loading");
    btn.disabled = true;
  }
  try {
    const remember = $("remember-me") ? $("remember-me").checked : true;
    const data = await api("POST", "/api/auth/login", {
      username: $("login-username").value,
      password: $("login-password").value,
    });
    setAuthState(data.access_token, data.user, remember);
    const roleField = $("login-role");
    if (roleField) roleField.value = data.user?.role || "";
    toast("Signed in successfully.", "success");
    pushNotification({
      title: "Signed in",
      message: `Welcome back, ${data.user?.full_name || data.user?.username || ""}`,
      type: "success",
    });
  } catch (e) {
    if (err) err.textContent = e.message;
    toast(e.message || "Login failed.", "error");
  } finally {
    if (btn) {
      btn.classList.remove("is-loading");
      btn.disabled = false;
    }
  }
}

function forgotPassword() {
  toast(
    "Password resets are not implemented in this demo. Ask an admin to reset your credentials.",
    "info",
  );
}

async function register() {
  try {
    const data = await api("POST", "/api/auth/register", {
      username: $("reg-username").value,
      email: $("reg-email").value,
      full_name: $("reg-fullname").value,
      phone: $("reg-phone").value,
      password: $("reg-password").value,
    });
    setAuthState(data.access_token, data.user);
  } catch (e) {
    $("auth-error").textContent = e.message;
  }
}

function setAuthState(newToken, user, remember = true) {
  token = newToken;
  currentUser = user;
  clearAuth();
  _authStorage = remember ? localStorage : sessionStorage;
  _authStorage.setItem(AUTH_KEYS.token, token);
  _authStorage.setItem(AUTH_KEYS.user, JSON.stringify(user));
  showDashboard();
}

function logout() {
  token = null;
  currentUser = null;
  clearAuth();
  $("auth-section").style.display = "flex";
  $("dashboard-section").style.display = "none";
  if (trackingWs) {
    trackingWs.close();
    trackingWs = null;
  }
  const roleField = $("login-role");
  if (roleField) roleField.value = "Auto-detected after sign in";
}

function toggleSidebar() {
  const sidebar = $("sidebar");
  const overlay = $("sidebar-overlay");
  sidebar.classList.toggle("open");
  overlay.classList.toggle("open");
}

function toggleSidebarCollapse() {
  const dash = $("dashboard-section");
  if (!dash) return;
  dash.classList.toggle("sidebar-collapsed");
  try {
    localStorage.setItem(
      AUTH_KEYS.sidebarCollapsed,
      dash.classList.contains("sidebar-collapsed") ? "1" : "0",
    );
  } catch {}
}

/* ══════════════════════════════════════════════════════════ */
/*  DASHBOARD INIT                                           */
/* ══════════════════════════════════════════════════════════ */
function showDashboard() {
  $("auth-section").style.display = "none";
  $("dashboard-section").style.display = "flex";
  $("user-info").textContent = currentUser.full_name || currentUser.username;
  const roleBadge = $("user-role-badge");
  if (roleBadge) roleBadge.textContent = currentUser.role;
  const avatarEl = $("user-avatar");
  if (avatarEl)
    avatarEl.textContent = (
      currentUser.full_name ||
      currentUser.username ||
      "U"
    )
      .charAt(0)
      .toUpperCase();

  // Mount React dashboard shells (if present) before running any tab loaders.
  // This preserves the element IDs that the existing DOM-based loaders expect.
  try {
    if (window.mountDashboards) window.mountDashboards();
  } catch (e) {
    console.warn("React dashboards failed to mount", e);
  }

  // Apply sidebar collapse preference
  try {
    const collapsed = localStorage.getItem(AUTH_KEYS.sidebarCollapsed) === "1";
    if (collapsed) $("dashboard-section").classList.add("sidebar-collapsed");
  } catch {}

  renderNotifications();

  buildTabs();
}

/* SVG icon fragments for sidebar nav */
const _si =
  'width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"';
const NAV_ICONS = {
  "admin-dashboard": `<svg ${_si}><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>`,
  analytics: `<svg ${_si}><path d="M3 3v18h18"/><path d="M7 14l3-3 3 2 5-6"/><circle cx="7" cy="14" r="1"/><circle cx="10" cy="11" r="1"/><circle cx="13" cy="13" r="1"/><circle cx="18" cy="7" r="1"/></svg>`,
  "system-status": `<svg ${_si}><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>`,
  "message-queue": `<svg ${_si}><polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11L2 12v6a2 2 0 002 2h16a2 2 0 002-2v-6l-3.45-6.89A2 2 0 0016.76 4H7.24a2 2 0 00-1.79 1.11z"/></svg>`,
  "integration-log": `<svg ${_si}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>`,
  "failed-messages": `<svg ${_si}><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
  "system-logs": `<svg ${_si}><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="8" y1="13" x2="16" y2="13"/><line x1="8" y1="17" x2="16" y2="17"/></svg>`,
  "client-dashboard": `<svg ${_si}><path d="M3 10l9-7 9 7"/><path d="M9 22V12h6v10"/></svg>`,
  "driver-dashboard": `<svg ${_si}><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>`,
  "route-map": `<svg ${_si}><path d="M9 18l-6 3V6l6-3 6 3 6-3v15l-6 3-6-3z"/><line x1="9" y1="3" x2="9" y2="18"/><line x1="15" y1="6" x2="15" y2="21"/></svg>`,
  "update-status": `<svg ${_si}><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>`,
  users: `<svg ${_si}><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>`,
  "all-orders": `<svg ${_si}><path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/></svg>`,
  orders: `<svg ${_si}><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/></svg>`,
  "new-order": `<svg ${_si}><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>`,
  manifests: `<svg ${_si}><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>`,
  integration: `<svg ${_si}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>`,
  tracking: `<svg ${_si}><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
  "track-delivery": `<svg ${_si}><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>`,
  "driver-orders": `<svg ${_si}><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>`,
  "my-manifest": `<svg ${_si}><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>`,
  "update-delivery": `<svg ${_si}><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg>`,
  driver: `<svg ${_si}><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>`,
};

const TAB_CONFIG = {
  admin: [
    { id: "admin-dashboard", label: "Dashboard", load: loadAdminDashboard },
    { id: "system-status", label: "System Status", load: loadSystemStatusTab },
    { id: "message-queue", label: "Message Queue", load: loadMessageQueue },
    {
      id: "integration-log",
      label: "Integration Log",
      load: loadIntegrationLog,
    },
    {
      id: "failed-messages",
      label: "Failed Messages",
      load: loadFailedMessagesTab,
    },
    { id: "all-orders", label: "All Orders", load: loadAllOrders },
    { id: "users", label: "Users", load: loadUsers },
    { id: "manifests", label: "Manifests", load: loadAllManifests },
  ],
  client: [
    { id: "client-dashboard", label: "Dashboard", load: loadClientDashboard },
    { id: "new-order", label: "New Order", load: null },
    { id: "orders", label: "My Orders", load: loadClientOrders },
    { id: "track-delivery", label: "Track Delivery", load: null },
  ],
  driver: [
    { id: "driver-dashboard", label: "Dashboard", load: loadDriverDashboard },
    { id: "my-manifest", label: "My Manifest", load: loadMyManifest },
    {
      id: "update-delivery",
      label: "Update Delivery",
      load: loadUpdateDelivery,
    },
    { id: "route-map", label: "Route", load: loadRouteView },
  ],
};

let currentTabLoad = null;

function buildTabs() {
  const nav = $("nav-tabs");
  nav.innerHTML = "";
  const tabs = TAB_CONFIG[currentUser.role] || TAB_CONFIG.client;
  tabs.forEach((t, i) => {
    const btn = document.createElement("button");
    btn.className = "nav-tab" + (i === 0 ? " active" : "");
    const icon = NAV_ICONS[t.id] || "";
    btn.innerHTML = icon + `<span>${t.label}</span>`;
    btn.onclick = () => switchTab(t.id, btn, t.load);
    nav.appendChild(btn);
  });
  // Activate first tab
  const first = tabs[0];
  switchTab(first.id, nav.querySelector(".nav-tab"), first.load);
}

function switchTab(tabId, btnEl, loadFn) {
  document
    .querySelectorAll(".tab-content")
    .forEach((el) => (el.style.display = "none"));
  document
    .querySelectorAll(".nav-tab")
    .forEach((el) => el.classList.remove("active"));
  const panel = $(`${tabId}-tab`);
  if (panel) panel.style.display = "block";
  if (btnEl) btnEl.classList.add("active");
  if (loadFn) loadFn();
  currentTabLoad = loadFn;
  /* Update page title in topbar */
  const titleEl = $("page-title");
  if (titleEl && btnEl) {
    const span = btnEl.querySelector("span");
    titleEl.textContent = span ? span.textContent : btnEl.textContent;
  } else if (titleEl && tabId === "profile") {
    titleEl.textContent = "My Profile";
  }
  /* Close sidebar on mobile after selection */
  const sidebar = $("sidebar");
  const overlay = $("sidebar-overlay");
  if (sidebar && sidebar.classList.contains("open")) {
    sidebar.classList.remove("open");
    if (overlay) overlay.classList.remove("open");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: ANALYTICS                                         */
/* ══════════════════════════════════════════════════════════ */
let _chartOrders = null;
let _chartStatus = null;

function statusToPct(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("cancel")) return 100;
  if (s.includes("fail")) return 100;
  if (s.includes("deliver")) return 100;
  if (s.includes("transit") || s.includes("picked")) return 70;
  if (s.includes("process")) return 40;
  if (s.includes("confirm")) return 25;
  if (s.includes("pend") || s.includes("create")) return 10;
  return 0;
}

async function loadAnalytics() {
  const ordersCanvas = $("chart-orders");
  const statusCanvas = $("chart-status");
  if (!ordersCanvas || !statusCanvas) return;
  if (typeof Chart === "undefined") {
    toast("Chart.js failed to load.", "error");
    return;
  }

  try {
    // Pie: status distribution
    const stats = await api("GET", "/api/orders/stats/summary");
    const statusLabels = [
      "pending",
      "confirmed",
      "processing",
      "in_transit",
      "delivered",
      "failed",
      "cancelled",
    ];
    const statusValues = statusLabels.map((k) => Number(stats[k] || 0));

    if (_chartStatus) _chartStatus.destroy();
    _chartStatus = new Chart(statusCanvas.getContext("2d"), {
      type: "doughnut",
      data: {
        labels: statusLabels.map((s) => s.replace(/_/g, " ")),
        datasets: [
          {
            data: statusValues,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { position: "bottom" },
        },
      },
    });

    // Line: recent orders by day (client-side aggregation)
    const recent = await api("GET", "/api/orders/?limit=100");
    const orders = recent.orders || [];
    const countsByDay = {};
    orders.forEach((o) => {
      const day = o.created_at ? new Date(o.created_at) : null;
      if (!day) return;
      const key = day.toISOString().slice(0, 10);
      countsByDay[key] = (countsByDay[key] || 0) + 1;
    });

    const days = [];
    const values = [];
    for (let i = 13; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      days.push(key.slice(5));
      values.push(countsByDay[key] || 0);
    }

    if (_chartOrders) _chartOrders.destroy();
    _chartOrders = new Chart(ordersCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels: days,
        datasets: [
          {
            label: "Orders",
            data: values,
            tension: 0.35,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
        },
        scales: {
          y: { beginAtZero: true, ticks: { precision: 0 } },
        },
      },
    });
  } catch (e) {
    toast(e.message || "Failed to load analytics.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: SYSTEM LOGS  (sub-tabs)                           */
/* ══════════════════════════════════════════════════════════ */

/** Switch between sub-tab panels inside System Logs */
function switchLogsSubTab(panelId, btnEl) {
  // Hide all sub-panels
  document
    .querySelectorAll(".logs-sub-panel")
    .forEach((p) => (p.style.display = "none"));
  // Show target
  const target = $("logs-" + panelId + "-panel");
  if (target) target.style.display = "";
  // Toggle active button
  document
    .querySelectorAll("#logs-sub-tabs .sub-tab")
    .forEach((b) => b.classList.remove("active"));
  if (btnEl) btnEl.classList.add("active");
  // Load data for the tab
  const loaders = {
    integration: loadSystemLogs,
    transactions: loadTransactionHistory,
    audit: loadAuditTrail,
    errors: loadErrorSummary,
  };
  if (loaders[panelId]) loaders[panelId]();
}

/** Integration Logs sub-tab */
async function loadSystemLogs() {
  const container = $("system-logs-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading logs…</p>';
  try {
    const status = $("log-status-filter") ? $("log-status-filter").value : "";
    const target = $("log-target-filter") ? $("log-target-filter").value : "";
    const hours = $("log-hours-filter") ? $("log-hours-filter").value : "24";
    let qs = `?hours=${hours}&limit=100&offset=0`;
    if (status) qs += `&severity=${status}`;
    if (target) qs += `&target_system=${target}`;
    const data = await api("GET", `/api/orders/admin/logs/integration${qs}`);
    const logs = data.logs || [];
    if (!logs.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No logs found for the selected filters.</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead>
          <tr><th>Time</th><th>Flow</th><th>Event</th><th>Status</th><th>Severity</th><th>Order</th><th>Duration</th></tr>
        </thead>
        <tbody>
          ${logs
            .map((l) => {
              const flow = `${l.source_system || "?"} → ${l.target_system || "?"}`;
              const st = String(l.status || "");
              const sev = String(l.severity || "");
              return `<tr>
              <td>${fmtDate(l.created_at)}</td>
              <td>${escapeHtml(flow)}</td>
              <td>${escapeHtml(String(l.event_type || ""))}</td>
              <td><span class="badge ${escapeHtml(st)}">${escapeHtml(st)}</span></td>
              <td><span class="badge ${escapeHtml(sev)}">${escapeHtml(sev)}</span></td>
              <td class="mono">${shortId(l.order_id)}</td>
              <td>${l.duration_ms != null ? `${l.duration_ms}ms` : "—"}</td>
            </tr>`;
            })
            .join("")}
        </tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load logs.", "error");
  }
}

/** Transactions sub-tab */
async function loadTransactionHistory() {
  const container = $("transactions-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading transactions…</p>';
  try {
    const state = $("txn-state-filter") ? $("txn-state-filter").value : "";
    let qs = "?limit=50&offset=0";
    if (state) qs += `&state=${state}`;
    const data = await api("GET", `/api/orders/admin/logs/transactions${qs}`);
    const txns = data.transactions || [];
    if (!txns.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No transactions found.</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead>
          <tr><th>Saga ID</th><th>Order</th><th>State</th><th>Steps</th><th>Started</th><th>Updated</th></tr>
        </thead>
        <tbody>
          ${txns
            .map(
              (t) => `<tr>
            <td class="mono">${shortId(t.saga_id)}</td>
            <td class="mono">${shortId(t.order_id)}</td>
            <td><span class="badge ${t.state || ""}">${t.state || "-"}</span></td>
            <td>${t.current_step || "-"}${t.total_steps ? "/" + t.total_steps : ""}</td>
            <td>${fmtDate(t.created_at)}</td>
            <td>${fmtDate(t.updated_at)}</td>
          </tr>`,
            )
            .join("")}
        </tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load transactions.", "error");
  }
}

/** Audit Trail sub-tab */
async function loadAuditTrail() {
  const container = $("audit-trail-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading audit trail…</p>';
  try {
    const entity = $("audit-entity-filter")
      ? $("audit-entity-filter").value
      : "";
    const hours = $("audit-hours-filter")
      ? $("audit-hours-filter").value
      : "24";
    let qs = `?hours=${hours}&limit=100&offset=0`;
    if (entity) qs += `&entity_type=${entity}`;
    const data = await api("GET", `/api/orders/admin/logs/audit${qs}`);
    const entries = data.entries || data.audit_trail || [];
    if (!entries.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No audit entries found.</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead>
          <tr><th>Time</th><th>Entity</th><th>Action</th><th>Actor</th><th>Order</th><th>Details</th></tr>
        </thead>
        <tbody>
          ${entries
            .map(
              (e) => `<tr>
            <td>${fmtDate(e.created_at || e.timestamp)}</td>
            <td>${escapeHtml(e.entity_type || "-")}</td>
            <td>${escapeHtml(e.action || "-")}</td>
            <td>${escapeHtml(e.actor_type || "-")}${e.actor_id ? " #" + e.actor_id : ""}</td>
            <td class="mono">${shortId(e.order_id || e.entity_id)}</td>
            <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${escapeHtml(JSON.stringify(e.details || {}))}">${escapeHtml(truncate(JSON.stringify(e.details || {}), 60))}</td>
          </tr>`,
            )
            .join("")}
        </tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load audit trail.", "error");
  }
}

/** Error Summary sub-tab */
async function loadErrorSummary() {
  const container = $("error-summary-content");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading error summary…</p>';
  try {
    const data = await api(
      "GET",
      "/api/orders/admin/logs/errors/summary?hours=24",
    );
    const summary = data.summary || data;

    const byTarget = summary.by_target || summary.by_system || {};
    const bySeverity = summary.by_severity || {};
    const totalErrors = summary.total_errors || 0;
    const recentErrors = summary.recent_errors || [];

    container.innerHTML = `
      <div class="stats-grid" style="margin-bottom: 20px">
        <div class="stat-card"><div class="stat-value" style="color: var(--danger)">${totalErrors}</div><div class="stat-label">Total Errors (24h)</div></div>
        ${Object.entries(byTarget)
          .map(
            ([sys, count]) =>
              `<div class="stat-card"><div class="stat-value">${count}</div><div class="stat-label">${sys.toUpperCase()} Errors</div></div>`,
          )
          .join("")}
        ${Object.entries(bySeverity)
          .map(
            ([sev, count]) =>
              `<div class="stat-card"><div class="stat-value">${count}</div><div class="stat-label">${sev}</div></div>`,
          )
          .join("")}
      </div>
      ${totalErrors === 0 ? '<div class="no-data-message"><p>No errors in the last 24 hours — all systems operating normally.</p></div>' : ""}
      ${
        recentErrors.length
          ? `
        <h4 style="margin: 12px 0 8px">Recent Errors</h4>
        <table>
          <thead><tr><th>Time</th><th>System</th><th>Event</th><th>Severity</th><th>Message</th></tr></thead>
          <tbody>
            ${recentErrors
              .map(
                (e) => `<tr>
              <td>${fmtDate(e.created_at)}</td>
              <td>${escapeHtml(e.target_system || e.source_system || "-")}</td>
              <td>${escapeHtml(e.event_type || "-")}</td>
              <td><span class="badge ${e.severity || ""}">${e.severity || "-"}</span></td>
              <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(e.error_message || "-")}</td>
            </tr>`,
              )
              .join("")}
          </tbody>
        </table>`
          : ""
      }`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load error summary.", "error");
  }
}

/** Helper: truncate string */
function truncate(str, len) {
  return str.length > len ? str.slice(0, len) + "…" : str;
}

/* ══════════════════════════════════════════════════════════ */
/*  CLIENT: DASHBOARD                                        */
/* ══════════════════════════════════════════════════════════ */
let _trackCardOrderId = null;

function updateTrackCard(order, opts = {}) {
  const orderEl = $("track-card-order");
  const statusEl = $("track-card-status");
  const bar = $("track-progress-bar");
  if (!orderEl || !statusEl || !bar) return;

  const orderId = order?.order_id || "—";
  const status = String(order?.status || opts.status || "Awaiting updates");
  _trackCardOrderId = order?.order_id || null;

  orderEl.textContent = orderId;
  statusEl.textContent = status.replace(/_/g, " ");
  statusEl.className = `badge ${status}`;
  bar.style.width = `${statusToPct(status)}%`;
}

async function loadClientDashboard() {
  const recentWrap = $("client-recent-orders");
  if (!recentWrap) return;

  recentWrap.innerHTML = '<p class="muted">Loading…</p>';
  try {
    const data = await api("GET", "/api/orders/");
    const orders = data.orders || [];

    // Stat cards
    const submitted = orders.length;
    const transit = orders.filter((o) => o.status === "in_transit").length;
    const delivered = orders.filter((o) => o.status === "delivered").length;
    const statSub = $("stat-submitted");
    const statTr = $("stat-transit");
    const statDel = $("stat-delivered");
    if (statSub) statSub.textContent = submitted;
    if (statTr) statTr.textContent = transit;
    if (statDel) statDel.textContent = delivered;

    if (!orders.length) {
      recentWrap.innerHTML =
        '<div class="no-data-message"><p>No orders yet.</p></div>';
      updateTrackCard(null);
      return;
    }

    // Recent orders mini-table (last 3)
    const recent = orders.slice(0, 3);
    recentWrap.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Order</th>
            <th>Recipient</th>
            <th>Status</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          ${recent
            .map(
              (o) => `
            <tr style="cursor:pointer" onclick="viewOrderDetail('${o.order_id}')">
              <td class="mono">${shortId(o.order_id)}</td>
              <td>${escapeHtml(o.recipient_name || "—")}</td>
              <td><span class="badge ${escapeHtml(o.status)}">${escapeHtml(
                String(o.status || "").replace(/_/g, " "),
              )}</span></td>
              <td>${fmtDate(o.created_at)}</td>
            </tr>
          `,
            )
            .join("")}
        </tbody>
      </table>
    `;

    // Live tracking card: focus newest order
    const latest = orders[0];
    updateTrackCard(latest);

    // Connect live tracking for the card (keeps it “real-time”)
    if (latest?.order_id) {
      connectTrackingWs(latest.order_id);
    }
  } catch (e) {
    recentWrap.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load dashboard.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: DASHBOARD                                        */
/* ══════════════════════════════════════════════════════════ */
let _driverManifestCache = null;

async function _fetchDriverManifests() {
  const data = await api(
    "GET",
    `/api/tracking/manifests/driver/${currentUser.id}`,
  );
  const manifests = data.manifests || data || [];
  _driverManifestCache = manifests;
  // Prefetch order details
  const orderIds = manifests.flatMap((m) =>
    (m.items || []).map((i) => i.order_id),
  );
  await fetchOrderDetails(orderIds);
  return manifests;
}

async function loadDriverDashboard() {
  try {
    const manifests = await _fetchDriverManifests();

    // Calculate stats
    let total = 0,
      completed = 0,
      pending = 0,
      failed = 0;
    manifests.forEach((m) => {
      (m.items || []).forEach((item) => {
        total++;
        if (item.status === "delivered") completed++;
        else if (item.status === "failed") failed++;
        else pending++;
      });
    });

    // Update stat cards
    if ($("ds-total")) $("ds-total").textContent = total;
    if ($("ds-completed")) $("ds-completed").textContent = completed;
    if ($("ds-pending")) $("ds-pending").textContent = pending;
    if ($("ds-failed")) $("ds-failed").textContent = failed;

    // Check urgent notifications
    checkDriverNotifications(manifests);

    // Today's manifest preview (first 3 stops)
    const previewEl = $("driver-manifest-preview");
    if (previewEl) {
      const allItems = manifests.flatMap((m) =>
        (m.items || []).map((item, idx) => ({
          ...item,
          seq: item.sequence || idx + 1,
        })),
      );
      allItems.sort((a, b) => a.seq - b.seq);
      const first3 = allItems.slice(0, 3);
      if (!first3.length) {
        previewEl.innerHTML =
          '<p class="muted">No deliveries assigned for today.</p>';
      } else {
        previewEl.innerHTML = first3
          .map((item) => {
            const order = orderDetailsCache[item.order_id] || {};
            return `
            <div class="manifest-preview-item">
              <span class="manifest-preview-seq">${item.seq}</span>
              <div class="manifest-preview-info">
                <div class="manifest-preview-name">${escapeHtml(order.recipient_name || "Customer")}</div>
                <div class="manifest-preview-addr">${escapeHtml(order.delivery_address || "—")}</div>
              </div>
              <span class="badge ${item.status}">${item.status.replace(/_/g, " ")}</span>
            </div>`;
          })
          .join("");
      }
    }
  } catch (e) {
    toast(e.message || "Failed to load driver dashboard.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: MY MANIFEST                                      */
/* ══════════════════════════════════════════════════════════ */
async function loadMyManifest() {
  const container = $("manifest-stops-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading manifest…</p>';
  try {
    const manifests = _driverManifestCache || (await _fetchDriverManifests());

    const allItems = manifests.flatMap((m) =>
      (m.items || []).map((item, idx) => ({
        ...item,
        seq: item.sequence || idx + 1,
      })),
    );
    allItems.sort((a, b) => a.seq - b.seq);

    if (!allItems.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No deliveries assigned for today.</p></div>';
      return;
    }

    container.innerHTML = allItems
      .map((item) => {
        const order = orderDetailsCache[item.order_id] || {};
        return `
        <div class="manifest-stop-card">
          <div class="manifest-stop-header" onclick="this.parentElement.classList.toggle('expanded')">
            <span class="manifest-stop-seq">${item.seq}</span>
            <div class="manifest-stop-info">
              <div class="manifest-stop-name">${escapeHtml(order.recipient_name || "Customer")}</div>
              <div class="manifest-stop-addr">${escapeHtml(order.delivery_address || "—")}</div>
            </div>
            <span class="badge ${item.status}">${item.status.replace(/_/g, " ")}</span>
            ${order.priority === "urgent" ? '<span class="badge urgent">URGENT</span>' : ""}
            ${order.priority === "high" ? '<span class="badge high">HIGH</span>' : ""}
            <span class="manifest-stop-chevron">▸</span>
          </div>
          <div class="manifest-stop-detail">
            <table style="box-shadow:none;margin:0">
              <tr><td><strong>Order ID</strong></td><td class="mono">${shortId(item.order_id)}</td></tr>
              <tr><td><strong>Pickup</strong></td><td>${escapeHtml(order.pickup_address || "—")}</td></tr>
              <tr><td><strong>Package</strong></td><td>${escapeHtml(order.package_description || "—")} (${order.package_weight || 0} kg)</td></tr>
              <tr><td><strong>Phone</strong></td><td>${escapeHtml(order.recipient_phone || "—")}</td></tr>
              <tr><td><strong>Priority</strong></td><td><span class="badge ${order.priority || "normal"}">${order.priority || "normal"}</span></td></tr>
              <tr><td><strong>Notes</strong></td><td>${escapeHtml(order.notes || "None")}</td></tr>
              ${item.proof_of_delivery ? '<tr><td><strong>POD</strong></td><td style="color:var(--success)">✓ Proof of delivery captured</td></tr>' : ""}
            </table>
          </div>
        </div>`;
      })
      .join("");
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load manifest.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: UPDATE DELIVERY                                  */
/* ══════════════════════════════════════════════════════════ */
let _udSignatureCanvas = null,
  _udSignatureCtx = null,
  _udIsDrawing = false,
  _udPhotoData = null;

async function loadUpdateDelivery() {
  const select = $("ud-order-select");
  if (!select) return;

  // Populate dropdown with pending/in_transit/picked_up orders
  try {
    const manifests = _driverManifestCache || (await _fetchDriverManifests());
    const items = manifests.flatMap((m) =>
      (m.items || []).filter((i) =>
        ["pending", "in_transit", "picked_up"].includes(i.status),
      ),
    );

    select.innerHTML = '<option value="">— Select an order —</option>';
    items.forEach((item) => {
      const order = orderDetailsCache[item.order_id] || {};
      const opt = document.createElement("option");
      opt.value = item.order_id;
      opt.textContent = `${shortId(item.order_id)} — ${order.recipient_name || "Customer"} (${item.status.replace(/_/g, " ")})`;
      select.appendChild(opt);
    });

    $("ud-order-info").style.display = "none";
    $("ud-result").innerHTML = "";
  } catch (e) {
    toast(e.message || "Failed to load orders.", "error");
  }
}

async function loadUdOrderDetails() {
  const orderId = $("ud-order-select").value;
  const infoPanel = $("ud-order-info");
  if (!orderId) {
    infoPanel.style.display = "none";
    return;
  }

  infoPanel.style.display = "block";
  $("ud-status").value = "delivered";
  $("ud-notes").value = "";
  $("ud-result").innerHTML = "";
  _udPhotoData = null;
  toggleUdSections();

  // Photo preview reset
  const pp = $("ud-photo-preview");
  if (pp)
    pp.innerHTML = '<span class="photo-placeholder">No photo captured</span>';
  const pi = $("ud-photo-input");
  if (pi) pi.value = "";

  // Init signature canvas
  initUdSignatureCanvas();

  // Show order details
  const infoGrid = $("ud-info-grid");
  try {
    let order = orderDetailsCache[orderId];
    if (!order || !order.recipient_name) {
      order = await api("GET", `/api/orders/${orderId}`);
      orderDetailsCache[orderId] = order;
    }
    infoGrid.innerHTML = `
      <div class="delivery-info-item"><div class="delivery-info-label">Customer</div><div class="delivery-info-value">${escapeHtml(order.recipient_name || "N/A")}</div></div>
      <div class="delivery-info-item"><div class="delivery-info-label">Phone</div><div class="delivery-info-value">${escapeHtml(order.recipient_phone || "N/A")}</div></div>
      <div class="delivery-info-item"><div class="delivery-info-label">Address</div><div class="delivery-info-value">${escapeHtml(order.delivery_address || "N/A")}</div></div>
      <div class="delivery-info-item"><div class="delivery-info-label">Package</div><div class="delivery-info-value">${escapeHtml(order.package_description || "N/A")} (${order.package_weight || 0} kg)</div></div>
      <div class="delivery-info-item"><div class="delivery-info-label">Priority</div><div class="delivery-info-value"><span class="badge ${order.priority}">${order.priority || "normal"}</span></div></div>
      <div class="delivery-info-item"><div class="delivery-info-label">Notes</div><div class="delivery-info-value">${escapeHtml(order.notes || "None")}</div></div>
    `;
  } catch (e) {
    infoGrid.innerHTML = '<p class="error">Could not load order details</p>';
  }
}

function toggleUdSections() {
  const status = $("ud-status").value;
  const fail = $("ud-failure-section");
  const pod = $("ud-pod-section");
  if (fail) fail.style.display = status === "failed" ? "block" : "none";
  if (pod) pod.style.display = status === "delivered" ? "block" : "none";
}

function toggleUdOther() {
  const other = $("ud-failure-other");
  if (other)
    other.style.display =
      $("ud-failure-reason").value === "Other" ? "block" : "none";
}

function initUdSignatureCanvas() {
  _udSignatureCanvas = $("ud-signature-canvas");
  if (!_udSignatureCanvas) return;
  _udSignatureCtx = _udSignatureCanvas.getContext("2d");
  clearUdSignature();
  _udSignatureCanvas.onmousedown = (e) => {
    _udIsDrawing = true;
    _udSignatureCtx.beginPath();
    _udSignatureCtx.moveTo(e.offsetX, e.offsetY);
  };
  _udSignatureCanvas.onmousemove = (e) => {
    if (!_udIsDrawing) return;
    _udSignatureCtx.lineTo(e.offsetX, e.offsetY);
    _udSignatureCtx.strokeStyle = "#000";
    _udSignatureCtx.lineWidth = 2;
    _udSignatureCtx.lineCap = "round";
    _udSignatureCtx.stroke();
  };
  _udSignatureCanvas.onmouseup = () => {
    _udIsDrawing = false;
  };
  _udSignatureCanvas.onmouseout = () => {
    _udIsDrawing = false;
  };
  _udSignatureCanvas.ontouchstart = (e) => {
    e.preventDefault();
    const t = e.touches[0];
    const r = _udSignatureCanvas.getBoundingClientRect();
    _udIsDrawing = true;
    _udSignatureCtx.beginPath();
    _udSignatureCtx.moveTo(t.clientX - r.left, t.clientY - r.top);
  };
  _udSignatureCanvas.ontouchmove = (e) => {
    if (!_udIsDrawing) return;
    e.preventDefault();
    const t = e.touches[0];
    const r = _udSignatureCanvas.getBoundingClientRect();
    _udSignatureCtx.lineTo(t.clientX - r.left, t.clientY - r.top);
    _udSignatureCtx.strokeStyle = "#000";
    _udSignatureCtx.lineWidth = 2;
    _udSignatureCtx.lineCap = "round";
    _udSignatureCtx.stroke();
  };
  _udSignatureCanvas.ontouchend = () => {
    _udIsDrawing = false;
  };
}

function clearUdSignature() {
  if (!_udSignatureCtx || !_udSignatureCanvas) return;
  _udSignatureCtx.fillStyle = "#fff";
  _udSignatureCtx.fillRect(
    0,
    0,
    _udSignatureCanvas.width,
    _udSignatureCanvas.height,
  );
}

function getUdSignatureData() {
  if (!_udSignatureCanvas) return null;
  const d = _udSignatureCtx.getImageData(
    0,
    0,
    _udSignatureCanvas.width,
    _udSignatureCanvas.height,
  );
  const empty = d.data.every((v, i) => i % 4 === 3 || v >= 250);
  if (empty) return null;
  return _udSignatureCanvas.toDataURL("image/png");
}

function handleUdPhotoSelect(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    _udPhotoData = e.target.result;
    const preview = $("ud-photo-preview");
    if (preview)
      preview.innerHTML = `<img src="${_udPhotoData}" alt="Proof of delivery">`;
  };
  reader.readAsDataURL(file);
}

function clearUdPhoto() {
  _udPhotoData = null;
  const preview = $("ud-photo-preview");
  if (preview)
    preview.innerHTML =
      '<span class="photo-placeholder">No photo captured</span>';
  const input = $("ud-photo-input");
  if (input) input.value = "";
}

async function submitUdUpdate() {
  const orderId = $("ud-order-select").value;
  if (!orderId) {
    toast("Please select an order.", "error");
    return;
  }
  const status = $("ud-status").value;
  const body = { status };

  if (status === "delivered") {
    const sig = getUdSignatureData();
    if (sig) body.signature_data = sig;
    if (_udPhotoData) body.proof_of_delivery = _udPhotoData;
  }

  if (status === "failed") {
    const reason = $("ud-failure-reason").value;
    if (!reason) {
      $("ud-result").innerHTML =
        '<p class="error">Please select a failure reason.</p>';
      return;
    }
    body.failure_reason =
      reason === "Other" ? $("ud-failure-other").value || "Other" : reason;
  }

  if ($("ud-notes").value.trim()) body.notes = $("ud-notes").value.trim();

  try {
    await api("PATCH", `/api/tracking/delivery-items/${orderId}`, body);
    $("ud-result").innerHTML =
      '<div class="ud-success"><p class="success">✅ WMS updated. Client notified via WebSocket.</p></div>';
    delete orderDetailsCache[orderId];
    _driverManifestCache = null;
    // Refresh the dropdown after a moment
    setTimeout(() => loadUpdateDelivery(), 1500);
  } catch (e) {
    $("ud-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: ROUTE VIEW                                       */
/* ══════════════════════════════════════════════════════════ */
async function loadRouteView() {
  const stopsList = $("route-stops-list");
  const statsEl = $("route-summary-stats");
  const label = $("route-map-label");
  if (!stopsList) return;

  stopsList.innerHTML = '<p class="muted">Loading route…</p>';
  if (statsEl) statsEl.style.display = "none";

  try {
    const manifests = _driverManifestCache || (await _fetchDriverManifests());

    // Get all items sorted by sequence
    const allItems = manifests.flatMap((m) =>
      (m.items || []).map((item, idx) => ({
        ...item,
        seq: item.sequence || idx + 1,
      })),
    );
    allItems.sort((a, b) => a.seq - b.seq);

    // Extract route data
    const routeData = manifests.find((m) => m.route_data)?.route_data || null;
    let route = null;
    if (routeData) {
      try {
        route =
          typeof routeData === "string" ? JSON.parse(routeData) : routeData;
      } catch (e) {
        /* ignore */
      }
    }

    // Summary stats
    const totalStops = allItems.length;
    const distance =
      route?.estimated_distance_km || route?.total_distance || null;
    const time = route?.estimated_duration_min || route?.total_time || null;

    if (statsEl) {
      statsEl.style.display = "flex";
      $("route-total-stops").textContent = totalStops;
      $("route-distance").textContent = distance ? distance + " km" : "—";
      $("route-time").textContent = time ? time + " min" : "—";
    }

    if (!allItems.length) {
      stopsList.innerHTML =
        '<div class="no-data-message"><p>No stops in route.</p></div>';
      if (label) label.textContent = "No route data available";
      return;
    }

    // Numbered list of stops
    stopsList.innerHTML = `<ol class="route-numbered-list">${allItems
      .map((item) => {
        const order = orderDetailsCache[item.order_id] || {};
        return `
        <li class="route-list-item">
          <div class="route-list-info">
            <strong>${escapeHtml(order.recipient_name || "Stop " + item.seq)}</strong>
            <span class="muted">${escapeHtml(order.delivery_address || "—")}</span>
          </div>
          <span class="badge ${item.status}">${item.status.replace(/_/g, " ")}</span>
        </li>`;
      })
      .join("")}</ol>`;

    // Update map label and pins
    if (label) label.textContent = `Route available • ${totalStops} stops`;

    // Render dynamic pins on map
    const pinsEl = $("route-map-pins");
    if (pinsEl && totalStops > 0) {
      pinsEl.innerHTML = allItems
        .map((item, idx) => {
          const pct =
            totalStops === 1 ? 50 : 15 + (idx / (totalStops - 1)) * 70;
          const topPct = 25 + Math.sin((idx / totalStops) * Math.PI) * 35;
          return `<div class="mini-map-pin" style="left:${pct}%;top:${topPct}%"><span class="pin-number">${idx + 1}</span></div>`;
        })
        .join("");
    }
  } catch (e) {
    stopsList.innerHTML = `<p class="error">${e.message}</p>`;
    toast(e.message || "Failed to load route.", "error");
  }
}

// Keep old loadRouteMap as alias
async function loadRouteMap() {
  return loadRouteView();
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: DASHBOARD                                         */
/* ══════════════════════════════════════════════════════════ */
async function loadAdminDashboard() {
  // Greeting banner
  const greetEl = $("dash-greeting-msg");
  const subEl = $("dash-greeting-sub");
  const clockEl = $("dash-clock");
  const dateEl = $("dash-date");
  if (greetEl) {
    const hour = new Date().getHours();
    const name = (
      currentUser.full_name ||
      currentUser.username ||
      "Admin"
    ).split(" ")[0];
    const greeting =
      hour < 12
        ? "Good morning"
        : hour < 17
          ? "Good afternoon"
          : "Good evening";
    greetEl.textContent = `${greeting}, ${name}! 👋`;
  }
  if (subEl) {
    const days = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ];
    const d = new Date();
    subEl.textContent = `${days[d.getDay()]} – here's your logistics overview.`;
  }
  // Live clock
  function updateClock() {
    const now = new Date();
    if (clockEl) {
      clockEl.textContent = now.toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    if (dateEl) {
      dateEl.textContent = now.toLocaleDateString([], {
        weekday: "short",
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    }
  }
  updateClock();
  if (window._dashClockInterval) clearInterval(window._dashClockInterval);
  window._dashClockInterval = setInterval(updateClock, 1000);

  // Stats
  try {
    const stats = await api("GET", "/api/orders/stats/summary");
    $("stats-cards").innerHTML = [
      {
        n: stats.total_orders,
        l: "Total Orders",
        cls: "stat-indigo",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/></svg>',
      },
      {
        n: stats.delivered,
        l: "Delivered",
        cls: "stat-green",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
      },
      {
        n: stats.in_transit,
        l: "In Transit",
        cls: "stat-teal",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
      },
      {
        n: stats.failed,
        l: "Failed",
        cls: "stat-red",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
      },
    ]
      .map(
        (s) =>
          `<div class="stat-card ${s.cls}"><div class="stat-card-header"><div class="stat-icon">${s.icon}</div></div><div class="stat-num">${s.n ?? 0}</div><div class="stat-label">${s.l}</div></div>`,
      )
      .join("");
  } catch (e) {
    $("stats-cards").innerHTML = `<p class="error">${e.message}</p>`;
  }

  // 3 System Health Dots
  try {
    const idata = await api("GET", "/api/orders/admin/integration-status");
    const integrations = idata.integrations || {};
    const dots = { cms: "CMS", ros: "ROS", wms: "WMS" };
    const dotsEl = $("admin-health-dots");
    if (dotsEl)
      dotsEl.innerHTML = Object.entries(dots)
        .map(([key, label]) => {
          const info = integrations[key] || {};
          const color =
            info.status === "healthy"
              ? "#27ae60"
              : info.status === "degraded"
                ? "#f39c12"
                : "#e74c3c";
          return `<div class="health-dot-item"><span class="health-dot" style="background:${color}"></span> ${label}</div>`;
        })
        .join("");
  } catch (e) {
    const el = $("admin-health-dots");
    if (el) el.innerHTML = `<p class="error">${e.message}</p>`;
  }

  // Recent Activity Feed
  try {
    const events = await api("GET", "/api/tracking/events/recent?limit=5");
    const evts = events.events || events || [];
    const feedEl = $("admin-activity-feed");
    if (feedEl)
      feedEl.innerHTML = evts.length
        ? evts
            .map(
              (ev) => `<div class="activity-item">
          <div class="activity-dot ${ev.status || "info"}"></div>
          <div class="activity-content">
            <strong>${(ev.event_type || "").replace(/_/g, " ")}</strong>
            <span class="muted"> — ${shortId(ev.order_id)}</span>
          </div>
          <span class="activity-time">${fmtDate(ev.timestamp)}</span>
        </div>`,
            )
            .join("")
        : '<p class="muted">No recent activity</p>';
  } catch (e) {
    const el = $("admin-activity-feed");
    if (el) el.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: NEW TAB FUNCTIONS                                 */
/* ══════════════════════════════════════════════════════════ */
async function loadSystemStatusTab() {
  const container = $("system-status-cards");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading system status…</p>';
  try {
    const data = await api("GET", "/api/orders/admin/integration-status");
    const integrations = data.integrations || {};
    const systemMeta = {
      cms: {
        name: "CMS",
        protocol: "SOAP / XML",
        desc: "Client Management System",
      },
      ros: {
        name: "ROS",
        protocol: "REST / JSON",
        desc: "Route Optimization System",
      },
      wms: {
        name: "WMS",
        protocol: "TCP / IP",
        desc: "Warehouse Management System",
      },
    };
    container.innerHTML =
      Object.entries(integrations)
        .map(([key, info]) => {
          const meta = systemMeta[key] || {
            name: key.toUpperCase(),
            protocol: "Unknown",
            desc: key,
          };
          const statusColor =
            info.status === "healthy"
              ? "#27ae60"
              : info.status === "degraded"
                ? "#f39c12"
                : "#e74c3c";
          const statusLabel =
            info.status === "healthy"
              ? "Healthy"
              : info.status === "degraded"
                ? "Degraded"
                : "Unhealthy";
          return `<div class="ss-card">
        <div class="ss-card-header">
          <div class="ss-dot" style="background:${statusColor}"></div>
          <h3>${meta.name}</h3>
          <span class="ss-status-badge" style="color:${statusColor}">${statusLabel}</span>
        </div>
        <div class="ss-card-body">
          <div class="ss-row"><span class="ss-label">Protocol</span><span class="ss-value">${meta.protocol}</span></div>
          <div class="ss-row"><span class="ss-label">Response Time</span><span class="ss-value">${info.response_time_ms || "—"}ms</span></div>
          <div class="ss-row"><span class="ss-label">Last Ping</span><span class="ss-value">${info.last_check ? fmtDate(info.last_check) : "—"}</span></div>
          <div class="ss-row"><span class="ss-label">Success Rate (24h)</span><span class="ss-value">${info.success_rate_24h ? info.success_rate_24h.toFixed(1) + "%" : "—"}</span></div>
          <div class="ss-row"><span class="ss-label">Calls (24h)</span><span class="ss-value">${info.total_calls_24h || 0}</span></div>
        </div>
        <div class="ss-card-footer">${meta.desc}</div>
      </div>`;
        })
        .join("") ||
      '<div class="no-data-message"><p>No integration data available</p></div>';
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function loadMessageQueue() {
  const statsContainer = $("mq-stats");
  const tableContainer = $("mq-table");
  if (!statsContainer || !tableContainer) return;
  try {
    const sysData = await api("GET", "/api/orders/admin/system-status");
    const eventsData = await api(
      "GET",
      "/api/tracking/integration-events?limit=20",
    );
    const events = eventsData.events || [];
    const pending = events.filter((ev) => ev.status === "pending").length;
    const processed = events.filter((ev) => ev.status === "success").length;
    const failed =
      sysData.dlq_messages ||
      events.filter((ev) => ev.status === "failed").length;
    statsContainer.innerHTML = `
      <div class="mq-stat"><div class="mq-stat-value">${pending}</div><div class="mq-stat-label">Pending</div></div>
      <div class="mq-stat processed"><div class="mq-stat-value">${processed}</div><div class="mq-stat-label">Processed Today</div></div>
      <div class="mq-stat failed"><div class="mq-stat-value">${failed}</div><div class="mq-stat-label">Failed</div></div>`;
    if (!events.length) {
      tableContainer.innerHTML =
        '<div class="no-data-message"><p>No messages in queue</p></div>';
      return;
    }
    tableContainer.innerHTML = `
      <table>
        <thead><tr><th>Message ID</th><th>Type</th><th>Target System</th><th>Status</th><th>Time</th></tr></thead>
        <tbody>${events
          .map(
            (ev) => `<tr>
          <td class="mono">${ev.event_id || "—"}</td>
          <td>${escapeHtml(ev.event_type || "—")}</td>
          <td>${escapeHtml(ev.target_system || "—")}</td>
          <td><span class="badge ${ev.status}">${ev.status}</span></td>
          <td>${fmtDate(ev.created_at)}</td>
        </tr>`,
          )
          .join("")}</tbody>
      </table>`;
  } catch (e) {
    statsContainer.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function loadIntegrationLog() {
  const container = $("intlog-events-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading integration log…</p>';
  try {
    const status = $("intlog-status-filter")
      ? $("intlog-status-filter").value
      : "";
    const source = $("intlog-source-filter")
      ? $("intlog-source-filter").value
      : "";
    let qs = "?limit=50";
    if (status) qs += `&status=${status}`;
    if (source) qs += `&source=${source}`;
    const data = await api("GET", `/api/tracking/integration-events${qs}`);
    const events = data.events || [];
    if (!events.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No integration events found.</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead><tr><th>Timestamp</th><th>Event</th><th>From</th><th>To</th><th>Status</th></tr></thead>
        <tbody>${events
          .map(
            (ev) => `<tr>
          <td>${fmtDate(ev.created_at)}</td>
          <td>${escapeHtml(ev.event_type || "—")}</td>
          <td>${escapeHtml(ev.source_system || "—")}</td>
          <td>${escapeHtml(ev.target_system || "—")}</td>
          <td><span class="badge ${ev.status === "success" ? "delivered" : ev.status}">${ev.status === "success" ? "✅" : ev.status === "failed" ? "❌" : ev.status}</span></td>
        </tr>`,
          )
          .join("")}</tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function loadFailedMessagesTab() {
  const container = $("fm-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading failed messages…</p>';
  try {
    const data = await api("GET", "/api/orders/admin/failed-messages?limit=20");
    const messages = data.messages || [];
    const countEl = $("fm-count");
    if (countEl) countEl.textContent = data.count || 0;
    if (!messages.length) {
      container.innerHTML =
        '<div class="no-data-message"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="24" height="24"><polyline points="20 6 9 17 4 12"/></svg><p>No failed messages — all systems operating normally</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead><tr><th>System</th><th>Event Type</th><th>Error Reason</th><th>Retry Count</th><th>Order</th><th>Action</th></tr></thead>
        <tbody>${messages
          .map(
            (msg) => `<tr>
          <td><span class="badge ${(msg.target_system || "").toLowerCase()}">${msg.target_system}</span></td>
          <td>${escapeHtml(msg.event_type || "Unknown")}</td>
          <td class="fm-error-cell" title="${escapeHtml(msg.error_message || "")}">${escapeHtml((msg.error_message || "—").slice(0, 60))}</td>
          <td>${msg.retry_count}/${msg.max_retries}</td>
          <td class="mono">${shortId(msg.order_id)}</td>
          <td><button class="btn-small btn-warning" onclick="retryFailedMessage('${msg.event_id}')" ${!msg.can_retry ? "disabled" : ""}>${msg.can_retry ? "Retry" : "Max Retries"}</button></td>
        </tr>`,
          )
          .join("")}</tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function retryFailedMessage(eventId) {
  try {
    const result = await api(
      "POST",
      `/api/orders/admin/retry-event/${eventId}`,
    );
    if (result.success) {
      toast("Message retried — check Integration Log!", "success");
      loadFailedMessagesTab();
    } else {
      toast(`Retry failed: ${result.error || "Unknown error"}`, "error");
    }
  } catch (e) {
    toast(e.message || "Retry failed.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: SYSTEM STATUS                                     */
/* ══════════════════════════════════════════════════════════ */
async function loadSystemStatus() {
  const container = $("system-status-grid");
  if (!container) return;

  try {
    const data = await api("GET", "/api/orders/admin/system-status");

    // Render alerts first
    renderAlerts(data);

    // System status items
    const statusItems = [
      {
        name: "Overall System",
        status: data.overall_health || "unknown",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" y1="9" x2="9.01" y2="9"/><line x1="15" y1="9" x2="15.01" y2="9"/></svg>',
      },
      {
        name: "Database",
        status: data.database_health || "unknown",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>',
      },
      {
        name: "Message Queue",
        status: data.queue_health || "unknown",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="12" x2="2" y2="12"/><path d="M5.45 5.11L2 12v6a2 2 0 002 2h16a2 2 0 002-2v-6l-3.45-6.89A2 2 0 0016.76 4H7.24a2 2 0 00-1.79 1.11z"/></svg>',
      },
      {
        name: "DLQ Messages",
        status: data.dlq_messages > 0 ? "warning" : "healthy",
        value: data.dlq_messages || 0,
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
      },
      {
        name: "Active Sagas",
        status: data.active_sagas > 5 ? "warning" : "healthy",
        value: data.active_sagas || 0,
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg>',
      },
    ];

    container.innerHTML = statusItems
      .map((item) => {
        const statusClass =
          item.status === "warning" ? "degraded" : item.status;
        return `
        <div class="system-status-item">
          <div class="system-status-icon ${statusClass}">${item.icon}</div>
          <div class="system-status-info">
            <div class="system-status-name">${item.name}</div>
            <div class="system-status-value ${statusClass}">
              ${item.value !== undefined ? item.value : item.status.replace("_", " ")}
            </div>
          </div>
        </div>
      `;
      })
      .join("");
  } catch (e) {
    container.innerHTML = `<div class="no-data-message"><p>${e.message}</p></div>`;
  }
}

function renderAlerts(data) {
  const alertsContainer = $("dash-alerts");
  if (!alertsContainer) return;

  const alerts = [];

  // Check for critical conditions
  if (data.overall_health === "unhealthy") {
    alerts.push({
      type: "critical",
      title: "System Health Critical",
      message: "One or more critical systems are down",
      action: "Check system status",
    });
  } else if (data.overall_health === "degraded") {
    alerts.push({
      type: "warning",
      title: "System Health Degraded",
      message: "Some systems are experiencing issues",
      action: "Monitor closely",
    });
  }

  if (data.dlq_messages > 10) {
    alerts.push({
      type: "warning",
      title: "Dead Letter Queue Alert",
      message: `${data.dlq_messages} messages in DLQ requiring attention`,
      action: "Review DLQ",
    });
  }

  if (data.active_sagas > 10) {
    alerts.push({
      type: "info",
      title: "High Active Transactions",
      message: `${data.active_sagas} transactions in progress`,
      action: "Monitor sagas",
    });
  }

  if (alerts.length === 0) {
    alertsContainer.style.display = "none";
    return;
  }

  alertsContainer.style.display = "block";
  alertsContainer.innerHTML = alerts
    .map(
      (alert) => `
    <div class="dash-alert ${alert.type}">
      <div class="dash-alert-icon">
        ${
          alert.type === "critical"
            ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>'
            : alert.type === "error"
              ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>'
              : alert.type === "warning"
                ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>'
                : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>'
        }
      </div>
      <div class="dash-alert-content">
        <div class="dash-alert-title">${alert.title}</div>
        <div class="dash-alert-message">${alert.message}</div>
      </div>
      <div class="dash-alert-action">${alert.action}</div>
    </div>
  `,
    )
    .join("");
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: INTEGRATION STATUS                                */
/* ══════════════════════════════════════════════════════════ */
async function loadIntegrationStatus() {
  const container = $("integration-status-cards");
  if (!container) return;

  try {
    const data = await api("GET", "/api/orders/admin/integration-status");
    const integrations = data.integrations || {};

    const systemLabels = {
      cms: { name: "CMS", desc: "Client Management System (SOAP/XML)" },
      ros: { name: "ROS", desc: "Route Optimization System (REST/JSON)" },
      wms: { name: "WMS", desc: "Warehouse Management System (TCP/IP)" },
    };

    container.innerHTML =
      Object.entries(integrations)
        .map(([key, info]) => {
          const label = systemLabels[key] || {
            name: key.toUpperCase(),
            desc: key,
          };
          return `
        <div class="integration-card">
          <div class="integration-card-header">
            <h4>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
              </svg>
              ${label.name}
            </h4>
            <span class="integration-badge ${info.status}">${info.status}</span>
          </div>
          <div class="integration-stats">
            <div class="integration-stat">
              <div class="integration-stat-value">${info.response_time_ms || "-"}</div>
              <div class="integration-stat-label">Response (ms)</div>
            </div>
            <div class="integration-stat">
              <div class="integration-stat-value">${info.success_rate_24h ? info.success_rate_24h.toFixed(1) + "%" : "-"}</div>
              <div class="integration-stat-label">Success Rate</div>
            </div>
            <div class="integration-stat">
              <div class="integration-stat-value">${info.total_calls_24h || 0}</div>
              <div class="integration-stat-label">Calls (24h)</div>
            </div>
          </div>
          ${info.error_message ? `<div class="integration-error">${info.error_message}</div>` : ""}
          <div class="integration-card-footer">
            <span>${label.desc}</span>
            <span>${info.last_check ? "Checked: " + fmtDate(info.last_check) : ""}</span>
          </div>
        </div>
      `;
        })
        .join("") ||
      '<div class="no-data-message">No integration data available</div>';
  } catch (e) {
    container.innerHTML = `<div class="no-data-message"><p>${e.message}</p></div>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: FAILED MESSAGES                                   */
/* ══════════════════════════════════════════════════════════ */
async function loadFailedMessages() {
  const container = $("failed-messages-list");
  const countBadge = $("failed-msg-count");
  if (!container) return;

  try {
    const data = await api("GET", "/api/orders/admin/failed-messages?limit=10");
    const messages = data.messages || [];

    if (countBadge) {
      countBadge.textContent = data.count || 0;
      countBadge.style.display = data.count > 0 ? "inline" : "none";
    }

    if (!messages.length) {
      container.innerHTML = `
        <div class="no-data-message">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <polyline points="20 6 9 17 4 12"/>
          </svg>
          <p>No failed messages - all systems operating normally</p>
        </div>
      `;
      return;
    }

    container.innerHTML = messages
      .map(
        (msg) => `
      <div class="failed-message-item" data-event-id="${msg.event_id}">
        <div class="failed-message-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="12" cy="12" r="10"/>
            <line x1="15" y1="9" x2="9" y2="15"/>
            <line x1="9" y1="9" x2="15" y2="15"/>
          </svg>
        </div>
        <div class="failed-message-content">
          <div class="failed-message-type">${msg.event_type || "Unknown Event"}</div>
          <div class="failed-message-meta">
            <span>Target: <strong>${msg.target_system}</strong></span>
            <span>Order: ${shortId(msg.order_id)}</span>
            <span>Retries: ${msg.retry_count}/${msg.max_retries}</span>
          </div>
          ${msg.error_message ? `<div class="failed-message-error" title="${msg.error_message}">${msg.error_message}</div>` : ""}
        </div>
        <div class="failed-message-actions">
          <button class="btn-retry" onclick="retryFailedEvent('${msg.event_id}')" ${!msg.can_retry ? "disabled" : ""}>
            ${msg.can_retry ? "Retry" : "Max Retries"}
          </button>
        </div>
      </div>
    `,
      )
      .join("");
  } catch (e) {
    container.innerHTML = `<div class="no-data-message"><p>${e.message}</p></div>`;
  }
}

async function retryFailedEvent(eventId) {
  try {
    const result = await api(
      "POST",
      `/api/orders/admin/retry-event/${eventId}`,
    );
    if (result.success) toast("Retry triggered successfully!", "success");
    else toast(`Retry failed: ${result.error || "Unknown error"}`, "error");
    loadFailedMessages();
  } catch (e) {
    toast(e.message || "Retry failed.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: USER MANAGEMENT                                   */
/* ══════════════════════════════════════════════════════════ */
async function loadUsers() {
  try {
    const role = $("user-role-filter") ? $("user-role-filter").value : "";
    const search = $("user-search") ? $("user-search").value : "";
    let qs = "?limit=50";
    if (role) qs += `&role=${role}`;
    if (search) qs += `&search=${encodeURIComponent(search)}`;
    const data = await api("GET", `/api/auth/users${qs}`);
    const users = data.users || [];
    if (!users.length) {
      $("users-list").innerHTML = "<p>No users found.</p>";
      return;
    }
    $("users-list").innerHTML = `
      <table>
        <thead><tr><th>ID</th><th>Username</th><th>Full Name</th><th>Email</th><th>Role</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>${users
          .map(
            (u) => `
          <tr>
            <td>${u.id}</td>
            <td>${u.username}</td>
            <td>${u.full_name || "-"}</td>
            <td>${u.email || "-"}</td>
            <td><span class="badge ${u.role}">${u.role}</span></td>
            <td><span class="badge ${u.is_active ? "active" : "inactive"}">${u.is_active ? "Active" : "Inactive"}</span></td>
            <td>
              <button class="btn-xs btn-small" onclick="openEditUser(${u.id})">Edit</button>
              <button class="btn-xs btn-small ${u.is_active ? "btn-warning" : "btn-success"}" onclick="toggleUserStatus(${u.id}, ${u.is_active})">${u.is_active ? "Disable" : "Enable"}</button>
            </td>
          </tr>
        `,
          )
          .join("")}</tbody>
      </table>`;
  } catch (e) {
    $("users-list").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function createUser() {
  try {
    await api("POST", "/api/auth/users", {
      username: $("cu-username").value,
      email: $("cu-email").value,
      full_name: $("cu-fullname").value,
      phone: $("cu-phone").value,
      password: $("cu-password").value,
      role: $("cu-role").value,
    });
    $("cu-result").innerHTML = '<p class="success">User created!</p>';
    [
      "cu-username",
      "cu-email",
      "cu-fullname",
      "cu-phone",
      "cu-password",
    ].forEach((id) => ($(id).value = ""));
    loadUsers();
    setTimeout(() => hideModal("create-user-modal"), 1000);
  } catch (e) {
    $("cu-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function openEditUser(userId) {
  try {
    const u = await api("GET", `/api/auth/users/${userId}`);
    $("eu-id").value = u.id;
    $("eu-fullname").value = u.full_name || "";
    $("eu-email").value = u.email || "";
    $("eu-phone").value = u.phone || "";
    $("eu-role").value = u.role;
    $("eu-active").value = u.is_active ? "true" : "false";
    $("eu-result").innerHTML = "";
    showModal("edit-user-modal");
  } catch (e) {
    toast(e.message || "Failed to load user.", "error");
  }
}

async function updateUser() {
  try {
    const userId = $("eu-id").value;
    await api("PUT", `/api/auth/users/${userId}`, {
      full_name: $("eu-fullname").value,
      email: $("eu-email").value,
      phone: $("eu-phone").value,
      role: $("eu-role").value,
      is_active: $("eu-active").value === "true",
    });
    $("eu-result").innerHTML = '<p class="success">Updated!</p>';
    loadUsers();
    setTimeout(() => hideModal("edit-user-modal"), 800);
  } catch (e) {
    $("eu-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function toggleUserStatus(userId, isActive) {
  try {
    await api("PATCH", `/api/auth/users/${userId}/status`);
    loadUsers();
  } catch (e) {
    toast(e.message || "Failed to update user status.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: ALL ORDERS                                        */
/* ══════════════════════════════════════════════════════════ */
let allOrdersPage = 0;
async function loadAllOrders(page) {
  if (page !== undefined) allOrdersPage = page;
  try {
    const status = $("admin-order-status-filter")
      ? $("admin-order-status-filter").value
      : "";
    let qs = `?skip=${allOrdersPage * 20}&limit=20`;
    if (status) qs += `&status=${status}`;
    const data = await api("GET", `/api/orders/${qs}`);
    const orders = data.orders || [];
    if (!orders.length) {
      $("all-orders-list").innerHTML =
        '<div class="no-data-message"><p>No orders found.</p></div>';
    } else {
      $("all-orders-list").innerHTML = `
        <table>
          <thead><tr><th>Order ID</th><th>Client</th><th>Status</th><th>Created</th><th>Assigned Driver</th></tr></thead>
          <tbody>${orders
            .map(
              (o) => `<tr>
            <td class="mono">${shortId(o.order_id)}</td>
            <td>${escapeHtml(o.recipient_name || "—")}</td>
            <td><span class="badge ${o.status}">${o.status.replace(/_/g, " ")}</span></td>
            <td>${fmtDate(o.created_at)}</td>
            <td>${o.assigned_driver || o.driver_id || "—"}</td>
          </tr>`,
            )
            .join("")}</tbody>
        </table>`;
    }
    // pagination
    const total = data.total || orders.length;
    const pages = Math.ceil(total / 20);
    let pag = "";
    for (let i = 0; i < pages && i < 10; i++) {
      pag += `<button class="${i === allOrdersPage ? "active" : ""}" onclick="loadAllOrders(${i})">${i + 1}</button>`;
    }
    $("all-orders-pagination").innerHTML = pag;
  } catch (e) {
    $("all-orders-list").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: INTEGRATION EVENTS (Integration Monitor tab)      */
/* ══════════════════════════════════════════════════════════ */
async function loadIntegrationEvents() {
  // Load all three sections in parallel
  loadIntegrationHealthCards();
  loadIntegrationFailedMessages();
  loadIntegrationEventLog();
}

/** Integration Health Cards (top of Integration Monitor) */
async function loadIntegrationHealthCards() {
  const container = $("integration-health-cards");
  if (!container) return;
  try {
    const data = await api("GET", "/api/orders/admin/integration-status");
    const integrations = data.integrations || {};
    const systemLabels = {
      cms: { name: "CMS", proto: "SOAP/XML" },
      ros: { name: "ROS", proto: "REST/JSON" },
      wms: { name: "WMS", proto: "TCP/IP" },
    };
    container.innerHTML =
      Object.entries(integrations)
        .map(([key, info]) => {
          const label = systemLabels[key] || {
            name: key.toUpperCase(),
            proto: "Unknown",
          };
          const dotColor =
            info.status === "healthy"
              ? "#27ae60"
              : info.status === "degraded"
                ? "#f39c12"
                : "#e74c3c";
          return `
        <div class="integration-card" style="min-width:200px">
          <div class="integration-card-header">
            <h4>${label.name} <small style="color:#888;font-weight:normal">(${label.proto})</small></h4>
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${dotColor}" title="${info.status}"></span>
          </div>
          <div class="integration-stats">
            <div class="integration-stat">
              <div class="integration-stat-value">${info.response_time_ms || "-"}<small>ms</small></div>
              <div class="integration-stat-label">Response</div>
            </div>
            <div class="integration-stat">
              <div class="integration-stat-value">${info.success_rate_24h ? info.success_rate_24h.toFixed(0) + "%" : "-"}</div>
              <div class="integration-stat-label">Success</div>
            </div>
            <div class="integration-stat">
              <div class="integration-stat-value">${info.total_calls_24h || 0}</div>
              <div class="integration-stat-label">Calls</div>
            </div>
          </div>
        </div>`;
        })
        .join("") || '<div class="no-data-message">No integration data</div>';
  } catch (e) {
    container.innerHTML = `<div class="no-data-message"><p>${e.message}</p></div>`;
  }
}

/** Failed Messages section in Integration Monitor */
async function loadIntegrationFailedMessages() {
  const container = $("integration-failed-list");
  const countBadge = $("int-failed-count");
  if (!container) return;
  try {
    const data = await api("GET", "/api/orders/admin/failed-messages?limit=10");
    const messages = data.messages || [];
    if (countBadge) {
      countBadge.textContent = data.count || 0;
      countBadge.style.display = data.count > 0 ? "inline" : "none";
    }
    if (!messages.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No failed messages — all systems operating normally.</p></div>';
      return;
    }
    container.innerHTML = messages
      .map(
        (msg) => `
      <div class="failed-message-item" style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid #f0f0f0">
        <div style="flex:1">
          <strong>${msg.event_type || "Unknown"}</strong>
          <span class="badge failed" style="margin-left:6px">${msg.target_system}</span>
          <div style="font-size:0.82rem;color:#888;margin-top:2px">
            Order: ${shortId(msg.order_id)} · Retries: ${msg.retry_count}/${msg.max_retries}
            ${msg.error_message ? ` · <span style="color:var(--danger)">${escapeHtml(msg.error_message.slice(0, 80))}</span>` : ""}
          </div>
        </div>
        <button class="btn-small btn-warning" onclick="retryFailedEvent('${msg.event_id}')" ${!msg.can_retry ? "disabled" : ""}>
          ${msg.can_retry ? "Retry" : "Max Retries"}
        </button>
      </div>
    `,
      )
      .join("");
  } catch (e) {
    container.innerHTML = `<div class="no-data-message"><p>${e.message}</p></div>`;
  }
}

/** Event Log table in Integration Monitor */
async function loadIntegrationEventLog() {
  const container = $("integration-events-list");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading events…</p>';
  try {
    const status = $("int-status-filter") ? $("int-status-filter").value : "";
    const source = $("int-source-filter") ? $("int-source-filter").value : "";
    let qs = "?limit=50";
    if (status) qs += `&status=${status}`;
    if (source) qs += `&source=${source}`;
    const data = await api("GET", `/api/tracking/integration-events${qs}`);
    const events = data.events || [];
    if (!events.length) {
      container.innerHTML =
        "<div class='no-data-message'><p>No integration events found.</p></div>";
      return;
    }
    container.innerHTML = `
      <table>
        <thead><tr><th>ID</th><th>Order</th><th>Source</th><th>Target</th><th>Type</th><th>Status</th><th>Retries</th><th>Time</th><th>Actions</th></tr></thead>
        <tbody>${events
          .map(
            (ev) => `
          <tr>
            <td>${ev.event_id}</td>
            <td title="${ev.order_id || ""}">${shortId(ev.order_id)}</td>
            <td>${ev.source_system || "-"}</td>
            <td>${ev.target_system || "-"}</td>
            <td>${ev.event_type || "-"}</td>
            <td><span class="badge ${ev.status}">${ev.status}</span></td>
            <td>${ev.retry_count || 0}/${ev.max_retries || 3}</td>
            <td style="font-size:0.8rem">${fmtDate(ev.created_at)}</td>
            <td>${ev.status === "failed" ? `<button class="btn-xs btn-small btn-warning" onclick="retryIntegration(${ev.event_id})">Retry</button>` : ""}</td>
          </tr>
        `,
          )
          .join("")}</tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function retryIntegration(eventId) {
  try {
    await api("POST", `/api/tracking/integration-events/${eventId}/retry`);
    loadIntegrationEvents();
  } catch (e) {
    toast(e.message || "Failed to retry integration event.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  ADMIN: MANIFESTS                                         */
/* ══════════════════════════════════════════════════════════ */
async function loadAllManifests() {
  try {
    const data = await api("GET", "/api/tracking/manifests/all");
    const manifests = data.manifests || data || [];
    if (!manifests.length) {
      $("all-manifests-list").innerHTML = "<p>No manifests yet.</p>";
      return;
    }
    $("all-manifests-list").innerHTML = manifests
      .map(
        (m) => `
      <div class="manifest-card">
        <h3>📋 ${shortId(m.manifest_id)} <span class="badge ${m.status}">${m.status}</span></h3>
        <p style="color:#666;font-size:0.85rem">Driver: ${m.driver_id} | Date: ${m.date} | Items: ${(m.items || []).length}</p>
        ${(m.items || [])
          .map(
            (item) => `
          <div class="manifest-item">
            <span>📦 ${shortId(item.order_id)} (#${item.sequence})</span>
            <span class="badge ${item.status}">${item.status}</span>
            ${item.proof_of_delivery ? `<span style="color:#27ae60;font-size:0.8rem">✓ POD</span>` : ""}
          </div>
        `,
          )
          .join("")}
      </div>
    `,
      )
      .join("");
  } catch (e) {
    $("all-manifests-list").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function createManifest() {
  try {
    const orderIds = $("cm-orders")
      .value.trim()
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!orderIds.length) throw new Error("Enter at least one Order ID");
    // Load drivers for select
    const driverId = parseInt($("cm-driver-select").value);
    const date = $("cm-date").value || new Date().toISOString().split("T")[0];
    await api("POST", "/api/tracking/manifests", {
      driver_id: driverId,
      date: date,
      order_ids: orderIds,
    });
    $("cm-result").innerHTML = '<p class="success">Manifest created!</p>';
    loadAllManifests();
    setTimeout(() => hideModal("create-manifest-modal"), 1000);
  } catch (e) {
    $("cm-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  CLIENT: MY ORDERS                                        */
/* ══════════════════════════════════════════════════════════ */
async function loadOrders() {
  try {
    const data = await api("GET", "/api/orders/");
    const orders = data.orders || [];
    renderOrderCards(orders, "orders-list", false);
  } catch (e) {
    const el = $("orders-list");
    if (el) el.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function loadClientOrders() {
  const container = $("client-orders-table");
  if (!container) return;
  container.innerHTML = '<p class="muted">Loading…</p>';
  try {
    const data = await api("GET", "/api/orders/");
    const orders = data.orders || [];
    if (!orders.length) {
      container.innerHTML =
        '<div class="no-data-message"><p>No orders yet. Create your first order!</p></div>';
      return;
    }
    container.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Order ID</th>
            <th>Recipient</th>
            <th>Status</th>
            <th>Date</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          ${orders
            .map(
              (o) => `
            <tr class="clickable-row" onclick="viewOrderDetail('${o.order_id}')">
              <td class="mono">${shortId(o.order_id)}</td>
              <td>${escapeHtml(o.recipient_name || "—")}</td>
              <td><span class="badge ${escapeHtml(o.status)}">${escapeHtml(String(o.status || "").replace(/_/g, " "))}</span></td>
              <td>${fmtDate(o.created_at)}</td>
              <td class="order-row-actions" onclick="event.stopPropagation()">
                <button class="btn-xs btn-small" onclick="trackDeliveryById('${o.order_id}')">Track</button>
                ${o.status === "pending" ? `<button class="btn-xs btn-small btn-danger" onclick="updateOrderStatus('${o.order_id}','cancelled')">Cancel</button>` : ""}
              </td>
            </tr>
          `,
            )
            .join("")}
        </tbody>
      </table>`;
  } catch (e) {
    container.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: ASSIGNED ORDERS                                  */
/* ══════════════════════════════════════════════════════════ */
async function loadDriverOrders() {
  try {
    const data = await api("GET", "/api/orders/");
    const orders = data.orders || [];
    if (!orders.length) {
      $("driver-orders-list").innerHTML =
        "<p>No orders assigned to you yet.</p>";
      return;
    }
    $("driver-orders-list").innerHTML = orders
      .map(
        (o) => `
      <div class="order-card">
        <div>
          <h3>📦 ${shortId(o.order_id)}</h3>
          <div class="meta">${o.recipient_name} – ${o.delivery_address}</div>
          <div class="meta">${o.pickup_address} → ${o.delivery_address}</div>
          <div class="meta">${fmtDate(o.created_at)}</div>
        </div>
        <div style="text-align:right">
          <span class="badge ${o.status}">${o.status.replace(/_/g, " ")}</span>
          <span class="badge ${o.priority}">${o.priority}</span>
          <div class="order-actions">
            <button class="btn-xs btn-small" onclick="trackOrderById('${o.order_id}')">Track</button>
            ${o.status === "confirmed" ? `<button class="btn-xs btn-small btn-success" onclick="updateOrderStatus('${o.order_id}','in_transit')">Start Delivery</button>` : ""}
            ${o.status === "in_transit" ? `<button class="btn-xs btn-small btn-success" onclick="updateOrderStatus('${o.order_id}','delivered')">Delivered</button>` : ""}
          </div>
        </div>
      </div>
    `,
      )
      .join("");
  } catch (e) {
    $("driver-orders-list").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  SHARED: ORDER CARD RENDERING                             */
/* ══════════════════════════════════════════════════════════ */
function renderOrderCards(orders, containerId, isAdmin) {
  const container = $(containerId);
  if (!orders.length) {
    container.innerHTML = "<p>No orders found.</p>";
    return;
  }
  container.innerHTML = orders
    .map((o) => {
      let actions = `<button class="btn-xs btn-small" onclick="viewOrderDetail('${o.order_id}')">View</button>`;
      actions += `<button class="btn-xs btn-small" onclick="trackOrderById('${o.order_id}')">Track</button>`;

      if (isAdmin) {
        if (!o.assigned_driver_id && o.status === "pending") {
          actions += `<button class="btn-xs btn-small btn-success" onclick="openAssignDriver('${o.order_id}')">Assign Driver</button>`;
        }
        if (!["delivered", "cancelled", "failed"].includes(o.status)) {
          actions += `<button class="btn-xs btn-small btn-danger" onclick="updateOrderStatus('${o.order_id}','cancelled')">Cancel</button>`;
        }
      } else if (currentUser.role === "client" && o.status === "pending") {
        actions += `<button class="btn-xs btn-small btn-danger" onclick="updateOrderStatus('${o.order_id}','cancelled')">Cancel</button>`;
      }

      return `
      <div class="order-card">
        <div>
          <h3>📦 ${shortId(o.order_id)}</h3>
          <div class="meta">${o.recipient_name || "-"} – ${o.delivery_address || "-"}</div>
          <div class="meta">${fmtDate(o.created_at)}${o.assigned_driver_id ? ` | Driver: ${o.assigned_driver_id}` : ""}</div>
        </div>
        <div style="text-align:right">
          <span class="badge ${o.status}">${o.status.replace(/_/g, " ")}</span>
          <span class="badge ${o.priority}">${o.priority}</span>
          ${o.estimated_cost ? `<div style="font-size:0.8rem;color:#666;margin-top:4px">Est: $${o.estimated_cost}</div>` : ""}
          <div class="order-actions">${actions}</div>
        </div>
      </div>`;
    })
    .join("");
}

/* ══════════════════════════════════════════════════════════ */
/*  ORDER ACTIONS                                            */
/* ══════════════════════════════════════════════════════════ */
async function viewOrderDetail(orderId) {
  try {
    const o = await api("GET", `/api/orders/${orderId}`);
    $("order-detail-content").innerHTML = `
      <table style="box-shadow:none">
        <tr><td><strong>Order ID</strong></td><td>${o.order_id}</td></tr>
        <tr><td><strong>Status</strong></td><td><span class="badge ${o.status}">${o.status}</span></td></tr>
        <tr><td><strong>Priority</strong></td><td><span class="badge ${o.priority}">${o.priority}</span></td></tr>
        <tr><td><strong>Pickup</strong></td><td>${o.pickup_address}</td></tr>
        <tr><td><strong>Delivery</strong></td><td>${o.delivery_address}</td></tr>
        <tr><td><strong>Package</strong></td><td>${o.package_description} (${o.package_weight} kg)</td></tr>
        <tr><td><strong>Recipient</strong></td><td>${o.recipient_name} (${o.recipient_phone || "-"})</td></tr>
        <tr><td><strong>Driver</strong></td><td>${o.assigned_driver_id || "Not assigned"}</td></tr>
        <tr><td><strong>Est. Cost</strong></td><td>${o.estimated_cost ? "$" + o.estimated_cost : "-"}</td></tr>
        <tr><td><strong>Notes</strong></td><td>${o.notes || "-"}</td></tr>
        <tr><td><strong>Created</strong></td><td>${fmtDate(o.created_at)}</td></tr>
        <tr><td><strong>Updated</strong></td><td>${fmtDate(o.updated_at)}</td></tr>
        <tr><td><strong>Client ID</strong></td><td>${o.client_id}</td></tr>
      </table>`;
    showModal("order-detail-modal");
  } catch (e) {
    toast(e.message || "Failed to load order details.", "error");
  }
}

async function openAssignDriver(orderId) {
  $("ad-order-id").value = orderId;
  $("ad-order-info").textContent = `Order: ${shortId(orderId)}`;
  $("ad-result").innerHTML = "";
  // Load drivers
  try {
    const data = await api("GET", "/api/auth/users?role=driver&limit=50");
    const drivers = data.users || [];
    $("ad-driver-select").innerHTML = drivers
      .map(
        (d) =>
          `<option value="${d.id}">${d.full_name || d.username} (ID: ${d.id})</option>`,
      )
      .join("");
    showModal("assign-driver-modal");
  } catch (e) {
    toast(e.message || "Failed to load drivers.", "error");
  }
}

async function assignDriver() {
  try {
    const orderId = $("ad-order-id").value;
    const driverId = parseInt($("ad-driver-select").value);
    await api("PATCH", `/api/orders/${orderId}/assign`, {
      driver_id: driverId,
    });
    $("ad-result").innerHTML = '<p class="success">Driver assigned!</p>';
    if (currentTabLoad) currentTabLoad();
    setTimeout(() => hideModal("assign-driver-modal"), 800);
  } catch (e) {
    $("ad-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function updateOrderStatus(orderId, status) {
  if (
    status === "cancelled" &&
    !confirm("Are you sure you want to cancel this order?")
  )
    return;
  try {
    await api("PATCH", `/api/orders/${orderId}/status`, { status });
    if (currentTabLoad) currentTabLoad();
  } catch (e) {
    toast(e.message || "Failed to update order status.", "error");
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  SUBMIT NEW ORDER                                         */
/* ══════════════════════════════════════════════════════════ */
function validateOrderForm() {
  const errors = [];
  const pickup = $("order-pickup").value.trim();
  const delivery = $("order-delivery").value.trim();
  const recipient = $("order-recipient").value.trim();
  const phone = $("order-phone").value.trim();
  const weight = parseFloat($("order-weight").value);

  if (pickup.length < 5)
    errors.push("Pickup address must be at least 5 characters");
  if (delivery.length < 5)
    errors.push("Delivery address must be at least 5 characters");
  if (recipient.length < 1) errors.push("Recipient name is required");
  if (phone.length < 9)
    errors.push("Phone number must be at least 9 characters");
  if (isNaN(weight) || weight <= 0)
    errors.push("Package weight must be greater than 0");

  return errors;
}

async function submitOrder() {
  // Client-side validation
  const errors = validateOrderForm();
  if (errors.length > 0) {
    $("order-result").innerHTML = `<p class="error">${errors.join("<br>")}</p>`;
    return;
  }

  try {
    const body = {
      pickup_address: $("order-pickup").value.trim(),
      delivery_address: $("order-delivery").value.trim(),
      package_description: $("order-desc").value.trim(),
      package_weight: parseFloat($("order-weight").value) || 1.0,
      recipient_name: $("order-recipient").value.trim(),
      recipient_phone: $("order-phone").value.trim(),
      priority: $("order-priority").value,
    };
    const notes = $("order-notes").value.trim();
    if (notes) body.notes = notes;
    const data = await api("POST", "/api/orders/", body);
    $("order-result").innerHTML = `
      <div class="order-success-card">
        <div class="order-success-icon">✅</div>
        <h3>Order Submitted Successfully!</h3>
        <p>Order ID: <strong class="mono">${data.order_id}</strong></p>
        <p><span class="badge pending">Pending — received by CMS</span></p>
        <div style="margin-top:12px;display:flex;gap:8px">
          <button class="btn-small btn-outline" onclick="trackDeliveryById('${data.order_id}')">Track Order</button>
          <button class="btn-small btn-outline" onclick="switchTab('orders')">View My Orders</button>
        </div>
      </div>`;
    [
      "order-pickup",
      "order-delivery",
      "order-desc",
      "order-recipient",
      "order-phone",
      "order-notes",
    ].forEach((id) => ($(id).value = ""));
    $("order-weight").value = "1.0";
  } catch (e) {
    $("order-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  TRACKING                                                 */
/* ══════════════════════════════════════════════════════════ */
function trackOrderById(orderId) {
  $("track-order-id").value = orderId;
  document
    .querySelectorAll(".tab-content")
    .forEach((el) => (el.style.display = "none"));
  document
    .querySelectorAll(".nav-tab")
    .forEach((el) => el.classList.remove("active"));
  $("tracking-tab").style.display = "block";
  trackOrder();
}

async function trackOrder() {
  const orderId = $("track-order-id").value.trim();
  if (!orderId) return;
  try {
    const data = await api("GET", `/api/tracking/${orderId}`);
    const container = $("tracking-result");
    const events = data.events || [];
    if (!events.length) {
      container.innerHTML =
        "<p>No tracking events yet. The order is being processed.</p>";
    } else {
      container.innerHTML = `<div class="timeline">${events
        .map(
          (e) => `
        <div class="timeline-item">
          <div class="time">${fmtDate(e.timestamp)}</div>
          <div class="info">
            <div class="event-type">${(e.event_type || "").replace(/_/g, " ")}</div>
            <div class="event-desc">${e.description || ""}</div>
            ${e.location ? `<div class="event-desc">📍 ${e.location}</div>` : ""}
          </div>
        </div>
      `,
        )
        .join("")}</div>`;
    }
    connectTrackingWs(orderId);
  } catch (e) {
    $("tracking-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

function connectTrackingWs(orderId) {
  if (trackingWs) trackingWs.close();
  const wsUrl = `${WS_BASE}/api/tracking/ws/${orderId}`;
  trackingWs = new WebSocket(wsUrl);
  trackingWs.onopen = () => {
    $("ws-status").className = "ws-badge connected";
    $("ws-status").textContent = "● Live Updates Connected";
  };
  trackingWs.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === "pong") return;

    // Notifications slide-over
    pushNotification({
      title: "Tracking update",
      message: `${(data.event_type || "").replace(/_/g, " ")}: ${data.description || ""}`,
      type: "info",
      timestamp: data.timestamp || new Date().toISOString(),
    });

    // Tracking tab live feed (if visible)
    const live = $("live-updates");
    if (live) {
      const div = document.createElement("div");
      div.className = "live-event";
      div.innerHTML = `<strong>${(data.event_type || "").replace(/_/g, " ")}</strong> – ${data.description || ""} <span style="color:#888;font-size:0.8rem">${data.timestamp ? new Date(data.timestamp).toLocaleTimeString() : ""}</span>`;
      live.prepend(div);
    }

    // Client dashboard live tracking card
    if (
      _trackCardOrderId &&
      data.order_id &&
      data.order_id === _trackCardOrderId
    ) {
      updateTrackCard(
        { order_id: data.order_id, status: data.event_type || "processing" },
        { status: data.event_type || "processing" },
      );
    }
  };
  trackingWs.onclose = () => {
    $("ws-status").className = "ws-badge disconnected";
    $("ws-status").textContent = "● Disconnected";
  };
  trackingWs.onerror = () => {
    $("ws-status").className = "ws-badge disconnected";
    $("ws-status").textContent = "● Connection Error";
  };
  setInterval(() => {
    if (trackingWs && trackingWs.readyState === WebSocket.OPEN)
      trackingWs.send("ping");
  }, 30000);
}

/* ══════════════════════════════════════════════════════════ */
/*  CLIENT: TRACK DELIVERY                                   */
/* ══════════════════════════════════════════════════════════ */
let _deliveryWs = null;

function trackDeliveryById(orderId) {
  // Switch to track-delivery tab
  const nav = $("nav-tabs");
  const tabs = TAB_CONFIG[currentUser.role] || TAB_CONFIG.client;
  const tdTab = tabs.find((t) => t.id === "track-delivery");
  if (tdTab) {
    const btns = nav.querySelectorAll(".nav-tab");
    const idx = tabs.indexOf(tdTab);
    switchTab("track-delivery", btns[idx], tdTab.load);
  }
  $("td-order-id").value = orderId;
  trackDelivery();
}

async function trackDelivery() {
  const orderId = $("td-order-id").value.trim();
  if (!orderId) {
    toast("Please enter an Order ID.", "error");
    return;
  }

  const resultEl = $("td-tracking-result");
  const progressWrap = $("td-progress-wrap");
  const liveFeed = $("td-live-feed");

  resultEl.innerHTML = '<p class="muted">Fetching tracking events…</p>';
  liveFeed.innerHTML = "";
  progressWrap.style.display = "none";

  try {
    const data = await api("GET", `/api/tracking/${orderId}`);
    const events = data.events || [];
    if (!events.length) {
      resultEl.innerHTML =
        "<p>No tracking events yet. The order is being processed.</p>";
    } else {
      resultEl.innerHTML = `<div class="timeline">${events
        .map(
          (e) => `
        <div class="timeline-item">
          <div class="time">${fmtDate(e.timestamp)}</div>
          <div class="info">
            <div class="event-type">${(e.event_type || "").replace(/_/g, " ")}</div>
            <div class="event-desc">${e.description || ""}</div>
            ${e.location ? `<div class="event-desc">📍 ${e.location}</div>` : ""}
          </div>
        </div>`,
        )
        .join("")}</div>`;
    }

    // Show progress bar
    const latestStatus = events.length
      ? events[events.length - 1].event_type
      : "pending";
    const pct = statusToPct(latestStatus);
    progressWrap.style.display = "block";
    $("td-progress-bar").style.width = pct + "%";
    $("td-progress-label").textContent = pct + "%";

    connectDeliveryWs(orderId);
  } catch (e) {
    resultEl.innerHTML = `<p class="error">${e.message}</p>`;
  }
}

function connectDeliveryWs(orderId) {
  if (_deliveryWs) _deliveryWs.close();
  const wsUrl = `${WS_BASE}/api/tracking/ws/${orderId}`;
  _deliveryWs = new WebSocket(wsUrl);

  const statusEl = $("td-ws-status");
  const liveFeed = $("td-live-feed");
  const progressWrap = $("td-progress-wrap");

  _deliveryWs.onopen = () => {
    statusEl.className = "ws-badge connected";
    statusEl.innerHTML = "&#x1F7E2; Connected";
  };

  _deliveryWs.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === "pong") return;

    // Live feed entry
    const div = document.createElement("div");
    div.className = "td-feed-item";
    const time = data.timestamp
      ? new Date(data.timestamp).toLocaleTimeString()
      : new Date().toLocaleTimeString();
    div.innerHTML = `<span class="td-feed-time">${time}</span> <strong>${(data.event_type || "").replace(/_/g, " ")}</strong> — ${data.description || "Status update"}`;
    liveFeed.prepend(div);

    // Update progress bar
    const pct = statusToPct(data.event_type || "");
    progressWrap.style.display = "block";
    $("td-progress-bar").style.width = pct + "%";
    $("td-progress-label").textContent = pct + "%";

    // Also update dashboard tracking card if same order
    if (_trackCardOrderId && data.order_id === _trackCardOrderId) {
      updateTrackCard(
        { order_id: data.order_id, status: data.event_type || "processing" },
        { status: data.event_type || "processing" },
      );
    }

    pushNotification({
      title: "Tracking update",
      message: `${(data.event_type || "").replace(/_/g, " ")}: ${data.description || ""}`,
      type: "info",
      timestamp: data.timestamp || new Date().toISOString(),
    });
  };

  _deliveryWs.onclose = () => {
    statusEl.className = "ws-badge disconnected";
    statusEl.innerHTML = "&#x1F534; Disconnected";
  };

  _deliveryWs.onerror = () => {
    statusEl.className = "ws-badge disconnected";
    statusEl.innerHTML = "&#x1F534; Connection Error";
  };

  setInterval(() => {
    if (_deliveryWs && _deliveryWs.readyState === WebSocket.OPEN)
      _deliveryWs.send("ping");
  }, 30000);
}

/* ══════════════════════════════════════════════════════════ */
/*  DRIVER: MANIFESTS + DELIVERY UPDATES                     */
/* ══════════════════════════════════════════════════════════ */

// Store for order details cache
let orderDetailsCache = {};
let signatureCanvas, signatureCtx;
let isDrawing = false;
let capturedPhotoData = null;

async function loadManifests() {
  try {
    const data = await api(
      "GET",
      `/api/tracking/manifests/driver/${currentUser.id}`,
    );
    const manifests = data.manifests || data || [];
    const containers = [
      $("driver-manifests"),
      $("driver-manifests-status"),
    ].filter(Boolean);
    if (!containers.length) return;
    const setHtml = (html) => containers.forEach((c) => (c.innerHTML = html));

    // Calculate summary stats
    let total = 0,
      completed = 0,
      pending = 0,
      failed = 0;
    let hasUrgent = false;
    let routeData = null;

    manifests.forEach((m) => {
      if (m.route_data) routeData = m.route_data;
      (m.items || []).forEach((item) => {
        total++;
        if (item.status === "delivered") completed++;
        else if (item.status === "failed") failed++;
        else pending++;
      });
    });

    // Update summary stats
    if ($("ds-total")) $("ds-total").textContent = total;
    if ($("ds-completed")) $("ds-completed").textContent = completed;
    if ($("ds-pending")) $("ds-pending").textContent = pending;
    if ($("ds-failed")) $("ds-failed").textContent = failed;

    // Display route info if available
    displayRouteInfo(routeData);

    // Check for urgent/high priority deliveries
    checkDriverNotifications(manifests);

    if (!manifests.length) {
      setHtml(
        '<p class="empty-state">No manifests assigned for today. Check with your admin.</p>',
      );
      return;
    }

    // Fetch order details for all items
    const orderIds = manifests.flatMap((m) =>
      (m.items || []).map((i) => i.order_id),
    );
    await fetchOrderDetails(orderIds);

    const html = manifests
      .map(
        (m) => `
      <div class="manifest-card">
        <h3>📋 ${shortId(m.manifest_id)} <span class="badge ${m.status}">${m.status}</span></h3>
        <p style="color:var(--text-secondary);font-size:0.85rem">Date: ${m.date} | ${(m.items || []).length} deliveries</p>
        ${(m.items || [])
          .map((item) => {
            const order = orderDetailsCache[item.order_id] || {};
            return `
          <div class="manifest-item">
            <div class="manifest-item-details">
              <div class="manifest-item-customer">${order.recipient_name || "Customer"}</div>
              <div class="manifest-item-address">${order.delivery_address || item.order_id}</div>
            </div>
            <span class="badge ${item.status}">${item.status.replace(/_/g, " ")}</span>
            ${order.priority === "urgent" ? '<span class="badge urgent">URGENT</span>' : ""}
            ${order.priority === "high" ? '<span class="badge high">HIGH</span>' : ""}
            ${item.proof_of_delivery ? '<span style="color:var(--success);font-size:0.8rem">✓ POD</span>' : ""}
            ${
              item.status === "pending" ||
              item.status === "in_transit" ||
              item.status === "picked_up"
                ? `
              <button class="btn-xs btn-small btn-success" onclick="openDeliveryUpdate('${item.order_id}')">Update</button>
            `
                : ""
            }
          </div>`;
          })
          .join("")}
      </div>
    `,
      )
      .join("");
    setHtml(html);
  } catch (e) {
    [$("driver-manifests"), $("driver-manifests-status")]
      .filter(Boolean)
      .forEach((c) => (c.innerHTML = `<p class="error">${e.message}</p>`));
    toast(e.message || "Failed to load manifests.", "error");
  }
}

async function fetchOrderDetails(orderIds) {
  // Fetch details for orders not in cache
  const toFetch = orderIds.filter((id) => !orderDetailsCache[id]);
  for (const orderId of toFetch) {
    try {
      const order = await api("GET", `/api/orders/${orderId}`);
      orderDetailsCache[orderId] = order;
    } catch (e) {
      // Order might not exist or unauthorized
      orderDetailsCache[orderId] = { order_id: orderId };
    }
  }
}

function displayRouteInfo(routeData) {
  const card = $("route-info-card");
  const details = $("route-details");
  if (!card || !details) return;

  if (!routeData) {
    card.style.display = "none";
    return;
  }

  try {
    const route =
      typeof routeData === "string" ? JSON.parse(routeData) : routeData;
    const stops = route.stops || route.waypoints || [];

    if (!stops.length) {
      card.style.display = "none";
      return;
    }

    card.style.display = "block";
    details.innerHTML = stops
      .map(
        (stop, idx) => `
      <div class="route-stop">
        <span class="route-stop-number">${idx + 1}</span>
        <div class="route-stop-info">
          <div class="route-stop-address">${stop.address || stop.location || "Stop " + (idx + 1)}</div>
          <div class="route-stop-meta">${stop.eta || ""} ${stop.distance ? "• " + stop.distance : ""}</div>
        </div>
      </div>
    `,
      )
      .join("");
  } catch (e) {
    card.style.display = "none";
  }
}

function checkDriverNotifications(manifests) {
  const notifDiv = $("driver-notifications");
  const notifText = $("driver-notification-text");
  if (!notifDiv || !notifText) return;

  // Check for urgent/high priority pending deliveries
  let urgentCount = 0;
  manifests.forEach((m) => {
    (m.items || []).forEach((item) => {
      const order = orderDetailsCache[item.order_id];
      if (
        order &&
        (order.priority === "urgent" || order.priority === "high") &&
        (item.status === "pending" || item.status === "in_transit")
      ) {
        urgentCount++;
      }
    });
  });

  if (urgentCount > 0) {
    notifDiv.style.display = "block";
    notifText.textContent = `You have ${urgentCount} urgent/high priority ${urgentCount === 1 ? "delivery" : "deliveries"} pending!`;
  } else {
    notifDiv.style.display = "none";
  }
}

function dismissDriverNotification() {
  const notifDiv = $("driver-notifications");
  if (notifDiv) notifDiv.style.display = "none";
}

async function openDeliveryUpdate(orderId) {
  $("du-order-id").value = orderId;
  $("du-status").value = "delivered";
  $("du-notes").value = "";
  $("du-result").innerHTML = "";
  capturedPhotoData = null;

  // Reset failure section
  toggleFailureReason();

  // Clear photo preview
  const photoPreview = $("photo-preview");
  if (photoPreview)
    photoPreview.innerHTML =
      '<span class="photo-placeholder">No photo captured</span>';

  // Initialize signature canvas
  initSignatureCanvas();

  // Load delivery details
  await loadDeliveryDetails(orderId);

  showModal("delivery-update-modal");
}

async function loadDeliveryDetails(orderId) {
  const infoGrid = $("du-info-grid");
  if (!infoGrid) return;

  try {
    let order = orderDetailsCache[orderId];
    if (!order || !order.recipient_name) {
      order = await api("GET", `/api/orders/${orderId}`);
      orderDetailsCache[orderId] = order;
    }

    infoGrid.innerHTML = `
      <div class="delivery-info-item">
        <div class="delivery-info-label">Customer Name</div>
        <div class="delivery-info-value">${order.recipient_name || "N/A"}</div>
      </div>
      <div class="delivery-info-item">
        <div class="delivery-info-label">Phone</div>
        <div class="delivery-info-value"><a href="tel:${order.recipient_phone}">${order.recipient_phone || "N/A"}</a></div>
      </div>
      <div class="delivery-info-item">
        <div class="delivery-info-label">Delivery Address</div>
        <div class="delivery-info-value">${order.delivery_address || "N/A"}</div>
      </div>
      <div class="delivery-info-item">
        <div class="delivery-info-label">Package</div>
        <div class="delivery-info-value">${order.package_description || "N/A"} (${order.package_weight || 0} kg)</div>
      </div>
      <div class="delivery-info-item">
        <div class="delivery-info-label">Priority</div>
        <div class="delivery-info-value"><span class="badge ${order.priority}">${order.priority || "normal"}</span></div>
      </div>
      <div class="delivery-info-item">
        <div class="delivery-info-label">Notes</div>
        <div class="delivery-info-value">${order.notes || "None"}</div>
      </div>
    `;
  } catch (e) {
    infoGrid.innerHTML = '<p class="error">Could not load delivery details</p>';
  }
}

function toggleFailureReason() {
  const status = $("du-status").value;
  const failureSection = $("du-failure-section");
  const podSection = $("du-pod-section");

  if (failureSection) {
    failureSection.style.display = status === "failed" ? "block" : "none";
  }
  if (podSection) {
    podSection.style.display = status === "delivered" ? "block" : "none";
  }
}

// Signature Canvas Functions
function initSignatureCanvas() {
  signatureCanvas = $("signature-canvas");
  if (!signatureCanvas) return;

  signatureCtx = signatureCanvas.getContext("2d");
  clearSignature();

  // Set up event listeners
  signatureCanvas.addEventListener("mousedown", startDrawing);
  signatureCanvas.addEventListener("mousemove", draw);
  signatureCanvas.addEventListener("mouseup", stopDrawing);
  signatureCanvas.addEventListener("mouseout", stopDrawing);

  // Touch events for mobile
  signatureCanvas.addEventListener("touchstart", handleTouchStart);
  signatureCanvas.addEventListener("touchmove", handleTouchMove);
  signatureCanvas.addEventListener("touchend", stopDrawing);
}

function startDrawing(e) {
  isDrawing = true;
  signatureCtx.beginPath();
  signatureCtx.moveTo(e.offsetX, e.offsetY);
}

function draw(e) {
  if (!isDrawing) return;
  signatureCtx.lineTo(e.offsetX, e.offsetY);
  signatureCtx.strokeStyle = "#000";
  signatureCtx.lineWidth = 2;
  signatureCtx.lineCap = "round";
  signatureCtx.stroke();
}

function stopDrawing() {
  isDrawing = false;
}

function handleTouchStart(e) {
  e.preventDefault();
  const touch = e.touches[0];
  const rect = signatureCanvas.getBoundingClientRect();
  const x = touch.clientX - rect.left;
  const y = touch.clientY - rect.top;
  isDrawing = true;
  signatureCtx.beginPath();
  signatureCtx.moveTo(x, y);
}

function handleTouchMove(e) {
  if (!isDrawing) return;
  e.preventDefault();
  const touch = e.touches[0];
  const rect = signatureCanvas.getBoundingClientRect();
  const x = touch.clientX - rect.left;
  const y = touch.clientY - rect.top;
  signatureCtx.lineTo(x, y);
  signatureCtx.strokeStyle = "#000";
  signatureCtx.lineWidth = 2;
  signatureCtx.lineCap = "round";
  signatureCtx.stroke();
}

function clearSignature() {
  if (!signatureCtx) return;
  signatureCtx.fillStyle = "#fff";
  signatureCtx.fillRect(0, 0, signatureCanvas.width, signatureCanvas.height);
}

function getSignatureData() {
  if (!signatureCanvas) return null;
  // Check if signature is empty (all white)
  const imageData = signatureCtx.getImageData(
    0,
    0,
    signatureCanvas.width,
    signatureCanvas.height,
  );
  const isEmpty = imageData.data.every((v, i) => i % 4 === 3 || v >= 250);
  if (isEmpty) return null;
  return signatureCanvas.toDataURL("image/png");
}

// Photo Capture Functions
function handlePhotoSelect(event) {
  const file = event.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = (e) => {
    capturedPhotoData = e.target.result;
    const preview = $("photo-preview");
    if (preview) {
      preview.innerHTML = `<img src="${capturedPhotoData}" alt="Proof of delivery">`;
    }
  };
  reader.readAsDataURL(file);
}

function clearPhoto() {
  capturedPhotoData = null;
  const preview = $("photo-preview");
  if (preview) {
    preview.innerHTML =
      '<span class="photo-placeholder">No photo captured</span>';
  }
  const input = $("photo-input");
  if (input) input.value = "";
}

async function submitDeliveryUpdate() {
  try {
    const orderId = $("du-order-id").value;
    const status = $("du-status").value;

    const body = { status };

    // Add signature if delivered
    if (status === "delivered") {
      const sigData = getSignatureData();
      if (sigData) body.signature_data = sigData;
      if (capturedPhotoData) body.proof_of_delivery = capturedPhotoData;
    }

    // Add failure reason if failed
    if (status === "failed") {
      const reason = $("du-failure-reason").value;
      if (!reason) {
        $("du-result").innerHTML =
          '<p class="error">Please select a failure reason</p>';
        return;
      }
      body.failure_reason =
        reason === "Other" ? $("du-failure-other").value : reason;
    }

    if ($("du-notes").value) body.notes = $("du-notes").value;

    await api("PATCH", `/api/tracking/delivery-items/${orderId}`, body);
    $("du-result").innerHTML =
      '<p class="success">Delivery updated successfully!</p>';

    // Clear cache and refresh
    delete orderDetailsCache[orderId];
    loadManifests();
    setTimeout(() => hideModal("delivery-update-modal"), 1000);
  } catch (e) {
    $("du-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

// Show/hide other failure reason input
document.addEventListener("DOMContentLoaded", () => {
  const failureReason = $("du-failure-reason");
  if (failureReason) {
    failureReason.addEventListener("change", (e) => {
      const otherInput = $("du-failure-other");
      if (otherInput) {
        otherInput.style.display =
          e.target.value === "Other" ? "block" : "none";
      }
    });
  }
});

/* ══════════════════════════════════════════════════════════ */
/*  PROFILE                                                  */
/* ══════════════════════════════════════════════════════════ */
async function loadProfile() {
  try {
    const u = await api("GET", "/api/auth/me");
    $("profile-fullname").value = u.full_name || "";
    $("profile-email").value = u.email || "";
    $("profile-phone").value = u.phone || "";
    $("profile-username").value = u.username || "";
    $("profile-role").value = u.role || "";
  } catch (e) {
    $("profile-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function updateProfile() {
  try {
    const body = {};
    if ($("profile-fullname").value)
      body.full_name = $("profile-fullname").value;
    if ($("profile-email").value) body.email = $("profile-email").value;
    if ($("profile-phone").value) body.phone = $("profile-phone").value;
    const u = await api("PUT", "/api/auth/me", body);
    currentUser = { ...currentUser, ...u };
    _authStorage = getActiveAuthStorage();
    _authStorage.setItem(AUTH_KEYS.user, JSON.stringify(currentUser));
    $("user-info").textContent = currentUser.full_name || currentUser.username;
    const roleBadge = $("user-role-badge");
    if (roleBadge) roleBadge.textContent = currentUser.role;
    $("profile-result").innerHTML = '<p class="success">Profile updated!</p>';
  } catch (e) {
    $("profile-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

async function changePassword() {
  const newPw = $("pw-new").value;
  const confirm = $("pw-confirm").value;
  if (newPw !== confirm) {
    $("password-result").innerHTML =
      '<p class="error">Passwords do not match</p>';
    return;
  }
  try {
    await api("PUT", "/api/auth/me/password", {
      current_password: $("pw-current").value,
      new_password: newPw,
    });
    $("password-result").innerHTML = '<p class="success">Password changed!</p>';
    $("pw-current").value = "";
    $("pw-new").value = "";
    $("pw-confirm").value = "";
  } catch (e) {
    $("password-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

/* ══════════════════════════════════════════════════════════ */
/*  POPULATE DRIVER DROPDOWNS (for admin modals)             */
/* ══════════════════════════════════════════════════════════ */
async function loadDriverOptions() {
  if (currentUser.role !== "admin") return;
  try {
    const data = await api("GET", "/api/auth/users?role=driver&limit=50");
    const drivers = data.users || [];
    const opts = drivers
      .map(
        (d) =>
          `<option value="${d.id}">${d.full_name || d.username} (ID: ${d.id})</option>`,
      )
      .join("");
    if ($("cm-driver-select")) $("cm-driver-select").innerHTML = opts;
  } catch (e) {}
}

/* ══════════════════════════════════════════════════════════ */
/*  INIT                                                     */
/* ══════════════════════════════════════════════════════════ */
window.addEventListener("DOMContentLoaded", () => {
  // Set default manifest date to today
  const dateEl = $("cm-date");
  if (dateEl) dateEl.value = new Date().toISOString().split("T")[0];

  if (token && currentUser) {
    showDashboard();
    loadProfile();
    loadDriverOptions();
  }
});

// Also load profile when tab is switched to profile
const origSwitchTab = switchTab;
switchTab = function (tabId, btnEl, loadFn) {
  origSwitchTab(tabId, btnEl, loadFn);
  if (tabId === "profile") loadProfile();
  if (currentUser && currentUser.role === "admin") loadDriverOptions();
};
