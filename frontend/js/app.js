/**
 * SwiftTrack Frontend – Unified Portal (Admin / Client / Driver)
 * Communicates with the API Gateway (port 8000).
 */

const API_BASE =
  window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.protocol}//${window.location.hostname}:8000`;
const WS_BASE = API_BASE.replace("http", "ws");

let token = localStorage.getItem("swifttrack_token") || null;
let currentUser = JSON.parse(localStorage.getItem("swifttrack_user") || "null");
let trackingWs = null;
let _debounceTimers = {};

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
  if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
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
  $("login-form").style.display = "block";
  $("register-form").style.display = "none";
  $("auth-error").textContent = "";
}
function showRegister() {
  $("login-form").style.display = "none";
  $("register-form").style.display = "block";
  $("auth-error").textContent = "";
}

async function login() {
  try {
    const data = await api("POST", "/api/auth/login", {
      username: $("login-username").value,
      password: $("login-password").value,
    });
    setAuthState(data.access_token, data.user);
  } catch (e) {
    $("auth-error").textContent = e.message;
  }
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

function setAuthState(newToken, user) {
  token = newToken;
  currentUser = user;
  localStorage.setItem("swifttrack_token", token);
  localStorage.setItem("swifttrack_user", JSON.stringify(user));
  showDashboard();
}

function logout() {
  token = null;
  currentUser = null;
  localStorage.removeItem("swifttrack_token");
  localStorage.removeItem("swifttrack_user");
  $("auth-section").style.display = "flex";
  $("dashboard-section").style.display = "none";
  if (trackingWs) {
    trackingWs.close();
    trackingWs = null;
  }
}

function toggleSidebar() {
  const sidebar = $("sidebar");
  const overlay = $("sidebar-overlay");
  sidebar.classList.toggle("open");
  overlay.classList.toggle("open");
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
  buildTabs();
}

/* SVG icon fragments for sidebar nav */
const NAV_ICONS = {
  "admin-dashboard":
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>',
  users:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>',
  "all-orders":
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/></svg>',
  orders:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/></svg>',
  "new-order":
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>',
  manifests:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
  integration:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
  tracking:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
  "driver-orders":
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
  driver:
    '<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
};

const TAB_CONFIG = {
  admin: [
    { id: "admin-dashboard", label: "Dashboard", load: loadAdminDashboard },
    { id: "users", label: "Users", load: loadUsers },
    { id: "all-orders", label: "All Orders", load: loadAllOrders },
    { id: "new-order", label: "New Order", load: null },
    { id: "manifests", label: "Manifests", load: loadAllManifests },
    { id: "integration", label: "Integrations", load: loadIntegrationEvents },
    { id: "tracking", label: "Track", load: null },
  ],
  client: [
    { id: "orders", label: "My Orders", load: loadOrders },
    { id: "new-order", label: "New Order", load: null },
    { id: "tracking", label: "Track Order", load: null },
  ],
  driver: [
    { id: "driver-orders", label: "My Orders", load: loadDriverOrders },
    { id: "driver", label: "Manifests", load: loadManifests },
    { id: "tracking", label: "Track", load: null },
  ],
};

let currentTabLoad = null;

function buildTabs() {
  const nav = $("nav-tabs");
  nav.innerHTML = "";
  const tabs = TAB_CONFIG[currentUser.role] || TAB_CONFIG.client;
  tabs.forEach((t, i) => {
    const btn = document.createElement("button");
    btn.className = "tab" + (i === 0 ? " active" : "");
    const icon = NAV_ICONS[t.id] || "";
    btn.innerHTML = icon + `<span>${t.label}</span>`;
    btn.onclick = () => switchTab(t.id, btn, t.load);
    nav.appendChild(btn);
  });
  // Activate first tab
  const first = tabs[0];
  switchTab(first.id, nav.querySelector(".tab"), first.load);
}

function switchTab(tabId, btnEl, loadFn) {
  document
    .querySelectorAll(".tab-content")
    .forEach((el) => (el.style.display = "none"));
  document
    .querySelectorAll(".tab")
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
        n: stats.pending,
        l: "Pending",
        cls: "stat-amber",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
      },
      {
        n: stats.confirmed,
        l: "Confirmed",
        cls: "stat-sky",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>',
      },
      {
        n: stats.processing,
        l: "Processing",
        cls: "stat-violet",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg>',
      },
      {
        n: stats.in_transit,
        l: "In Transit",
        cls: "stat-teal",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
      },
      {
        n: stats.delivered,
        l: "Delivered",
        cls: "stat-green",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
      },
      {
        n: stats.failed,
        l: "Failed",
        cls: "stat-red",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
      },
      {
        n: stats.cancelled,
        l: "Cancelled",
        cls: "stat-slate",
        icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>',
      },
    ]
      .map(
        (s) =>
          `<div class="stat-card ${s.cls}">
            <div class="stat-card-header">
              <div class="stat-icon">${s.icon}</div>
            </div>
            <div class="stat-num">${s.n ?? 0}</div>
            <div class="stat-label">${s.l}</div>
          </div>`,
      )
      .join("");
  } catch (e) {
    $("stats-cards").innerHTML = `<p class="error">${e.message}</p>`;
  }

  // Recent orders
  try {
    const data = await api("GET", "/api/orders/?limit=5");
    const orders = data.orders || [];
    $("admin-recent-orders").innerHTML = orders.length
      ? orders
          .map(
            (o) =>
              `<div class="mini-list-item">
        <strong>${shortId(o.order_id)}</strong> ${o.recipient_name}
        <span class="badge ${o.status}">${o.status}</span>
        <span style="float:right;color:#999;font-size:0.8rem">${fmtDate(o.created_at)}</span>
      </div>`,
          )
          .join("")
      : "<p style='color:#999'>No orders yet</p>";
  } catch (e) {
    $("admin-recent-orders").innerHTML = `<p class="error">${e.message}</p>`;
  }

  // Recent tracking events
  try {
    const events = await api("GET", "/api/tracking/events/recent?limit=5");
    const evts = events.events || events || [];
    $("admin-recent-events").innerHTML = evts.length
      ? evts
          .map(
            (ev) =>
              `<div class="mini-list-item">
        <strong>${(ev.event_type || "").replace(/_/g, " ")}</strong>
        <span style="color:#666"> – ${shortId(ev.order_id)}</span>
        <span style="float:right;color:#999;font-size:0.8rem">${fmtDate(ev.timestamp)}</span>
      </div>`,
          )
          .join("")
      : "<p style='color:#999'>No events yet</p>";
  } catch (e) {
    $("admin-recent-events").innerHTML = `<p class="error">${e.message}</p>`;
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
    alert(e.message);
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
    alert(e.message);
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
    const priority = $("admin-order-priority-filter")
      ? $("admin-order-priority-filter").value
      : "";
    let qs = `?skip=${allOrdersPage * 20}&limit=20`;
    if (status) qs += `&status=${status}`;
    if (priority) qs += `&priority=${priority}`;
    const data = await api("GET", `/api/orders/${qs}`);
    const orders = data.orders || [];
    renderOrderCards(orders, "all-orders-list", true);
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
/*  ADMIN: INTEGRATION EVENTS                                */
/* ══════════════════════════════════════════════════════════ */
async function loadIntegrationEvents() {
  try {
    const status = $("int-status-filter") ? $("int-status-filter").value : "";
    const source = $("int-source-filter") ? $("int-source-filter").value : "";
    let qs = "?limit=50";
    if (status) qs += `&status=${status}`;
    if (source) qs += `&source=${source}`;
    const data = await api("GET", `/api/tracking/integration-events${qs}`);
    const events = data.events || [];
    if (!events.length) {
      $("integration-events-list").innerHTML =
        "<p>No integration events found.</p>";
      return;
    }
    $("integration-events-list").innerHTML = `
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
    $("integration-events-list").innerHTML =
      `<p class="error">${e.message}</p>`;
  }
}

async function retryIntegration(eventId) {
  try {
    await api("POST", `/api/tracking/integration-events/${eventId}/retry`);
    loadIntegrationEvents();
  } catch (e) {
    alert(e.message);
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
    $("orders-list").innerHTML = `<p class="error">${e.message}</p>`;
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
    alert(e.message);
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
    alert(e.message);
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
    alert(e.message);
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
    $("order-result").innerHTML =
      `<p class="success">Order created! ID: <strong>${data.order_id}</strong></p>`;
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
    .querySelectorAll(".tab")
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
    const div = document.createElement("div");
    div.className = "live-event";
    div.innerHTML = `<strong>${(data.event_type || "").replace(/_/g, " ")}</strong> – ${data.description || ""} <span style="color:#888;font-size:0.8rem">${data.timestamp ? new Date(data.timestamp).toLocaleTimeString() : ""}</span>`;
    $("live-updates").prepend(div);
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
/*  DRIVER: MANIFESTS + DELIVERY UPDATES                     */
/* ══════════════════════════════════════════════════════════ */
async function loadManifests() {
  try {
    const data = await api(
      "GET",
      `/api/tracking/manifests/driver/${currentUser.id}`,
    );
    const manifests = data.manifests || data || [];
    const container = $("driver-manifests");
    if (!manifests.length) {
      container.innerHTML =
        "<p>No manifests assigned. Check with your admin.</p>";
      return;
    }
    container.innerHTML = manifests
      .map(
        (m) => `
      <div class="manifest-card">
        <h3>📋 ${shortId(m.manifest_id)} <span class="badge ${m.status}">${m.status}</span></h3>
        <p style="color:#666;font-size:0.85rem">Date: ${m.date} | Items: ${(m.items || []).length}</p>
        ${(m.items || [])
          .map(
            (item) => `
          <div class="manifest-item">
            <span>📦 ${shortId(item.order_id)} (#${item.sequence})</span>
            <span class="badge ${item.status}">${item.status}</span>
            ${item.proof_of_delivery ? `<span style="color:#27ae60;font-size:0.8rem">✓ POD</span>` : ""}
            ${
              item.status === "pending" || item.status === "in_transit"
                ? `
              <button class="btn-xs btn-small btn-success" onclick="openDeliveryUpdate('${item.order_id}')">Update</button>
            `
                : ""
            }
          </div>
        `,
          )
          .join("")}
      </div>
    `,
      )
      .join("");
  } catch (e) {
    $("driver-manifests").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

function openDeliveryUpdate(orderId) {
  $("du-order-id").value = orderId;
  $("du-proof").value = "";
  $("du-signature").value = "";
  $("du-notes").value = "";
  $("du-failure").value = "";
  $("du-status").value = "delivered";
  $("du-result").innerHTML = "";
  showModal("delivery-update-modal");
}

async function submitDeliveryUpdate() {
  try {
    const orderId = $("du-order-id").value;
    const body = { status: $("du-status").value };
    if ($("du-proof").value) body.proof_of_delivery = $("du-proof").value;
    if ($("du-signature").value) body.signature = $("du-signature").value;
    if ($("du-notes").value) body.notes = $("du-notes").value;
    if ($("du-failure").value) body.failure_reason = $("du-failure").value;
    await api("PATCH", `/api/tracking/delivery-items/${orderId}`, body);
    $("du-result").innerHTML = '<p class="success">Updated!</p>';
    loadManifests();
    setTimeout(() => hideModal("delivery-update-modal"), 800);
  } catch (e) {
    $("du-result").innerHTML = `<p class="error">${e.message}</p>`;
  }
}

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
    localStorage.setItem("swifttrack_user", JSON.stringify(currentUser));
    $("user-info").textContent =
      `${currentUser.full_name} (${currentUser.role})`;
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
