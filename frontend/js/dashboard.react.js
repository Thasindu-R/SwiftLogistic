(function () {
  "use strict";
  const e = React.createElement;

  /* ICON PRIMITIVES */

  function Svg({ size = 16, vb = "0 0 24 24", children, className }) {
    return e(
      "svg",
      {
        width: size,
        height: size,
        viewBox: vb,
        fill: "none",
        stroke: "currentColor",
        strokeWidth: 1.75,
        strokeLinecap: "round",
        strokeLinejoin: "round",
        style: { flexShrink: 0, display: "block" },
        className,
      },
      ...children,
    );
  }

  const IRefresh = ({ size = 14 }) =>
    e(
      Svg,
      { size },
      e("polyline", { points: "23 4 23 10 17 10" }),
      e("path", { d: "M20.49 15a9 9 0 11-2.12-9.36L23 10" }),
    );

  const IPlus = ({ size = 13 }) =>
    e(
      Svg,
      { size },
      e("line", { x1: "12", y1: "5", x2: "12", y2: "19" }),
      e("line", { x1: "5", y1: "12", x2: "19", y2: "12" }),
    );

  const IBell = ({ size = 18 }) =>
    e(
      Svg,
      { size },
      e("path", { d: "M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" }),
      e("path", { d: "M13.73 21a2 2 0 0 1-3.46 0" }),
    );

  const ICompass = ({ size = 16 }) =>
    e(
      Svg,
      { size },
      e("circle", { cx: "12", cy: "12", r: "10" }),
      e("polygon", {
        points: "16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88 16.24 7.76",
      }),
    );

  /* ═══════════════════════════════════════════════
     SHARED COMPONENTS
  ═══════════════════════════════════════════════ */

  /* Pill button in card header */
  function PillBtn({ onClick, label = "Refresh", icon }) {
    return e(
      "button",
      {
        className: "btn-xs btn-small btn-outline",
        onClick,
        style: { display: "inline-flex", alignItems: "center", gap: 5 },
      },
      icon || e(IRefresh),
      label,
    );
  }

  /* Dashboard hero banner */
  function Hero({ title, subtitle, actions }) {
    return e(
      "div",
      { className: "rd-hero" },
      e(
        "div",
        null,
        e("h2", { className: "rd-title" }, title),
        subtitle && e("p", { className: "rd-subtitle" }, subtitle),
      ),
      actions && e("div", { className: "rd-actions" }, actions),
    );
  }

  /* Card wrapper */
  function Panel({ title, badge, action, children, noPad }) {
    return e(
      "section",
      { className: "rd-card" },
      e(
        "div",
        { className: "rd-card-header" },
        e(
          "div",
          { className: "rd-card-title" },
          e("h3", null, title),
          badge || null,
        ),
        action || null,
      ),
      e(
        "div",
        {
          className: "rd-card-body",
          style: noPad ? { padding: 0 } : undefined,
        },
        children,
      ),
    );
  }

  /* Inline badge chip */
  function Chip({
    text,
    color = "var(--accent)",
    bg = "var(--accent-dim)",
    border = "var(--border)",
  }) {
    return e(
      "span",
      {
        style: {
          fontSize: "0.68rem",
          fontWeight: 800,
          color,
          background: bg,
          border: `1px solid ${border}`,
          padding: "2px 8px",
          borderRadius: 999,
          textTransform: "uppercase",
          letterSpacing: "0.06em",
          whiteSpace: "nowrap",
        },
      },
      text,
    );
  }

  /* ═══════════════════════════════════════════════
     ADMIN DASHBOARD
  ═══════════════════════════════════════════════ */

  function AdminDashboard() {
    const refresh = () => window.loadAdminDashboard?.();

    return e(
      "div",
      { className: "rd-shell" },

      /* Greeting banner with clock */
      e(
        "div",
        { className: "rd-admin-hero" },
        e(
          "div",
          { className: "dash-greeting", id: "dash-greeting-banner" },
          e(
            "div",
            { className: "dash-greeting-left" },
            e("h2", { id: "dash-greeting-msg" }, "Welcome back!"),
            e(
              "p",
              { id: "dash-greeting-sub" },
              "Here's what's happening with your logistics today.",
            ),
          ),
          e(
            "div",
            { className: "dash-greeting-right" },
            e("div", { className: "dash-greeting-time", id: "dash-clock" }),
            e("div", { className: "dash-greeting-date", id: "dash-date" }),
          ),
        ),
      ),

      e(
        "div",
        { className: "rd-grid" },

        /* 4 Stat Cards */
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            { title: "Order Overview", action: e(Chip, { text: "Live" }) },
            e("div", { id: "stats-cards", className: "stats-grid" }),
          ),
        ),

        /* System Health — 3 dots */
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            {
              title: "System Health",
              action: e(PillBtn, { onClick: refresh }),
            },
            e("div", {
              id: "admin-health-dots",
              className: "health-dots-row",
            }),
          ),
        ),

        /* Recent Activity Feed */
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            {
              title: "Recent Activity",
              action: e(PillBtn, { onClick: refresh }),
            },
            e("div", {
              id: "admin-activity-feed",
              className: "activity-feed",
            }),
          ),
        ),
      ),
    );
  }

  /* ═══════════════════════════════════════════════
     CLIENT DASHBOARD
  ═══════════════════════════════════════════════ */

  function ClientDashboard() {
    return e(
      "div",
      { className: "rd-shell" },

      e(Hero, {
        title: "My Orders",
        subtitle: "Track progress, view details, and submit new deliveries.",
        actions: e(
          "div",
          { style: { display: "flex", gap: 8 } },
          e(
            "button",
            {
              className: "btn-small btn-primary",
              style: {
                width: "auto",
                marginTop: 0,
                padding: "7px 16px",
                fontSize: "0.82rem",
              },
              onClick: () => window.switchTab?.("new-order"),
            },
            e(IPlus),
            " New Order",
          ),
          e(PillBtn, { onClick: () => window.loadOrders?.() }),
        ),
      }),

      e(
        "div",
        { className: "rd-grid" },
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            { title: "Orders" },
            e("div", { id: "orders-list", className: "orders-grid" }),
          ),
        ),
      ),
    );
  }

  /* ═══════════════════════════════════════════════
     DRIVER DASHBOARD
  ═══════════════════════════════════════════════ */

  function DriverDashboard() {
    return e(
      "div",
      { className: "rd-shell" },

      e(Hero, {
        title: "Driver Dashboard",
        subtitle:
          "Today's summary — deliveries, routes, and status at a glance.",
        actions: e(PillBtn, { onClick: () => window.loadDriverDashboard?.() }),
      }),

      /* Urgent push notification banner */
      e(
        "div",
        {
          id: "driver-notifications",
          className: "driver-notifications",
          style: { display: "none" },
        },
        e(
          "div",
          { className: "notification-banner urgent" },
          e(IBell, { size: 17 }),
          e("span", { id: "driver-notification-text" }),
          e(
            "button",
            {
              className: "btn-dismiss",
              onClick: () => window.dismissDriverNotification?.(),
            },
            "×",
          ),
        ),
      ),

      e(
        "div",
        { className: "rd-grid" },

        /* Daily summary stats — 4 cards */
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            { title: "Today's Summary" },
            e(
              "div",
              { className: "driver-summary", id: "driver-summary" },
              e(
                "div",
                { className: "driver-stat" },
                e(
                  "span",
                  { className: "driver-stat-value", id: "ds-total" },
                  "0",
                ),
                e("span", { className: "driver-stat-label" }, "Total"),
              ),
              e(
                "div",
                { className: "driver-stat completed" },
                e(
                  "span",
                  { className: "driver-stat-value", id: "ds-completed" },
                  "0",
                ),
                e("span", { className: "driver-stat-label" }, "Completed"),
              ),
              e(
                "div",
                { className: "driver-stat pending" },
                e(
                  "span",
                  { className: "driver-stat-value", id: "ds-pending" },
                  "0",
                ),
                e("span", { className: "driver-stat-label" }, "Pending"),
              ),
              e(
                "div",
                { className: "driver-stat failed" },
                e(
                  "span",
                  { className: "driver-stat-value", id: "ds-failed" },
                  "0",
                ),
                e("span", { className: "driver-stat-label" }, "Failed"),
              ),
            ),
          ),
        ),

        /* Today's manifest preview (first 3 stops) */
        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            Panel,
            {
              title: "Today's Manifest Preview",
              action: e(PillBtn, {
                onClick: () => window.switchTab?.("my-manifest"),
                label: "View All",
              }),
            },
            e("div", { id: "driver-manifest-preview" }),
          ),
        ),
      ),
    );
  }

  /* ═══════════════════════════════════════════════
     MOUNT
  ═══════════════════════════════════════════════ */

  function safeMount(rootId, element) {
    const root = document.getElementById(rootId);
    if (!root || root.__reactMounted) return;
    root.__reactMounted = true;
    ReactDOM.createRoot(root).render(element);
  }

  window.mountDashboards = function () {
    safeMount("admin-dashboard-root", e(AdminDashboard));
    safeMount("client-dashboard-root", e(ClientDashboard));
    safeMount("driver-dashboard-root", e(DriverDashboard));
  };
})();
