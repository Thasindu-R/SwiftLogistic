/*
 * Embedded React dashboards (Admin / Client / Driver)
 * - Uses React UMD globals (React, ReactDOM)
 * - Preserves existing element IDs that app.js populates.
 */

(function () {
  const e = React.createElement;

  function IconRefresh(props) {
    return e(
      "svg",
      {
        width: props.size || 16,
        height: props.size || 16,
        viewBox: "0 0 24 24",
        fill: "none",
        stroke: "currentColor",
        strokeWidth: 2,
      },
      e("polyline", { points: "23 4 23 10 17 10" }),
      e("path", { d: "M20.49 15a9 9 0 11-2.12-9.36L23 10" }),
    );
  }

  function DashHero(props) {
    return e(
      "div",
      { className: "rd-hero" },
      e(
        "div",
        { className: "rd-hero-left" },
        e("h2", { className: "rd-title" }, props.title),
        e("p", { className: "rd-subtitle" }, props.subtitle || ""),
      ),
      e("div", { className: "rd-hero-right" }, props.actions || null),
    );
  }

  function Card(props) {
    return e(
      "section",
      { className: "rd-card" },
      e(
        "div",
        { className: "rd-card-header" },
        e(
          "div",
          { className: "rd-card-title" },
          e("h3", null, props.title),
          props.badge || null,
        ),
        props.headerAction || null,
      ),
      e("div", { className: "rd-card-body" }, props.children),
    );
  }

  function AdminDashboard() {
    const onRefreshSystem = () =>
      window.loadSystemStatus && window.loadSystemStatus();

    return e(
      "div",
      { className: "rd-shell" },
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
        e("div", {
          id: "dash-alerts",
          className: "dash-alerts",
          style: { display: "none" },
        }),
      ),

      e(
        "div",
        { className: "rd-grid" },
        e(
          "div",
          { className: "rd-col-span-12" },
          Card({
            title: "Order Overview",
            children: e("div", { id: "stats-cards", className: "stats-grid" }),
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-7" },
          Card({
            title: "System Status",
            headerAction: e(
              "button",
              { className: "btn-xs btn-small", onClick: onRefreshSystem },
              e(IconRefresh, { size: 14 }),
              " ",
              "Refresh",
            ),
            children: e("div", {
              id: "system-status-grid",
              className: "system-status-grid",
            }),
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-5" },
          Card({
            title: "Integrations (CMS / ROS / WMS)",
            children: e("div", {
              id: "integration-status-cards",
              className: "integration-cards",
            }),
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-12" },
          Card({
            title: "Failed Messages",
            badge: e("span", {
              id: "failed-msg-count",
              className: "badge failed",
              style: { marginLeft: 10 },
            }),
            children: e("div", {
              id: "failed-messages-list",
              className: "failed-messages-list",
            }),
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-6" },
          Card({
            title: "Recent Orders",
            children: e("div", { id: "admin-recent-orders" }),
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-6" },
          Card({
            title: "Recent Events",
            children: e("div", { id: "admin-recent-events" }),
          }),
        ),
      ),
    );
  }

  function ClientDashboard() {
    const onRefresh = () => window.loadOrders && window.loadOrders();

    return e(
      "div",
      { className: "rd-shell" },
      e(DashHero, {
        title: "My Orders",
        subtitle: "Track progress, view details, and create new deliveries.",
        actions: e(
          "div",
          { className: "rd-actions" },
          e(
            "button",
            { className: "btn-small btn-outline", onClick: onRefresh },
            e(IconRefresh, null),
            " ",
            "Refresh",
          ),
        ),
      }),
      e(
        "div",
        { className: "rd-grid" },
        e(
          "div",
          { className: "rd-col-span-12" },
          Card({
            title: "Orders",
            children: e("div", { id: "orders-list", className: "orders-grid" }),
          }),
        ),
      ),
    );
  }

  function DriverDashboard() {
    const onRefresh = () => window.loadManifests && window.loadManifests();

    return e(
      "div",
      { className: "rd-shell" },
      e(DashHero, {
        title: "My Deliveries",
        subtitle:
          "Review manifests, follow route guidance, and complete deliveries.",
        actions: e(
          "div",
          { className: "rd-actions" },
          e(
            "button",
            { className: "btn-small btn-outline", onClick: onRefresh },
            e(IconRefresh, null),
            " ",
            "Refresh",
          ),
        ),
      }),

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
          e(
            "svg",
            {
              width: 20,
              height: 20,
              viewBox: "0 0 24 24",
              fill: "none",
              stroke: "currentColor",
              strokeWidth: 2,
            },
            e("path", { d: "M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" }),
            e("path", { d: "M13.73 21a2 2 0 0 1-3.46 0" }),
          ),
          e("span", { id: "driver-notification-text" }),
          e(
            "button",
            {
              onClick: () =>
                window.dismissDriverNotification &&
                window.dismissDriverNotification(),
              className: "btn-dismiss",
            },
            "×",
          ),
        ),
      ),

      e(
        "div",
        { className: "rd-grid" },
        e(
          "div",
          { className: "rd-col-span-12" },
          Card({
            title: "Today",
            children: e(
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
                e(
                  "span",
                  { className: "driver-stat-label" },
                  "Total Deliveries",
                ),
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
          }),
        ),

        e(
          "div",
          { className: "rd-col-span-12" },
          e(
            "div",
            {
              className: "route-info-card",
              id: "route-info-card",
              style: { display: "none" },
            },
            e(
              "h3",
              null,
              e(
                "svg",
                {
                  width: 18,
                  height: 18,
                  viewBox: "0 0 24 24",
                  fill: "none",
                  stroke: "currentColor",
                  strokeWidth: 2,
                },
                e("circle", { cx: "12", cy: "12", r: "10" }),
                e("polygon", {
                  points:
                    "16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88 16.24 7.76",
                }),
              ),
              " ",
              "Optimized Route",
            ),
            e("div", { id: "route-details" }),
          ),
        ),

        e(
          "div",
          { className: "rd-col-span-12" },
          Card({
            title: "Manifests",
            headerAction: e(
              "button",
              { className: "btn-small btn-outline", onClick: onRefresh },
              e(IconRefresh, null),
              " ",
              "Refresh",
            ),
            children: e("div", { id: "driver-manifests" }),
          }),
        ),
      ),
    );
  }

  function safeMount(rootId, element) {
    const rootEl = document.getElementById(rootId);
    if (!rootEl) return;

    // Prevent double-mounting in case showDashboard is called multiple times.
    if (rootEl.__reactMounted) return;
    rootEl.__reactMounted = true;

    const root = ReactDOM.createRoot(rootEl);
    root.render(element);
  }

  window.mountDashboards = function mountDashboards() {
    safeMount("admin-dashboard-root", e(AdminDashboard));
    safeMount("client-dashboard-root", e(ClientDashboard));
    safeMount("driver-dashboard-root", e(DriverDashboard));
  };
})();
