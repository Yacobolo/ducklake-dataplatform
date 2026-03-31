using TypeSpec.Http;

namespace Duck;

@route("/dashboards")
@get
@tag("Dashboards")
@summary("List dashboards")
op listDashboards(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedDashboards> | StandardErrorResponses;

@route("/dashboards")
@post
@tag("Dashboards")
@summary("Create dashboard")
op createDashboard(@body body: CreateDashboardRequest): Created<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@get
@tag("Dashboards")
@summary("Get dashboard")
op getDashboard(dashboardId: string): Ok<DashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}/resolved")
@get
@tag("Dashboards")
@summary("Get resolved dashboard")
op getResolvedDashboard(dashboardId: string, @query f?: string[]): Ok<ResolvedDashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard")
op updateDashboard(dashboardId: string, @body body: UpdateDashboardRequest): Ok<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@delete
@tag("Dashboards")
@summary("Delete dashboard")
op deleteDashboard(dashboardId: string): NoContent | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets")
@post
@tag("Dashboards")
@summary("Create dashboard widget")
op createDashboardWidget(dashboardId: string, @body body: CreateDashboardWidgetRequest): Created<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard widget")
op updateDashboardWidget(dashboardId: string, widgetId: string, @body body: UpdateDashboardWidgetRequest): Ok<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@delete
@tag("Dashboards")
@summary("Delete dashboard widget")
op deleteDashboardWidget(dashboardId: string, widgetId: string): NoContent | MutatingErrorResponses;
