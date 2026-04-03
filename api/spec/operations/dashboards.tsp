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

@route("/dashboards/{dashboard_id}")
@get
@tag("Dashboards")
@summary("Get dashboard")
op getDashboard(dashboard_id: string): Ok<DashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboard_id}/resolved")
@get
@tag("Dashboards")
@summary("Get resolved dashboard")
op getResolvedDashboard(dashboard_id: string): Ok<ResolvedDashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboard_id}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard")
op updateDashboard(dashboard_id: string, @body body: UpdateDashboardRequest): Ok<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboard_id}")
@delete
@tag("Dashboards")
@summary("Delete dashboard")
op deleteDashboard(dashboard_id: string): NoContent | MutatingErrorResponses;

@route("/dashboards/{dashboard_id}/widgets")
@get
@tag("Dashboards")
@summary("List dashboard widgets")
op listDashboardWidgets(dashboard_id: string): Ok<DashboardWidget[]> | ResourceErrorResponses;

@route("/dashboards/{dashboard_id}/widgets")
@post
@tag("Dashboards")
@summary("Create dashboard widget")
op createDashboardWidget(dashboard_id: string, @body body: CreateDashboardWidgetRequest): Created<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboard_id}/widgets/{widget_id}")
@get
@tag("Dashboards")
@summary("Get dashboard widget")
op getDashboardWidget(dashboard_id: string, widget_id: string): Ok<DashboardWidget> | ResourceErrorResponses;

@route("/dashboards/{dashboard_id}/widgets/{widget_id}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard widget")
op updateDashboardWidget(dashboard_id: string, widget_id: string, @body body: UpdateDashboardWidgetRequest): Ok<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboard_id}/widgets/{widget_id}")
@delete
@tag("Dashboards")
@summary("Delete dashboard widget")
op deleteDashboardWidget(dashboard_id: string, widget_id: string): NoContent | MutatingErrorResponses;
