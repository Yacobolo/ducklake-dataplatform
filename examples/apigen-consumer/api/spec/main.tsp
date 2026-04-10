import "@typespec/http";
import "@typespec/openapi";
import "@typespec/openapi3";
import "./extensions/openapi.tsp";

using TypeSpec.Http;
using TypeSpec.OpenAPI;

@service(#{ title: "APIGen Example API" })
@doc("Minimal API used to prove APIGen can be consumed from a second project boundary.")
@info(#{ version: "0.1.0" })
@server("https://example.local/v1", "Example server")
@tagMetadata("Operations", #{ description: "Operational endpoints." })
@tagMetadata("Widgets", #{ description: "Widget management endpoints." })
namespace Example;

@error
model Error {
  code: int32;
  message: string;
}

model Ok<T> {
  @statusCode statusCode: 200;
  @body body: T;
}

model Created<T> {
  @statusCode statusCode: 201;
  @body body: T;
}

model NoContent {
  @statusCode statusCode: 204;
}

@error
model BadRequestResponse {
  @statusCode statusCode: 400;
  @body body: Error;
}

alias StandardErrorResponses = BadRequestResponse;

model HealthResponse {
  status: string;
}

model Widget {
  id: string;
  name: string;
}

model PaginatedWidgets {
  data: Widget[];
  next_page_token?: string;
}

model CreateWidgetRequest {
  name: string;
}

@route("/healthz")
@get
@tag("Operations")
@summary("Get health")
op getHealth(): Ok<HealthResponse>;

@route("/widgets")
@get
@tag("Widgets")
@summary("List widgets")
op listWidgets(
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedWidgets> | StandardErrorResponses;

@route("/widgets")
@post
@tag("Widgets")
@summary("Create widget")
op createWidget(@body body: CreateWidgetRequest): Created<Widget> | StandardErrorResponses;

@route("/widgets/{widgetId}")
@delete
@tag("Widgets")
@summary("Delete widget")
op deleteWidget(@path widgetId: string): NoContent | StandardErrorResponses;
