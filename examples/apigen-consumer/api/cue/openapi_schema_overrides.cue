package api

openapi_schema_overrides: {
  Widget: {
    property_order: [
      "id",
      "name",
    ]
  }
  CreateWidgetRequest: {
    property_order: [
      "name",
    ]
  }
  ListWidgetsResponse: {
    property_order: [
      "data",
    ]
  }
  Error: {
    property_order: [
      "code",
      "message",
    ]
  }
}
