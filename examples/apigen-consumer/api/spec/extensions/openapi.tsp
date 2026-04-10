using TypeSpec.OpenAPI;

namespace Example;

@@extension(getHealth, "x-authz", #{ mode: "public" });
@@extension(listWidgets, "x-authz", #{ mode: "admin_only" });
@@extension(listWidgets, "x-cli-command", "widgets list");
@@extension(createWidget, "x-authz", #{ mode: "admin_only" });
@@extension(createWidget, "x-cli-command", "widgets create");
@@extension(deleteWidget, "x-authz", #{ mode: "admin_only" });
@@extension(deleteWidget, "x-apigen-manual", true);
