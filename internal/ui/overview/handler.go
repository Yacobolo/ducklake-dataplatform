package overview

import (
	"net/http"

	"duck-demo/internal/ui/core"
)

type Handler struct{}

func New() *Handler { return &Handler{} }

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, overviewPage(core.PrincipalFromContext(r.Context()), []overviewSectionData{
		{
			Kicker:      "Discover",
			Title:       "Find governed data consumers should trust first",
			Description: "Start with the published interface layer, then drill into operational detail only when you need to validate runtime state or ownership.",
			StartHref:   "/ui/products",
			StartLabel:  "Start in Products",
			StartCopy:   "Products package ownership, contract, trust signals, and linked runtime assets into the clearest consumer-facing entry point.",
			Links: []overviewLinkData{
				{Label: "Explore", Copy: "Browse folders and mixed authored assets from one shared workspace-style browser.", Href: "/ui/explore"},
				{Label: "Catalogs", Copy: "Browse schemas, tables, and version-aware metastore detail from the explorer workspace.", Href: "/ui/catalogs"},
				{Label: "Runtime Assets", Copy: "Inspect freshness, orchestration state, and asset-level runtime signals.", Href: "/ui/assets"},
				{Label: "Dashboards", Copy: "Review published dashboard surfaces and their supporting widgets.", Href: "/ui/dashboards"},
			},
		},
		{
			Kicker:      "Build",
			Title:       "Create semantic, notebook, and transformation assets",
			Description: "Use build workspaces when you are authoring logic, defining reusable models, or shaping the semantic layer that products expose.",
			StartHref:   "/ui/models",
			StartLabel:  "Start in Models",
			StartCopy:   "Models are the backbone of the transformation layer, with macros tucked alongside them as implementation support rather than primary navigation.",
			Links: []overviewLinkData{
				{Label: "Explore", Copy: "Work across notebooks, dashboards, pipelines, and project-bound assets from one browser.", Href: "/ui/explore"},
				{Label: "Semantic", Copy: "Manage semantic models, pre-aggregations, and relationship paths for consumer queries.", Href: "/ui/semantic"},
				{Label: "Macros", Copy: "Open reusable SQL helper definitions from the build toolchain workspace.", Href: "/ui/macros"},
			},
		},
		{
			Kicker:      "Operate",
			Title:       "Manage access, policy, storage, and platform health",
			Description: "Operational workspaces focus on who can access data, how the platform executes work, and where governance and storage controls are enforced.",
			StartHref:   "/ui/security/principals",
			StartLabel:  "Start in Security",
			StartCopy:   "Principals and groups are the operational backbone for permissions, API keys, row filters, and masking policies.",
			Links: []overviewLinkData{
				{Label: "Governance", Copy: "Search metadata, inspect query history, and manage tags, lineage, and manifests.", Href: "/ui/governance/search"},
				{Label: "Storage", Copy: "Manage credentials, external locations, and volumes used by the platform.", Href: "/ui/storage"},
				{Label: "Compute", Copy: "Inspect endpoints, health, and principal assignments for remote execution.", Href: "/ui/compute/endpoints"},
			},
		},
	}))
}
