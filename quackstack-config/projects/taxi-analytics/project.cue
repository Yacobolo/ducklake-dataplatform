package duckconfig

platform: projects: "taxi-analytics": {
	workspace_ref:  #WorkspaceRef
	kind:           "personal"
	description:    "Seeded analytics authoring project for local development"
	default_branch: "main"
	environments: dev: {
		project_ref:      #ProjectRef
		kind:             "development"
		description:      "Local development environment bound to the seeded taxi catalog"
		target_catalog:   #TargetCatalog
		target_schema:    #TargetSchema
		compute_endpoint: "local-dev"
	}
	macros: safe_divide: {
		macro_type: "SCALAR"
		parameters: [
			"numerator",
			"denominator",
		]
		body:        "CASE WHEN denominator = 0 THEN NULL ELSE numerator / denominator END"
		description: "Avoid divide-by-zero errors in quick seeded analyses"
		owner:       #BootstrapPrincipal
	}
}
