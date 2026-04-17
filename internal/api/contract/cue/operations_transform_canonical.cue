package api

import "list"

// Canonical project/environment-centered transformation operations.

#transformCanonicalTag: "Projects"

#buildIDPathParameterCanonical: #pathStringParameter & {
	#name: "build_id"
}

#compilationIDPathParameter: #pathStringParameter & {
	#name: "compilation_id"
}

#releaseIDPathParameterCanonical: #pathStringParameter & {
	#name: "release_id"
}

#runIDPathParameterCanonical: #pathStringParameter & {
	#name: "run_id"
}

#stepIDPathParameterCanonical: #pathStringParameter & {
	#name: "step_id"
}

#columnNamePathParameterCanonical: #pathStringParameter & {
	#name: "column_name"
}

#canonicalProjectModelPathParameters: [
	#projectIDPathParameter,
	#modelNamePathParameter,
]

#canonicalProjectMacroPathParameters: [
	#projectIDPathParameter,
	#macroNamePathParameter,
]

#canonicalProjectModelTestPathParameters: [
	#projectIDPathParameter,
	#modelNamePathParameter,
	#testIDPathParameter,
]

#projectEnvironmentPathParametersOnly: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
]

#projectEnvironmentModelPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#modelNamePathParameter,
]

#projectEnvironmentMacroPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#macroNamePathParameter,
]

#projectEnvironmentSourcePathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#sourceNamePathParameter,
	#sourceTableNamePathParameter,
]

#projectEnvironmentSourceColumnPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#sourceNamePathParameter,
	#sourceTableNamePathParameter,
	#columnNamePathParameterCanonical,
]

#projectEnvironmentCompilationPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#compilationIDPathParameter,
]

#projectEnvironmentCompilationModelPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#compilationIDPathParameter,
	#modelNamePathParameter,
]

#projectEnvironmentCompilationMacroPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#compilationIDPathParameter,
	#macroNamePathParameter,
]

#projectEnvironmentCompilationSourceColumnPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#compilationIDPathParameter,
	#sourceNamePathParameter,
	#sourceTableNamePathParameter,
	#columnNamePathParameterCanonical,
]

#projectEnvironmentBuildPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
]

#projectEnvironmentBuildModelPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
	#modelNamePathParameter,
]

#projectEnvironmentBuildMacroPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
	#macroNamePathParameter,
]

#projectEnvironmentBuildSourceColumnPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
	#sourceNamePathParameter,
	#sourceTableNamePathParameter,
	#columnNamePathParameterCanonical,
]

#projectEnvironmentBuildRunPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
	#runIDPathParameterCanonical,
]

#projectEnvironmentBuildOnlyPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#buildIDPathParameterCanonical,
]

#projectEnvironmentRunPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#runIDPathParameterCanonical,
]

#projectEnvironmentRunStepPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
	#runIDPathParameterCanonical,
	#stepIDPathParameterCanonical,
]

#projectReleasePathParametersCanonical: [
	#projectIDPathParameter,
	#releaseIDPathParameterCanonical,
]

#transformCanonicalOps: [
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectDAGByID"
		path:         "/projects/{project_id}/dag"
		summary:      "Get project DAG"
		cli:          "projects dag get"
		returns:      "ModelDAG"
		error_family: "resource"
		params:       #projectPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectModelsByID"
		path:         "/projects/{project_id}/models"
		summary:      "List project models"
		cli:          "projects models list"
		returns:      "PaginatedModels"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectModelByID"
		path:             "/projects/{project_id}/models"
		summary:          "Create project model"
		cli:              "projects models create"
		returns:          "Model"
		success_status:   201
		error_family:     "mutating"
		params:           #projectPathParameters
		body_ref:         "CreateModelRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectModelByID"
		path:         "/projects/{project_id}/models/{model_name}"
		summary:      "Get project model"
		cli:          "projects models get"
		returns:      "Model"
		error_family: "resource"
		params:       #canonicalProjectModelPathParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "patch"
		op:               "updateProjectModelByID"
		path:             "/projects/{project_id}/models/{model_name}"
		summary:          "Update project model"
		cli:              "projects models update"
		returns:          "Model"
		error_family:     "mutating"
		params:           #canonicalProjectModelPathParameters
		body_ref:         "UpdateModelRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectModelByID"
		path:         "/projects/{project_id}/models/{model_name}"
		summary:      "Delete project model"
		cli:          "projects models delete"
		error_family: "mutating"
		params:       #canonicalProjectModelPathParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectModelTestByID"
		path:             "/projects/{project_id}/models/{model_name}/tests"
		summary:          "Create project model test"
		cli:              "projects models tests create"
		returns:          "ModelTest"
		success_status:   201
		error_family:     "mutating"
		params:           #canonicalProjectModelPathParameters
		body_ref:         "CreateModelTestRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectModelTestsByID"
		path:         "/projects/{project_id}/models/{model_name}/tests"
		summary:      "List project model tests"
		cli:          "projects models tests list"
		returns:      "ModelTestList"
		error_family: "resource"
		params:       #canonicalProjectModelPathParameters
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectModelTestByID"
		path:         "/projects/{project_id}/models/{model_name}/tests/{test_id}"
		summary:      "Delete project model test"
		cli:          "projects models tests delete"
		error_family: "mutating"
		params:       #canonicalProjectModelTestPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectMacrosByID"
		path:         "/projects/{project_id}/macros"
		summary:      "List project macros"
		cli:          "projects macros list"
		returns:      "PaginatedMacros"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectMacroByID"
		path:             "/projects/{project_id}/macros"
		summary:          "Create project macro"
		cli:              "projects macros create"
		returns:          "Macro"
		success_status:   201
		error_family:     "mutating"
		params:           #projectPathParameters
		body_ref:         "CreateMacroRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectMacroByID"
		path:         "/projects/{project_id}/macros/{macro_name}"
		summary:      "Get project macro"
		cli:          "projects macros get"
		returns:      "Macro"
		error_family: "resource"
		params:       #canonicalProjectMacroPathParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "patch"
		op:               "updateProjectMacroByID"
		path:             "/projects/{project_id}/macros/{macro_name}"
		summary:          "Update project macro"
		cli:              "projects macros update"
		returns:          "Macro"
		error_family:     "mutating"
		params:           #canonicalProjectMacroPathParameters
		body_ref:         "UpdateMacroRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectMacroByID"
		path:         "/projects/{project_id}/macros/{macro_name}"
		summary:      "Delete project macro"
		cli:          "projects macros delete"
		error_family: "mutating"
		params:       #canonicalProjectMacroPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectMacroRevisionsByID"
		path:         "/projects/{project_id}/macros/{macro_name}/revisions"
		summary:      "List project macro revisions"
		cli:          "projects macros revisions list"
		returns:      "MacroRevisionList"
		error_family: "resource"
		params:       #canonicalProjectMacroPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "diffProjectMacroRevisionsByID"
		path:         "/projects/{project_id}/macros/{macro_name}/revision-diffs"
		summary:      "Diff project macro revisions"
		cli:          "projects macros revisions diff"
		returns:      "MacroRevisionDiff"
		error_family: "resource"
		params:       list.Concat([#canonicalProjectMacroPathParameters, [#fromVersionQueryParameter, #toVersionQueryParameter]])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "checkProjectEnvironmentModelFreshness"
		path:         "/projects/{project_id}/environments/{environment_id}/models/{model_name}/freshness"
		summary:      "Check project environment model freshness"
		returns:      "FreshnessStatus"
		error_family: "resource"
		params:       #projectEnvironmentModelPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "checkProjectEnvironmentSourceFreshness"
		path:         "/projects/{project_id}/environments/{environment_id}/sources/{source_name}/{table_name}/freshness"
		summary:      "Check project environment source freshness"
		returns:      "SourceFreshnessStatus"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentSourcePathParameters, [#queryStringParameter & {#name: "timestamp_column"}, #queryInt64Parameter & {#name: "max_lag_seconds"}]])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironmentCompilations"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations"
		summary:      "List project environment compilations"
		cli:          "projects environments compilations list"
		returns:      "PaginatedCompilations"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentPathParametersOnly, #paginationParameters])
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectEnvironmentCompilation"
		path:             "/projects/{project_id}/environments/{environment_id}/compilations"
		summary:          "Create project environment compilation"
		cli:              "projects environments compilations create"
		returns:          "Compilation"
		success_status:   201
		error_family:     "mutating"
		params:           #projectEnvironmentPathParametersOnly
		body_ref:         "CreateCompilationRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilation"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}"
		summary:      "Get project environment compilation"
		cli:          "projects environments compilations get"
		returns:      "Compilation"
		error_family: "resource"
		params:       #projectEnvironmentCompilationPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilationDiagnostics"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}/diagnostics"
		summary:      "Get project environment compilation diagnostics"
		cli:          "projects environments compilations diagnostics list"
		returns:      "PaginatedCompileDiagnostics"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentCompilationPathParameters, [#modelNameQueryParameter, #diagnosticSeverityQueryParameter, #diagnosticCodeQueryParameter], #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilationColumnLineage"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}/lineage/columns"
		summary:      "Get project environment compilation column lineage"
		cli:          "projects environments compilations lineage columns"
		returns:      "PaginatedCompiledColumnLineage"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentCompilationPathParameters, [#modelNameQueryParameter], #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilationModelImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}/impacts/models/{model_name}"
		summary:      "Get project environment compilation model impact"
		returns:      "BuildImpactResult"
		error_family: "resource"
		params:       #projectEnvironmentCompilationModelPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilationMacroImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}/impacts/macros/{macro_name}"
		summary:      "Get project environment compilation macro impact"
		returns:      "BuildImpactResult"
		error_family: "resource"
		params:       #projectEnvironmentCompilationMacroPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentCompilationSourceColumnImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/compilations/{compilation_id}/impacts/sources/{source_name}/{table_name}/columns/{column_name}"
		summary:      "Get project environment compilation impact for a source column"
		returns:      "PaginatedCompiledColumnLineage"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentCompilationSourceColumnPathParameters, #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironmentBuilds"
		path:         "/projects/{project_id}/environments/{environment_id}/builds"
		summary:      "List project environment builds"
		cli:          "projects environments builds list"
		returns:      "PaginatedBuilds"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentPathParametersOnly, #paginationParameters])
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectEnvironmentBuild"
		path:             "/projects/{project_id}/environments/{environment_id}/builds"
		summary:          "Create project environment build"
		cli:              "projects environments builds create"
		returns:          "Build"
		success_status:   201
		error_family:     "mutating"
		params:           #projectEnvironmentPathParametersOnly
		body_ref:         "CreateCompilationRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuild"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}"
		summary:      "Get project environment build"
		cli:          "projects environments builds get"
		returns:      "Build"
		error_family: "resource"
		params:       #projectEnvironmentBuildPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuildDiagnostics"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/diagnostics"
		summary:      "Get project environment build diagnostics"
		cli:          "projects environments builds diagnostics list"
		returns:      "PaginatedCompileDiagnostics"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentBuildPathParameters, [#modelNameQueryParameter, #diagnosticSeverityQueryParameter, #diagnosticCodeQueryParameter], #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuildColumnLineage"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/lineage/columns"
		summary:      "Get project environment build column lineage"
		cli:          "projects environments builds lineage columns"
		returns:      "PaginatedCompiledColumnLineage"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentBuildPathParameters, [#modelNameQueryParameter], #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuildModelImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/impacts/models/{model_name}"
		summary:      "Get project environment build model impact"
		returns:      "BuildImpactResult"
		error_family: "resource"
		params:       #projectEnvironmentBuildModelPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuildMacroImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/impacts/macros/{macro_name}"
		summary:      "Get project environment build macro impact"
		returns:      "BuildImpactResult"
		error_family: "resource"
		params:       #projectEnvironmentBuildMacroPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentBuildSourceColumnImpact"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/impacts/sources/{source_name}/{table_name}/columns/{column_name}"
		summary:      "Get project environment build impact for a source column"
		returns:      "PaginatedCompiledColumnLineage"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentBuildSourceColumnPathParameters, #paginationParameters])
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironmentBuildRuns"
		path:         "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/runs"
		summary:      "List project environment build runs"
		cli:          "projects environments builds runs list"
		returns:      "PaginatedModelRuns"
		error_family: "resource"
		params:       list.Concat([#projectEnvironmentBuildPathParameters, #paginationParameters])
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectEnvironmentBuildRun"
		path:             "/projects/{project_id}/environments/{environment_id}/builds/{build_id}/runs"
		summary:          "Create project environment build run"
		cli:              "projects environments builds runs create"
		returns:          "ModelRun"
		success_status:   201
		error_family:     "mutating"
		params:           #projectEnvironmentBuildPathParameters
		body_ref:         "CreateBuildRunRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectEnvironmentRun"
		path:         "/projects/{project_id}/environments/{environment_id}/runs/{run_id}"
		summary:      "Get project environment run"
		cli:          "projects environments runs get"
		returns:      "ModelRun"
		error_family: "resource"
		params:       #projectEnvironmentRunPathParameters
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "post"
		op:           "cancelProjectEnvironmentRun"
		path:         "/projects/{project_id}/environments/{environment_id}/runs/{run_id}/cancellations"
		summary:      "Cancel project environment run"
		cli:          "projects environments runs cancel"
		error_family: "mutating"
		params:       #projectEnvironmentRunPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironmentRunSteps"
		path:         "/projects/{project_id}/environments/{environment_id}/runs/{run_id}/steps"
		summary:      "List project environment run steps"
		cli:          "projects environments runs steps list"
		returns:      "ModelRunStepList"
		error_family: "resource"
		params:       #projectEnvironmentRunPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironmentRunStepTestResults"
		path:         "/projects/{project_id}/environments/{environment_id}/runs/{run_id}/steps/{step_id}/test-results"
		summary:      "List project environment run step test results"
		cli:          "projects environments runs steps test-results list"
		returns:      "ModelTestResultList"
		error_family: "resource"
		params:       #projectEnvironmentRunStepPathParameters
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectEnvironmentRebuildPlan"
		path:             "/projects/{project_id}/environments/{environment_id}/rebuild-plans"
		summary:          "Create project environment rebuild plan"
		cli:              "projects environments rebuild-plans create"
		returns:          "RebuildPlan"
		error_family:     "resource"
		params:           #projectEnvironmentPathParametersOnly
		body_ref:         "PlanRebuildRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:             "response"
		method:           "post"
		op:               "createProjectEnvironmentBuildComparison"
		path:             "/projects/{project_id}/environments/{environment_id}/build-comparisons"
		summary:          "Create project environment build comparison"
		cli:              "projects environments build-comparisons create"
		returns:          "BuildCompareResult"
		error_family:     "resource"
		params:           #projectEnvironmentPathParametersOnly
		body_ref:         "CompareBuildsRequest"
		body_description: "Request payload"
	},
]

endpoints_transform_canonical: [
	for op in #transformCanonicalOps {
		(#endpointFromGenericOperation & {
			tag:  #transformCanonicalTag
			spec: op
		}).endpoint
	},
]
