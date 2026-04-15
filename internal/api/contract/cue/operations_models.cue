package api

// Authored model operations.

#modelsTag: "Models"

#projectNamePathParameter: #pathStringParameter & {
	#name: "project_name"
}

#modelNamePathParameter: #pathStringParameter & {
	#name: "model_name"
}

#runIDPathParameter: #pathStringParameter & {
	#name: "run_id"
}

#stepIDPathParameter: #pathStringParameter & {
	#name: "step_id"
}

#testIDPathParameter: #pathStringParameter & {
	#name: "test_id"
}

#notebookIDPathParameter: #pathStringParameter & {
	#name: "notebook_id"
}

#macroNamePathParameter: #pathStringParameter & {
	#name: "macro_name"
}

#statusQueryParameter: #queryStringParameter & {
	#name: "status"
}

#projectNameQueryParameter: #queryStringParameter & {
	#name: "project_name"
}

#requiredQueryInt32Parameter: {
	#name: string

	name:     #name
	in:       "query"
	required: true
	explode:  false
	schema: {
		type:   "integer"
		format: "int32"
	}
}

#fromVersionQueryParameter: #requiredQueryInt32Parameter & {
	#name: "from_version"
}

#toVersionQueryParameter: #requiredQueryInt32Parameter & {
	#name: "to_version"
}

#modelPathParameters: [
	#projectNamePathParameter,
	#modelNamePathParameter,
]

#modelTestPathParameters: [
	#projectNamePathParameter,
	#modelNamePathParameter,
	#testIDPathParameter,
]

#modelRunStepResultPathParameters: [
	#runIDPathParameter,
	#stepIDPathParameter,
]

#macroImpactParameters: [
	#macroNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listModelsParameters: [
	#projectNameQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listModelRunsParameters: [
	#statusQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listMacrosParameters: #paginationParameters

#diffMacroRevisionsParameters: [
	#macroNamePathParameter,
	#fromVersionQueryParameter,
	#toVersionQueryParameter,
]

#modelOps: [
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listModels"
		path:         "/models"
		summary:      "List models"
		cli:          "models list"
		returns:      "PaginatedModels"
		error_family: "standard"
		params:       #listModelsParameters
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createModel"
		path:           "/models"
		summary:        "Create model"
		cli:            "models create"
		returns:        "Model"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateModelRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getModelDAG"
		path:         "/models/dag"
		summary:      "Get model DAG"
		cli:          "models dag get"
		returns:      "ModelDAG"
		error_family: "standard"
		params: [
			#projectNameQueryParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getModel"
		path:         "/models/{project_name}/{model_name}"
		summary:      "Get model"
		cli:          "models get"
		returns:      "Model"
		error_family: "resource"
		params:       #modelPathParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "patch"
		op:           "updateModel"
		path:         "/models/{project_name}/{model_name}"
		summary:      "Update model"
		cli:          "models update"
		returns:      "Model"
		error_family: "mutating"
		params:       #modelPathParameters
		body_ref:     "UpdateModelRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteModel"
		path:         "/models/{project_name}/{model_name}"
		summary:      "Delete model"
		cli:          "models delete"
		error_family: "mutating"
		params:       #modelPathParameters
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createModelTest"
		path:           "/models/{project_name}/{model_name}/tests"
		summary:        "Create model test"
		cli:            "models tests create"
		returns:        "ModelTest"
		success_status: 201
		error_family:   "mutating"
		params:         #modelPathParameters
		body_ref:       "CreateModelTestRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listModelTests"
		path:         "/models/{project_name}/{model_name}/tests"
		summary:      "List model tests"
		cli:          "models tests list"
		returns:      "ModelTestList"
		error_family: "resource"
		params:       #modelPathParameters
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteModelTest"
		path:         "/models/{project_name}/{model_name}/tests/{test_id}"
		summary:      "Delete model test"
		cli:          "models tests delete"
		error_family: "mutating"
		params:       #modelTestPathParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "checkModelFreshness"
		path:         "/models/{project_name}/{model_name}/freshness"
		summary:      "Check model freshness"
		cli:          "models freshness check"
		returns:      "FreshnessStatus"
		error_family: "resource"
		params:       #modelPathParameters
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "triggerModelRun"
		path:           "/model-runs"
		summary:        "Trigger model run"
		cli:            "models runs trigger"
		returns:        "ModelRun"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "TriggerModelRunRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listModelRuns"
		path:         "/model-runs"
		summary:      "List model runs"
		cli:          "models runs list"
		returns:      "PaginatedModelRuns"
		error_family: "standard"
		params:       #listModelRunsParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getModelRun"
		path:         "/model-runs/{run_id}"
		summary:      "Get model run"
		cli:          "models runs get"
		returns:      "ModelRun"
		error_family: "resource"
		params: [
			#runIDPathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "post"
		op:           "cancelModelRun"
		path:         "/model-runs/{run_id}/cancellations"
		summary:      "Cancel model run"
		cli:          "models runs cancel"
		returns:      "ModelRun"
		error_family: "mutating"
		params: [
			#runIDPathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listModelRunSteps"
		path:         "/model-runs/{run_id}/steps"
		summary:      "List model run steps"
		cli:          "models steps list"
		returns:      "ModelRunStepList"
		error_family: "resource"
		params: [
			#runIDPathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listModelTestResults"
		path:         "/model-runs/{run_id}/steps/{step_id}/test-results"
		summary:      "List model test results"
		cli:          "models test-results list"
		returns:      "ModelTestResultList"
		error_family: "resource"
		params:       #modelRunStepResultPathParameters
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "promoteNotebookToModel"
		path:           "/notebooks/{notebook_id}/model-promotions"
		summary:        "Promote notebook to model"
		cli:            "models from-notebook promote"
		returns:        "Model"
		success_status: 201
		error_family:   "mutating"
		params: [
			#notebookIDPathParameter,
		]
		body_ref:       "PromoteNotebookRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "unpublishNotebookModel"
		path:         "/notebooks/{notebook_id}/model-promotions"
		summary:      "Unpublish notebook model"
		error_family: "resource"
		params: [
			#notebookIDPathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listMacros"
		path:         "/macros"
		summary:      "List macros"
		cli:          "models macros list"
		returns:      "PaginatedMacros"
		error_family: "standard"
		params:       #listMacrosParameters
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createMacro"
		path:           "/macros"
		summary:        "Create macro"
		cli:            "models macros create"
		returns:        "Macro"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateMacroRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getMacro"
		path:         "/macros/{macro_name}"
		summary:      "Get macro"
		cli:          "models macros get"
		returns:      "Macro"
		error_family: "resource"
		params: [
			#macroNamePathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "patch"
		op:           "updateMacro"
		path:         "/macros/{macro_name}"
		summary:      "Update macro"
		cli:          "models macros update"
		returns:      "Macro"
		error_family: "mutating"
		params: [
			#macroNamePathParameter,
		]
		body_ref:     "UpdateMacroRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteMacro"
		path:         "/macros/{macro_name}"
		summary:      "Delete macro"
		cli:          "models macros delete"
		error_family: "mutating"
		params: [
			#macroNamePathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listMacroRevisions"
		path:         "/macros/{macro_name}/revisions"
		summary:      "List macro revisions"
		cli:          "models revisions list"
		returns:      "MacroRevisionList"
		error_family: "resource"
		params: [
			#macroNamePathParameter,
		]
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getMacroImpact"
		path:         "/macros/{macro_name}/impacts"
		summary:      "Get macro impact"
		cli:          "models impact get"
		returns:      "MacroImpactList"
		error_family: "resource"
		params:       #macroImpactParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "diffMacroRevisions"
		path:         "/macros/{macro_name}/revision-diffs"
		summary:      "Diff macro revisions"
		cli:          "models revisions diff"
		returns:      "MacroRevisionDiff"
		error_family: "resource"
		params:       #diffMacroRevisionsParameters
	},
]

endpoints_models: [
	for op in #modelOps {
		(#endpointFromGenericOperation & {
			tag:  #modelsTag
			spec: op
		}).endpoint
	},
]
