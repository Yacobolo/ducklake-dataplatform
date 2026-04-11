package api

// Authored notebook operations.

#notebooksTag: "Notebooks"

#wrappedNotebookOperation: #genericOperationSpec & {
	wrapped: true
}

#plainNotebookOperation: #genericOperationSpec & {
	wrapped: false
}

#notebookIDPathParameter: #pathStringParameter & {
	#name: "notebook_id"
}

#cellIDPathParameter: #pathStringParameter & {
	#name: "cell_id"
}

#sessionIDPathParameter: #pathStringParameter & {
	#name: "session_id"
}

#jobIDPathParameter: #pathStringParameter & {
	#name: "job_id"
}

#principalNamePathParameter: #pathStringParameter & {
	#name: "principal_name"
}

#ownerQueryParameter: #queryStringParameter & {
	#name: "owner"
}

#notebookPathParameters: [
	#notebookIDPathParameter,
]

#notebookCellPathParameters: [
	#notebookIDPathParameter,
	#cellIDPathParameter,
]

#notebookSessionPathParameters: [
	#notebookIDPathParameter,
	#sessionIDPathParameter,
]

#notebookJobPathParameters: [
	#notebookIDPathParameter,
	#jobIDPathParameter,
]

#notebookSharePathParameters: [
	#notebookIDPathParameter,
	#principalNamePathParameter,
]

#executeCellPathParameters: [
	#notebookIDPathParameter,
	#sessionIDPathParameter,
	#cellIDPathParameter,
]

#listNotebookParameters: [
	#ownerQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listNotebookJobsParameters: [
	#notebookIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#notebookOps: [
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "get"
		op:           "listNotebooks"
		path:         "/notebooks"
		summary:      "List notebooks"
		cli:          "notebooks notebooks list"
		returns:      "PaginatedNotebooks"
		error_family: "standard"
		params:       #listNotebookParameters
	},
	#wrappedNotebookOperation & {
		kind:           "response"
		method:         "post"
		op:             "createNotebook"
		path:           "/notebooks"
		summary:        "Create notebook"
		cli:            "notebooks notebooks create"
		returns:        "Notebook"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateNotebookRequest"
		body_description: "Request payload"
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "get"
		op:           "getNotebook"
		path:         "/notebooks/{notebook_id}"
		summary:      "Get notebook"
		cli:          "notebooks notebooks get"
		returns:      "NotebookDetail"
		error_family: "resource"
		params:       #notebookPathParameters
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateNotebook"
		path:         "/notebooks/{notebook_id}"
		summary:      "Update notebook"
		cli:          "notebooks notebooks update"
		returns:      "Notebook"
		error_family: "resource"
		params:       #notebookPathParameters
		body_ref:     "UpdateNotebookRequest"
		body_description: "Request payload"
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "post"
		op:           "moveNotebook"
		path:         "/notebooks/{notebook_id}/moves"
		summary:      "Move notebook"
		returns:      "Notebook"
		error_family: "resource"
		params:       #notebookPathParameters
		body_ref:     "MoveNotebookRequest"
		body_description: "Request payload"
	},
	#wrappedNotebookOperation & {
		kind:           "response"
		method:         "post"
		op:             "duplicateNotebook"
		path:           "/notebooks/{notebook_id}/copies"
		summary:        "Duplicate notebook"
		returns:        "Notebook"
		success_status: 201
		error_family:   "resource"
		params:         #notebookPathParameters
		body_ref:       "DuplicateNotebookRequest"
		body_description: "Request payload"
	},
	#plainNotebookOperation & {
		kind:         "response"
		method:       "get"
		op:           "listNotebookShares"
		path:         "/notebooks/{notebook_id}/shares"
		summary:      "List notebook shares"
		error_family: "resource"
		params:       #notebookPathParameters
		success_schema: {
			type: "array"
			items: {
				ref: "NotebookShare"
			}
		}
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "post"
		op:           "shareNotebook"
		path:         "/notebooks/{notebook_id}/shares"
		summary:      "Share notebook"
		returns:      "NotebookShare"
		error_family: "resource"
		params:       #notebookPathParameters
		body_ref:     "ShareNotebookRequest"
		body_description: "Request payload"
	},
	#plainNotebookOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "unshareNotebook"
		path:         "/notebooks/{notebook_id}/shares/{principal_name}"
		summary:      "Remove notebook share"
		error_family: "resource"
		params:       #notebookSharePathParameters
	},
	#plainNotebookOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteNotebook"
		path:         "/notebooks/{notebook_id}"
		summary:      "Delete notebook"
		cli:          "notebooks notebooks delete"
		error_family: "resource"
		params:       #notebookPathParameters
	},
	#wrappedNotebookOperation & {
		kind:           "response"
		method:         "post"
		op:             "createCell"
		path:           "/notebooks/{notebook_id}/cells"
		summary:        "Create cell"
		cli:            "notebooks cells create"
		returns:        "Cell"
		success_status: 201
		error_family:   "resource"
		params:         #notebookPathParameters
		body_ref:       "CreateCellRequest"
		body_description: "Request payload"
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "post"
		op:           "reorderCells"
		path:         "/notebooks/{notebook_id}/cells/reorder"
		summary:      "Reorder cells"
		cli:          "notebooks cells reorder"
		returns:      "CellList"
		error_family: "resource"
		params:       #notebookPathParameters
		body_ref:     "ReorderCellsRequest"
		body_description: "Request payload"
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateCell"
		path:         "/notebooks/{notebook_id}/cells/{cell_id}"
		summary:      "Update cell"
		cli:          "notebooks cells update"
		returns:      "Cell"
		error_family: "resource"
		params:       #notebookCellPathParameters
		body_ref:     "UpdateCellRequest"
		body_description: "Request payload"
	},
	#plainNotebookOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteCell"
		path:         "/notebooks/{notebook_id}/cells/{cell_id}"
		summary:      "Delete cell"
		cli:          "notebooks cells delete"
		error_family: "resource"
		params:       #notebookCellPathParameters
	},
	#wrappedNotebookOperation & {
		kind:           "response"
		method:         "post"
		op:             "createNotebookSession"
		path:           "/notebooks/{notebook_id}/sessions"
		summary:        "Create notebook session"
		cli:            "notebooks sessions create"
		returns:        "NotebookSession"
		success_status: 201
		error_family:   "resource"
		params:         #notebookPathParameters
	},
	#plainNotebookOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "closeNotebookSession"
		path:         "/notebooks/{notebook_id}/sessions/{session_id}"
		summary:      "Close notebook session"
		cli:          "notebooks sessions close"
		error_family: "resource"
		params:       #notebookSessionPathParameters
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "post"
		op:           "executeCell"
		path:         "/notebooks/{notebook_id}/sessions/{session_id}/cell-executions/{cell_id}"
		summary:      "Execute cell"
		cli:          "notebooks cells execute"
		returns:      "CellExecutionResult"
		error_family: "resource"
		params:       #executeCellPathParameters
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "post"
		op:           "runAllCells"
		path:         "/notebooks/{notebook_id}/sessions/{session_id}/cell-executions"
		summary:      "Run all cells"
		cli:          "notebooks sessions run-all"
		returns:      "RunAllResult"
		error_family: "resource"
		params:       #notebookSessionPathParameters
	},
	#wrappedNotebookOperation & {
		kind:           "response"
		method:         "post"
		op:             "runAllCellsAsync"
		path:           "/notebooks/{notebook_id}/sessions/{session_id}/job-runs"
		summary:        "Run all cells asynchronously"
		cli:            "notebooks sessions run-all-async"
		returns:        "NotebookJob"
		success_status: 202
		error_family:   "resource"
		params:         #notebookSessionPathParameters
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "get"
		op:           "listNotebookJobs"
		path:         "/notebooks/{notebook_id}/jobs"
		summary:      "List notebook jobs"
		cli:          "notebooks jobs list"
		returns:      "PaginatedNotebookJobs"
		error_family: "resource"
		params:       #listNotebookJobsParameters
	},
	#wrappedNotebookOperation & {
		kind:         "response"
		method:       "get"
		op:           "getNotebookJob"
		path:         "/notebooks/{notebook_id}/jobs/{job_id}"
		summary:      "Get notebook job"
		cli:          "notebooks jobs get"
		returns:      "NotebookJob"
		error_family: "resource"
		params:       #notebookJobPathParameters
	},
]

endpoints_notebooks: [
	for op in #notebookOps {
		(#endpointFromGenericOperation & {
			tag:  #notebooksTag
			spec: op
		}).endpoint
	},
]
