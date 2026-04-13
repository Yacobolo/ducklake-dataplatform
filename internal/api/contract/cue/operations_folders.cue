package api

// Authored folder operations.

#foldersTag: "Folders"

#plainFolderOperation: #genericOperationSpec & {
	wrapped: false
}

#folderIDPathParameter: #pathStringParameter & {
	#name: "folder_id"
}

#folderWorkspaceIDPathParameter: #pathStringParameter & {
	#name: "workspace_id"
}

#folderPrincipalNamePathParameter: #pathStringParameter & {
	#name: "principal_name"
}

#folderKindQueryParameter: #queryStringParameter & {
	#name: "kind"
}

#folderSearchQueryParameter: #queryStringParameter & {
	#name: "q"
}

#requiredFolderSearchQueryParameter: {
	name:     "q"
	in:       "query"
	required: true
	explode:  false
	schema: {
		type: "string"
	}
}

#folderOwnerQueryParameter: #queryStringParameter & {
	#name: "owner"
}

#folderPathParameters: [
	#folderIDPathParameter,
]

#workspaceFolderPathParameters: [
	#folderWorkspaceIDPathParameter,
]

#folderSharePathParameters: [
	#folderIDPathParameter,
	#folderPrincipalNamePathParameter,
]

#workspaceFolderListParameters: [
	#folderWorkspaceIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#rootFolderListParameters: [
	#folderKindQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#rootFolderSearchParameters: [
	#requiredFolderSearchQueryParameter,
	#folderKindQueryParameter,
	#folderOwnerQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#folderContentsParameters: [
	#folderIDPathParameter,
	#folderKindQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#folderSearchParameters: [
	#folderIDPathParameter,
	#requiredFolderSearchQueryParameter,
	#folderKindQueryParameter,
	#folderOwnerQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#folderOps: [
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "listFolders"
		path:         "/workspaces/{workspace_id}/folders"
		summary:      "List folders in a workspace"
		returns:      "PaginatedFolders"
		error_family: "guarded_read"
		params:       #workspaceFolderListParameters
	},
	#plainFolderOperation & {
		kind:           "response"
		method:         "post"
		op:             "createFolder"
		path:           "/workspaces/{workspace_id}/folders"
		summary:        "Create folder in a workspace"
		returns:        "Folder"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #workspaceFolderPathParameters
		body_ref:       "CreateFolderRequest"
		body_description: "Request payload"
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "getFolder"
		path:         "/folders/{folder_id}"
		summary:      "Get folder"
		returns:      "Folder"
		error_family: "resource"
		params:       #folderPathParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateFolder"
		path:         "/folders/{folder_id}"
		summary:      "Update folder"
		returns:      "Folder"
		error_family: "resource"
		params:       #folderPathParameters
		body_ref:     "UpdateFolderRequest"
		body_description: "Request payload"
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "post"
		op:           "moveFolder"
		path:         "/folders/{folder_id}/moves"
		summary:      "Move folder"
		returns:      "Folder"
		error_family: "resource"
		params:       #folderPathParameters
		body_ref:     "MoveFolderRequest"
		body_description: "Request payload"
	},
	#plainFolderOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteFolder"
		path:         "/folders/{folder_id}"
		summary:      "Delete folder"
		error_family: "resource"
		params:       #folderPathParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "listFolderShares"
		path:         "/folders/{folder_id}/shares"
		summary:      "List folder shares"
		error_family: "resource"
		params:       #folderPathParameters
		success_schema: {
			type: "array"
			items: {
				ref: "FolderShare"
			}
		}
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "post"
		op:           "shareFolder"
		path:         "/folders/{folder_id}/shares"
		summary:      "Share folder"
		returns:      "FolderShare"
		error_family: "resource"
		params:       #folderPathParameters
		body_ref:     "ShareFolderRequest"
		body_description: "Request payload"
	},
	#plainFolderOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "unshareFolder"
		path:         "/folders/{folder_id}/shares/{principal_name}"
		summary:      "Remove folder share"
		error_family: "resource"
		params:       #folderSharePathParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "listRootFolderContents"
		path:         "/folders/contents"
		summary:      "List root folder contents"
		returns:      "PaginatedFolderContents"
		error_family: "standard"
		params:       #rootFolderListParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "searchRootFolderContents"
		path:         "/folders/search"
		summary:      "Search root folder namespace"
		returns:      "PaginatedFolderContents"
		error_family: "standard"
		params:       #rootFolderSearchParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "listFolderContents"
		path:         "/folders/{folder_id}/contents"
		summary:      "List folder contents"
		returns:      "PaginatedFolderContents"
		error_family: "standard"
		params:       #folderContentsParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "searchFolderContents"
		path:         "/folders/{folder_id}/search"
		summary:      "Search folder namespace"
		returns:      "PaginatedFolderContents"
		error_family: "standard"
		params:       #folderSearchParameters
	},
	#plainFolderOperation & {
		kind:         "response"
		method:       "get"
		op:           "getFolderPath"
		path:         "/folders/{folder_id}/path"
		summary:      "Get folder path"
		returns:      "FolderPath"
		error_family: "resource"
		params:       #folderPathParameters
	},
]

endpoints_folders: [
	for op in #folderOps {
		(#endpointFromGenericOperation & {
			tag:  #foldersTag
			spec: op
		}).endpoint
	},
]
