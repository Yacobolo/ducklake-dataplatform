using TypeSpec.Http;

namespace Duck;

@route("/notebooks")
@get
@tag("Notebooks")
@summary("List notebooks")
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebooks> | StandardErrorResponses;

@route("/notebooks/folders")
@get
@tag("Notebooks")
@summary("List notebook folders")
op listNotebookFolders(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedFolders> | StandardErrorResponses;

@route("/notebooks/folders")
@post
@tag("Notebooks")
@summary("Create notebook folder")
op createNotebookFolder(@body body: CreateFolderRequest): Created<Folder> | MutatingErrorResponses;

@route("/notebooks/folders/{folderId}")
@get
@tag("Notebooks")
@summary("Get notebook folder")
op getNotebookFolder(folderId: string): Ok<Folder> | ResourceErrorResponses;

@route("/notebooks/folders/{folderId}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update notebook folder")
op updateNotebookFolder(folderId: string, @body body: UpdateFolderRequest): Ok<Folder> | ResourceErrorResponses;

@route("/notebooks/folders/{folderId}")
@delete
@tag("Notebooks")
@summary("Delete notebook folder")
op deleteNotebookFolder(folderId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/folders/{folderId}/shares")
@get
@tag("Notebooks")
@summary("List notebook folder shares")
op listNotebookFolderShares(folderId: string): Ok<FolderShare[]> | ResourceErrorResponses;

@route("/notebooks/folders/{folderId}/shares")
@post
@tag("Notebooks")
@summary("Share notebook folder")
op shareNotebookFolder(folderId: string, @body body: ShareFolderRequest): Ok<FolderShare> | ResourceErrorResponses;

@route("/notebooks/folders/{folderId}/shares/{principalName}")
@delete
@tag("Notebooks")
@summary("Remove notebook folder share")
op unshareNotebookFolder(folderId: string, principalName: string): NoContent | ResourceErrorResponses;

@route("/notebooks")
@post
@tag("Notebooks")
@summary("Create notebook")
op createNotebook(@body body: CreateNotebookRequest): Created<Notebook> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@get
@tag("Notebooks")
@summary("Get notebook")
op getNotebook(notebookId: string): Ok<NotebookDetail> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update notebook")
op updateNotebook(notebookId: string, @body body: UpdateNotebookRequest): Ok<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/move")
@post
@tag("Notebooks")
@summary("Move notebook")
op moveNotebook(notebookId: string, @body body: MoveNotebookRequest): Ok<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/duplicate")
@post
@tag("Notebooks")
@summary("Duplicate notebook")
op duplicateNotebook(notebookId: string, @body body: DuplicateNotebookRequest): Created<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/shares")
@get
@tag("Notebooks")
@summary("List notebook shares")
op listNotebookShares(notebookId: string): Ok<NotebookShare[]> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/shares")
@post
@tag("Notebooks")
@summary("Share notebook")
op shareNotebook(notebookId: string, @body body: ShareNotebookRequest): Ok<NotebookShare> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/shares/{principalName}")
@delete
@tag("Notebooks")
@summary("Remove notebook share")
op unshareNotebook(notebookId: string, principalName: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@delete
@tag("Notebooks")
@summary("Delete notebook")
op deleteNotebook(notebookId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/cells")
@post
@tag("Notebooks")
@summary("Create cell")
op createCell(notebookId: string, @body body: CreateCellRequest): Created<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/reorder")
@post
@tag("Notebooks")
@summary("Reorder cells")
op reorderCells(notebookId: string, @body body: ReorderCellsRequest): Ok<CellList> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update cell")
op updateCell(notebookId: string, cellId: string, @body body: UpdateCellRequest): Ok<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
@tag("Notebooks")
@summary("Delete cell")
op deleteCell(notebookId: string, cellId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions")
@post
@tag("Notebooks")
@summary("Create notebook session")
op createNotebookSession(notebookId: string): Created<NotebookSession> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
@tag("Notebooks")
@summary("Close notebook session")
op closeNotebookSession(notebookId: string, sessionId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
@tag("Notebooks")
@summary("Execute cell")
op executeCell(notebookId: string, sessionId: string, cellId: string): Ok<CellExecutionResult> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
@tag("Notebooks")
@summary("Run all cells")
op runAllCells(notebookId: string, sessionId: string): Ok<RunAllResult> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
@tag("Notebooks")
@summary("Run all cells asynchronously")
op runAllCellsAsync(notebookId: string, sessionId: string): Accepted<NotebookJob> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs")
@get
@tag("Notebooks")
@summary("List notebook jobs")
op listNotebookJobs(notebookId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebookJobs> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
@tag("Notebooks")
@summary("Get notebook job")
op getNotebookJob(notebookId: string, jobId: string): Ok<NotebookJob> | ResourceErrorResponses;
