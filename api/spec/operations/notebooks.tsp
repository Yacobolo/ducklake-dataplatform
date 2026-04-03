using TypeSpec.Http;

namespace Duck;

@route("/notebooks")
@get
@tag("Notebooks")
@summary("List notebooks")
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebooks> | StandardErrorResponses;

@route("/notebooks")
@post
@tag("Notebooks")
@summary("Create notebook")
op createNotebook(@body body: CreateNotebookRequest): Created<Notebook> | MutatingErrorResponses;

@route("/notebooks/{notebook_id}")
@get
@tag("Notebooks")
@summary("Get notebook")
op getNotebook(notebook_id: string): Ok<NotebookDetail> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update notebook")
op updateNotebook(notebook_id: string, @body body: UpdateNotebookRequest): Ok<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/moves")
@post
@tag("Notebooks")
@summary("Move notebook")
op moveNotebook(notebook_id: string, @body body: MoveNotebookRequest): Ok<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/copies")
@post
@tag("Notebooks")
@summary("Duplicate notebook")
op duplicateNotebook(notebook_id: string, @body body: DuplicateNotebookRequest): Created<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/shares")
@get
@tag("Notebooks")
@summary("List notebook shares")
op listNotebookShares(notebook_id: string): Ok<NotebookShare[]> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/shares")
@post
@tag("Notebooks")
@summary("Share notebook")
op shareNotebook(notebook_id: string, @body body: ShareNotebookRequest): Ok<NotebookShare> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/shares/{principal_name}")
@delete
@tag("Notebooks")
@summary("Remove notebook share")
op unshareNotebook(notebook_id: string, principal_name: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebook_id}")
@delete
@tag("Notebooks")
@summary("Delete notebook")
op deleteNotebook(notebook_id: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/cells")
@post
@tag("Notebooks")
@summary("Create cell")
op createCell(notebook_id: string, @body body: CreateCellRequest): Created<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebook_id}/cells/reorder")
@post
@tag("Notebooks")
@summary("Reorder cells")
op reorderCells(notebook_id: string, @body body: ReorderCellsRequest): Ok<CellList> | ValidationResourceErrorResponses;

@route("/notebooks/{notebook_id}/cells/{cell_id}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update cell")
op updateCell(notebook_id: string, cell_id: string, @body body: UpdateCellRequest): Ok<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebook_id}/cells/{cell_id}")
@delete
@tag("Notebooks")
@summary("Delete cell")
op deleteCell(notebook_id: string, cell_id: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/sessions")
@post
@tag("Notebooks")
@summary("Create notebook session")
op createNotebookSession(notebook_id: string): Created<NotebookSession> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/sessions/{session_id}")
@delete
@tag("Notebooks")
@summary("Close notebook session")
op closeNotebookSession(notebook_id: string, session_id: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/sessions/{session_id}/cell-executions/{cell_id}")
@post
@tag("Notebooks")
@summary("Execute cell")
op executeCell(notebook_id: string, session_id: string, cell_id: string): Ok<CellExecutionResult> | ValidationResourceErrorResponses;

@route("/notebooks/{notebook_id}/sessions/{session_id}/cell-executions")
@post
@tag("Notebooks")
@summary("Run all cells")
op runAllCells(notebook_id: string, session_id: string): Ok<RunAllResult> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/sessions/{session_id}/job-runs")
@post
@tag("Notebooks")
@summary("Run all cells asynchronously")
op runAllCellsAsync(notebook_id: string, session_id: string): Accepted<NotebookJob> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/jobs")
@get
@tag("Notebooks")
@summary("List notebook jobs")
op listNotebookJobs(notebook_id: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebookJobs> | ResourceErrorResponses;

@route("/notebooks/{notebook_id}/jobs/{job_id}")
@get
@tag("Notebooks")
@summary("Get notebook job")
op getNotebookJob(notebook_id: string, job_id: string): Ok<NotebookJob> | ResourceErrorResponses;
