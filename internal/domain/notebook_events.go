package domain

const (
	// NotebookEventTypeInvalidateContext requests notebook runtime context invalidation.
	NotebookEventTypeInvalidateContext = "NOTEBOOK_INVALIDATE_CONTEXT"
	// NotebookEventPayloadNotebookID stores the invalidated notebook identifier.
	NotebookEventPayloadNotebookID = "notebook_id"
)
