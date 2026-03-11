package architecture_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAuthorizationCoverage_SensitiveReadMethods(t *testing.T) {
	t.Helper()

	expects := []methodExpectation{
		{file: "internal/service/notebook/notebook.go", method: "GetNotebookForPrincipal", snippets: []string{"requireNotebookAccess("}},
		{file: "internal/service/notebook/notebook.go", method: "ListNotebooksForPrincipal", snippets: []string{"domain.ErrAccessDenied(", "s.repo.ListNotebooks(ctx, &ownerName, page)"}},
		{file: "internal/service/notebook/git.go", method: "GetGitRepoForPrincipal", snippets: []string{"requireGitRepoAccess("}},
		{file: "internal/service/notebook/git.go", method: "ListGitReposForPrincipal", snippets: []string{"if isAdmin {", "if repo.Owner == principal"}},
		{file: "internal/service/storage/credential.go", method: "GetByName", snippets: []string{"canReadOwnedResource(", "logAuditDenied("}},
		{file: "internal/service/storage/credential.go", method: "List", snippets: []string{"isAdmin(ctx)", "if cred.Owner == principal"}},
		{file: "internal/service/storage/external_location.go", method: "GetByName", snippets: []string{"canReadOwnedResource(", "logAuditDenied("}},
		{file: "internal/service/storage/external_location.go", method: "List", snippets: []string{"isAdmin(ctx)", "if location.Owner == principal"}},
		{file: "internal/service/catalog/registration.go", method: "List", snippets: []string{"requireAdmin(ctx, \"list catalog registrations\")"}},
		{file: "internal/service/catalog/registration.go", method: "Get", snippets: []string{"requireAdmin(ctx, \"get catalog registration\")"}},
		{file: "internal/service/catalog/registration.go", method: "Update", snippets: []string{"requireAdmin(ctx, \"update catalog registration\")"}},
		{file: "internal/service/catalog/registration.go", method: "Delete", snippets: []string{"requireAdmin(ctx, \"delete catalog registration\")"}},
		{file: "internal/service/catalog/registration.go", method: "SetDefault", snippets: []string{"requireAdmin(ctx, \"set default catalog\")"}},
	}

	for _, exp := range expects {
		body := methodBody(t, exp.file, exp.method)
		for _, snippet := range exp.snippets {
			if !containsAny(body, []string{snippet}) {
				t.Fatalf("governance: %s.%s must contain %q", exp.file, exp.method, snippet)
			}
		}
	}
}

func TestNotebookHandlers_UseParentScopedAndPrincipalScopedMethods(t *testing.T) {
	t.Helper()

	expects := []struct {
		method      string
		mustContain []string
		mustNotHave []string
	}{
		{method: "ListNotebooks", mustContain: []string{"ListNotebooksForPrincipal("}, mustNotHave: []string{"ListNotebooks(ctx,"}},
		{method: "GetNotebook", mustContain: []string{"GetNotebookForPrincipal("}, mustNotHave: []string{"GetNotebook(ctx,"}},
		{method: "CreateNotebookSession", mustContain: []string{"CreateSessionForNotebook("}, mustNotHave: []string{"CreateSession(ctx,"}},
		{method: "CloseNotebookSession", mustContain: []string{"CloseNotebookSession("}, mustNotHave: []string{"CloseSession(ctx,"}},
		{method: "ExecuteCell", mustContain: []string{"ExecuteNotebookCell("}, mustNotHave: []string{"ExecuteCell(ctx, req.SessionId, req.CellId"}},
		{method: "RunAllCells", mustContain: []string{"RunAllNotebook("}, mustNotHave: []string{"RunAll(ctx, req.SessionId"}},
		{method: "RunAllCellsAsync", mustContain: []string{"RunAllNotebookAsync("}, mustNotHave: []string{"RunAllAsync(ctx, req.SessionId"}},
		{method: "ListNotebookJobs", mustContain: []string{"ListNotebookJobs(ctx, req.NotebookId"}, mustNotHave: []string{"ListJobs(ctx, req.NotebookId"}},
		{method: "GetNotebookJob", mustContain: []string{"GetNotebookJob(ctx, req.NotebookId, req.JobId"}, mustNotHave: []string{"GetJob(ctx, req.JobId"}},
		{method: "ListGitRepos", mustContain: []string{"ListGitReposForPrincipal("}, mustNotHave: []string{"ListGitRepos(ctx,"}},
		{method: "GetGitRepo", mustContain: []string{"GetGitRepoForPrincipal("}, mustNotHave: []string{"GetGitRepo(ctx,"}},
	}

	for _, exp := range expects {
		body := methodBody(t, "internal/api/handler_notebooks.go", exp.method)
		for _, snippet := range exp.mustContain {
			if !containsAny(body, []string{snippet}) {
				t.Fatalf("governance: internal/api/handler_notebooks.go.%s must contain %q", exp.method, snippet)
			}
		}
		for _, snippet := range exp.mustNotHave {
			if containsAny(body, []string{snippet}) {
				t.Fatalf("governance: internal/api/handler_notebooks.go.%s must not contain %q", exp.method, snippet)
			}
		}
	}
}

func TestSemanticHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_semantic.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_semantic.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_semantic.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_semantic.go must not use ad hoc errors.As domain error switches")
	}
}

func TestNotebookHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_notebooks.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_notebooks.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_notebooks.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_notebooks.go must not use ad hoc errors.As domain error switches")
	}
}
