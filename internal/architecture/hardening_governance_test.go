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

func TestServicePolicyHelpers_DelegateToSharedPolicyPackage(t *testing.T) {
	t.Helper()

	expects := []struct {
		file     string
		snippets []string
	}{
		{
			file:     "internal/service/security/helpers.go",
			snippets: []string{"internal/service/policy", "servicepolicy.RequireAdmin(ctx)", "servicepolicy.CallerName(ctx)"},
		},
		{
			file:     "internal/service/governance/helpers.go",
			snippets: []string{"internal/service/policy", "servicepolicy.RequireAdmin(ctx)"},
		},
		{
			file:     "internal/service/storage/access.go",
			snippets: []string{"internal/service/policy", "servicepolicy.IsAdmin(ctx)", "servicepolicy.CanReadOwnedResource(ctx, principal, owner)"},
		},
		{
			file:     "internal/service/catalog/registration.go",
			snippets: []string{"internal/service/policy", "servicepolicy.RequireAdminIfPresentForAction(ctx, action)"},
		},
	}

	for _, exp := range expects {
		body, err := os.ReadFile(filepath.Join(repoRootDir(), exp.file))
		if err != nil {
			t.Fatalf("read %s: %v", exp.file, err)
		}
		source := string(body)
		for _, snippet := range exp.snippets {
			if !containsAny(source, []string{snippet}) {
				t.Fatalf("governance: %s must contain %q", exp.file, snippet)
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

func TestStorageHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_storage.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_storage.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_storage.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_storage.go must not use ad hoc errors.As domain error switches")
	}
}

func TestAssetHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_assets.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_assets.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_assets.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_assets.go must not use ad hoc errors.As domain error switches")
	}
}

func TestPipelineHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_pipeline.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_pipeline.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_pipeline.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_pipeline.go must not use ad hoc errors.As domain error switches")
	}
}

func TestIngestionHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_ingestion.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_ingestion.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_ingestion.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_ingestion.go must not use ad hoc errors.As domain error switches")
	}
}

func TestAPIKeyHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_apikeys.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_apikeys.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_apikeys.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_apikeys.go must not use ad hoc errors.As domain error switches")
	}
}

func TestViewHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_views.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_views.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_views.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_views.go must not use ad hoc errors.As domain error switches")
	}
}

func TestQueryHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_query.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_query.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_query.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"case errors.As(err, new(*domain.NotFoundError)):", "case errors.As(err, new(*domain.AccessDeniedError)):", "case errors.As(err, new(*domain.ValidationError)):"}) {
		t.Fatal("governance: internal/api/handler_query.go must not use ad hoc domain error switches")
	}
}

func TestCatalogRegistrationHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_catalogs.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_catalogs.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_catalogs.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_catalogs.go must not use ad hoc errors.As domain error switches")
	}
}

func TestComputeHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_compute.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_compute.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_compute.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_compute.go must not use ad hoc errors.As domain error switches")
	}
}

func TestMacroHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_macros.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_macros.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_macros.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_macros.go must not use ad hoc errors.As domain error switches")
	}
}

func TestCatalogHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_catalog.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_catalog.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_catalog.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_catalog.go must not use ad hoc errors.As domain error switches")
	}
}

func TestGovernanceHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_governance.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_governance.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_governance.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_governance.go must not use ad hoc errors.As domain error switches")
	}
}

func TestModelHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_models.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_models.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_models.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_models.go must not use ad hoc errors.As domain error switches")
	}
}

func TestSecurityHandlers_UseSharedDomainErrorResponder(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "internal/api/handler_security.go"))
	if err != nil {
		t.Fatalf("read internal/api/handler_security.go: %v", err)
	}
	source := string(body)
	if !containsAny(source, []string{"respondDomainError["}) {
		t.Fatal("governance: internal/api/handler_security.go must use respondDomainError for domain error mapping")
	}
	if containsAny(source, []string{"errors.As(err,"}) {
		t.Fatal("governance: internal/api/handler_security.go must not use ad hoc errors.As domain error switches")
	}
}
