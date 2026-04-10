//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
)

type declarativeSupportMatrixEntry struct {
	Kind         string
	LoadValidate bool
	ReadExport   bool
	Diff         bool
	Apply        bool
	ReplanClean  bool
	Coverage     string
}

var declarativeSupportMatrix = []declarativeSupportMatrixEntry{
	{Kind: declarative.KindNamePrincipalList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "TestDeclarative_ValidateOnly, TestDeclarative_ApplyDeletes"},
	{Kind: declarative.KindNameGroupList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "TestDeclarative_PlanShowsGroupDescriptionUpdate, TestDeclarative_GroupDescriptionUpdateConverges"},
	{Kind: declarative.KindNameGrantList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "TestDeclarative_GrantsPlanAndApply"},
	{Kind: declarative.KindNamePrivilegePresetList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Compiled through declarative load/export path and grant lifecycle coverage"},
	{Kind: declarative.KindNameBindingList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Compiled through declarative load/export path and grant lifecycle coverage"},
	{Kind: declarative.KindNameAPIKeyList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "API key declarative client tests and strict read/export coverage"},
	{Kind: declarative.KindNameCatalog, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Catalog declarative client tests and export coverage"},
	{Kind: declarative.KindNameSchema, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Schema declarative client tests and export coverage"},
	{Kind: declarative.KindNameTable, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Table declarative client tests; destructive type changes remain blocking plan errors"},
	{Kind: declarative.KindNameView, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "View declarative client tests and export coverage"},
	{Kind: declarative.KindNameVolume, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Volume declarative client tests and export coverage"},
	{Kind: declarative.KindNameRowFilterList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Row filter diff and declarative client tests"},
	{Kind: declarative.KindNameColumnMaskList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Column mask diff and declarative client tests"},
	{Kind: declarative.KindNameTagConfig, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Tag and tag assignment declarative client tests"},
	{Kind: declarative.KindNameStorageCredentialList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Storage declarative client tests and strict read/export coverage"},
	{Kind: declarative.KindNameExternalLocationList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "External location declarative client tests and strict read/export coverage"},
	{Kind: declarative.KindNameComputeEndpointList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Compute declarative client tests and strict read/export coverage"},
	{Kind: declarative.KindNameComputeAssignmentList, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Compute assignment declarative client tests and strict read/export coverage"},
	{Kind: declarative.KindNameComputeRoutingDefaults, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Strict read/export coverage after compute defaults API wiring"},
	{Kind: declarative.KindNameDomain, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Domain strict read/export coverage and data product lifecycle tests"},
	{Kind: declarative.KindNameTeam, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Team strict read/export coverage and data product lifecycle tests"},
	{Kind: declarative.KindNameDataProduct, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Data product declarative client tests with version deletion convergence"},
	{Kind: declarative.KindNameNotebook, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "TestDeclarative_NotebookPublishRemovalConverges and notebook declarative client tests"},
	{Kind: declarative.KindNameDashboard, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Dashboard declarative loader/validator/diff tests, declarative client reconciliation tests, and dashboard runtime state integration coverage"},
	{Kind: declarative.KindNameAsset, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Asset declarative diff and client tests"},
	{Kind: declarative.KindNameModel, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Model declarative integration and client tests"},
	{Kind: declarative.KindNameSemanticModel, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Semantic model declarative integration and client tests"},
	{Kind: declarative.KindNameMacro, LoadValidate: true, ReadExport: true, Diff: true, Apply: true, ReplanClean: true, Coverage: "Macro declarative integration and client tests"},
}

func TestDeclarative_SupportMatrixCoversSchemaKinds(t *testing.T) {
	t.Parallel()

	matrixByKind := make(map[string]declarativeSupportMatrixEntry, len(declarativeSupportMatrix))
	for _, entry := range declarativeSupportMatrix {
		_, exists := matrixByKind[entry.Kind]
		require.Falsef(t, exists, "duplicate support matrix entry for %s", entry.Kind)
		matrixByKind[entry.Kind] = entry
	}

	for _, docType := range declarative.SchemaDocumentTypes() {
		entry, ok := matrixByKind[docType.Kind]
		require.Truef(t, ok, "missing declarative support matrix entry for schema kind %s", docType.Kind)
		assert.Truef(t, entry.LoadValidate, "expected load/validate coverage for %s", docType.Kind)
		assert.Truef(t, entry.ReadExport, "expected read/export coverage for %s", docType.Kind)
		assert.Truef(t, entry.Diff, "expected diff coverage for %s", docType.Kind)
		assert.Truef(t, entry.Apply, "expected apply coverage for %s", docType.Kind)
		assert.Truef(t, entry.ReplanClean, "expected re-plan coverage for %s", docType.Kind)
		assert.NotEmptyf(t, entry.Coverage, "expected coverage note for %s", docType.Kind)
	}

	assert.Len(t, declarativeSupportMatrix, len(declarative.SchemaDocumentTypes()))
}
