package engine

import (
	"testing"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
)

// Helper to build a minimal ReadRel with a NamedTable.
func namedTableRead(name string) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: []string{name},
					},
				},
			},
		},
	}
}

// namedTableReadCompound creates a ReadRel with a compound NamedTable identifier.
func namedTableReadCompound(names ...string) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: names,
					},
				},
			},
		},
	}
}

// Helper to wrap a Rel in a ProjectRel.
func projectRel(input *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Project{
			Project: &pb.ProjectRel{
				Input: input,
			},
		},
	}
}

// Helper to wrap a Rel in a FilterRel.
func filterRel(input *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Filter{
			Filter: &pb.FilterRel{
				Input: input,
			},
		},
	}
}

// Helper to wrap a Rel in a FetchRel (LIMIT).
func fetchRel(input *pb.Rel, count int64) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Fetch{
			Fetch: &pb.FetchRel{
				Input: input,
				CountMode: &pb.FetchRel_Count{
					Count: count,
				},
			},
		},
	}
}

// Helper to create a JoinRel over two inputs.
func joinRel(left, right *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Join{
			Join: &pb.JoinRel{
				Left:  left,
				Right: right,
			},
		},
	}
}

// Helper to create a SortRel.
func sortRel(input *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Sort{
			Sort: &pb.SortRel{
				Input: input,
			},
		},
	}
}

// Helper to create an AggregateRel.
func aggregateRel(input *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Aggregate{
			Aggregate: &pb.AggregateRel{
				Input: input,
			},
		},
	}
}

// Helper to create a CrossRel.
func crossRel(left, right *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Cross{
			Cross: &pb.CrossRel{
				Left:  left,
				Right: right,
			},
		},
	}
}

// Helper to create a HashJoinRel.
func hashJoinRel(left, right *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_HashJoin{
			HashJoin: &pb.HashJoinRel{
				Left:  left,
				Right: right,
			},
		},
	}
}

// Helper to create a MergeJoinRel.
func mergeJoinRel(left, right *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_MergeJoin{
			MergeJoin: &pb.MergeJoinRel{
				Left:  left,
				Right: right,
			},
		},
	}
}

// Helper to create a SetRel with multiple inputs.
func setRel(inputs ...*pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Set{
			Set: &pb.SetRel{
				Inputs: inputs,
			},
		},
	}
}

// Helper to create an ExtensionSingleRel.
func extensionSingleRel(input *pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_ExtensionSingle{
			ExtensionSingle: &pb.ExtensionSingleRel{
				Input: input,
			},
		},
	}
}

// Helper to create an ExtensionMultiRel.
func extensionMultiRel(inputs ...*pb.Rel) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_ExtensionMulti{
			ExtensionMulti: &pb.ExtensionMultiRel{
				Inputs: inputs,
			},
		},
	}
}

// Helper to build a plan with a single root relation.
func makePlan(input *pb.Rel) *pb.Plan {
	return &pb.Plan{
		Relations: []*pb.PlanRel{
			{
				RelType: &pb.PlanRel_Root{
					Root: &pb.RelRoot{
						Input: input,
					},
				},
			},
		},
	}
}

// makeBarePlan builds a plan with a bare PlanRel_Rel (not wrapped in Root).
func makeBarePlan(input *pb.Rel) *pb.Plan {
	return &pb.Plan{
		Relations: []*pb.PlanRel{
			{
				RelType: &pb.PlanRel_Rel{
					Rel: input,
				},
			},
		},
	}
}

// --- Existing tests ---

func TestExtractSingleNamedTable(t *testing.T) {
	plan := makePlan(projectRel(fetchRel(namedTableRead("titanic"), 10)))

	tables := ExtractTableNames(plan)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %v", len(tables), tables)
	}
	if tables[0] != "titanic" {
		t.Errorf("expected 'titanic', got %q", tables[0])
	}
}

func TestExtractMultipleTablesFromJoin(t *testing.T) {
	plan := makePlan(
		projectRel(
			joinRel(namedTableRead("titanic"), namedTableRead("passengers")),
		),
	)

	tables := ExtractTableNames(plan)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}

	found := map[string]bool{}
	for _, tbl := range tables {
		found[tbl] = true
	}
	if !found["titanic"] || !found["passengers"] {
		t.Errorf("expected titanic and passengers, got %v", tables)
	}
}

func TestExtractFromNestedRelations(t *testing.T) {
	// project -> filter -> fetch -> read
	plan := makePlan(
		projectRel(
			filterRel(
				fetchRel(namedTableRead("deeply_nested"), 100),
			),
		),
	)

	tables := ExtractTableNames(plan)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %v", len(tables), tables)
	}
	if tables[0] != "deeply_nested" {
		t.Errorf("expected 'deeply_nested', got %q", tables[0])
	}
}

func TestEmptyPlanReturnsNoTables(t *testing.T) {
	plan := &pb.Plan{}
	tables := ExtractTableNames(plan)
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d: %v", len(tables), tables)
	}
}

func TestDeduplicateTableNames(t *testing.T) {
	// Self-join: same table appears twice
	plan := makePlan(
		joinRel(namedTableRead("titanic"), namedTableRead("titanic")),
	)

	tables := ExtractTableNames(plan)
	if len(tables) != 1 {
		t.Errorf("expected 1 unique table, got %d: %v", len(tables), tables)
	}
}

// --- New tests for all Rel types ---

func TestExtractFromSort(t *testing.T) {
	plan := makePlan(sortRel(namedTableRead("orders")))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "orders" {
		t.Errorf("expected [orders], got %v", tables)
	}
}

func TestExtractFromAggregate(t *testing.T) {
	plan := makePlan(aggregateRel(namedTableRead("sales")))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "sales" {
		t.Errorf("expected [sales], got %v", tables)
	}
}

func TestExtractFromCross(t *testing.T) {
	plan := makePlan(crossRel(namedTableRead("a"), namedTableRead("b")))
	tables := ExtractTableNames(plan)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	found := map[string]bool{}
	for _, tbl := range tables {
		found[tbl] = true
	}
	if !found["a"] || !found["b"] {
		t.Errorf("expected a and b, got %v", tables)
	}
}

func TestExtractFromHashJoin(t *testing.T) {
	plan := makePlan(hashJoinRel(namedTableRead("left_tbl"), namedTableRead("right_tbl")))
	tables := ExtractTableNames(plan)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	found := map[string]bool{}
	for _, tbl := range tables {
		found[tbl] = true
	}
	if !found["left_tbl"] || !found["right_tbl"] {
		t.Errorf("expected left_tbl and right_tbl, got %v", tables)
	}
}

func TestExtractFromMergeJoin(t *testing.T) {
	plan := makePlan(mergeJoinRel(namedTableRead("tbl_x"), namedTableRead("tbl_y")))
	tables := ExtractTableNames(plan)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	found := map[string]bool{}
	for _, tbl := range tables {
		found[tbl] = true
	}
	if !found["tbl_x"] || !found["tbl_y"] {
		t.Errorf("expected tbl_x and tbl_y, got %v", tables)
	}
}

func TestExtractFromSet(t *testing.T) {
	plan := makePlan(setRel(
		namedTableRead("union_a"),
		namedTableRead("union_b"),
		namedTableRead("union_c"),
	))
	tables := ExtractTableNames(plan)
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d: %v", len(tables), tables)
	}
}

func TestExtractFromExtensionSingle(t *testing.T) {
	plan := makePlan(extensionSingleRel(namedTableRead("ext_table")))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "ext_table" {
		t.Errorf("expected [ext_table], got %v", tables)
	}
}

func TestExtractFromExtensionMulti(t *testing.T) {
	plan := makePlan(extensionMultiRel(
		namedTableRead("multi_a"),
		namedTableRead("multi_b"),
	))
	tables := ExtractTableNames(plan)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
}

// --- Edge case tests ---

func TestNilPlanReturnsNil(t *testing.T) {
	tables := ExtractTableNames(nil)
	if tables != nil {
		t.Errorf("expected nil, got %v", tables)
	}
}

func TestNilRelInsidePlanRoot(t *testing.T) {
	plan := &pb.Plan{
		Relations: []*pb.PlanRel{
			{
				RelType: &pb.PlanRel_Root{
					Root: &pb.RelRoot{Input: nil},
				},
			},
		},
	}
	tables := ExtractTableNames(plan)
	if len(tables) != 0 {
		t.Errorf("expected 0 tables for nil rel, got %v", tables)
	}
}

func TestExtractFromBarePlanRel(t *testing.T) {
	plan := makeBarePlan(projectRel(namedTableRead("bare_table")))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "bare_table" {
		t.Errorf("expected [bare_table], got %v", tables)
	}
}

func TestCompoundNamedTableNames(t *testing.T) {
	// NamedTable with compound identifier: ["catalog", "schema", "titanic"]
	plan := makePlan(namedTableReadCompound("mydb", "public", "titanic"))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %v", len(tables), tables)
	}
	if tables[0] != "titanic" {
		t.Errorf("expected 'titanic' (last element), got %q", tables[0])
	}
}

func TestSingleElementNames(t *testing.T) {
	plan := makePlan(namedTableReadCompound("simple"))
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "simple" {
		t.Errorf("expected [simple], got %v", tables)
	}
}

func TestEmptyNamesSkipped(t *testing.T) {
	// NamedTable with empty Names slice
	plan := makePlan(&pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: []string{},
					},
				},
			},
		},
	})
	tables := ExtractTableNames(plan)
	if len(tables) != 0 {
		t.Errorf("expected 0 tables for empty names, got %v", tables)
	}
}

func TestDeeplyNestedAllRelTypes(t *testing.T) {
	// sort -> aggregate -> project -> filter -> fetch -> read
	plan := makePlan(
		sortRel(
			aggregateRel(
				projectRel(
					filterRel(
						fetchRel(namedTableRead("deep"), 10),
					),
				),
			),
		),
	)
	tables := ExtractTableNames(plan)
	if len(tables) != 1 || tables[0] != "deep" {
		t.Errorf("expected [deep], got %v", tables)
	}
}

func TestResolveTableName(t *testing.T) {
	tests := []struct {
		names    []string
		expected string
	}{
		{[]string{"titanic"}, "titanic"},
		{[]string{"catalog", "schema", "table"}, "table"},
		{[]string{"a", "b"}, "b"},
		{[]string{}, ""},
		{nil, ""},
	}
	for _, tt := range tests {
		got := resolveTableName(tt.names)
		if got != tt.expected {
			t.Errorf("resolveTableName(%v) = %q, want %q", tt.names, got, tt.expected)
		}
	}
}
