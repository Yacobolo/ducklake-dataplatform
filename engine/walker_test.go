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
