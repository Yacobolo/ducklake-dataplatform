package engine

import (
	"testing"

	"duck-demo/policy"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
)

// makeReadWithSchema creates a ReadRel with a NamedTable and a base schema.
func makeReadWithSchema(tableName string, colNames []string, colTypes []*pb.Type) *pb.Rel {
	return &pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				BaseSchema: &pb.NamedStruct{
					Names: colNames,
					Struct: &pb.Type_Struct{
						Types:       colTypes,
						Nullability: pb.Type_NULLABILITY_REQUIRED,
					},
				},
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: []string{tableName},
					},
				},
			},
		},
	}
}

func i64Type() *pb.Type {
	return &pb.Type{Kind: &pb.Type_I64_{I64: &pb.Type_I64{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
}

func stringType() *pb.Type {
	return &pb.Type{Kind: &pb.Type_String_{String_: &pb.Type_String{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
}

// titanicRead creates a simplified titanic ReadRel with relevant columns.
func titanicRead() *pb.Rel {
	return makeReadWithSchema("titanic",
		[]string{"PassengerId", "Survived", "Pclass", "Name", "Sex"},
		[]*pb.Type{i64Type(), i64Type(), i64Type(), stringType(), stringType()},
	)
}

// getReadRel extracts the ReadRel from a plan's root -> project -> fetch -> read chain.
func getReadRel(t *testing.T, plan *pb.Plan) *pb.ReadRel {
	t.Helper()
	root := plan.Relations[0].GetRoot()
	proj := root.Input.GetProject()
	fetch := proj.Input.GetFetch()
	read := fetch.Input.GetRead()
	if read == nil {
		t.Fatal("expected ReadRel at fetch level")
	}
	return read
}

func TestNoRLSRulesLeavePlanUnchanged(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ReadRel should have no filter
	readRel := getReadRel(t, plan)
	if readRel.Filter != nil {
		t.Error("expected no filter on ReadRel when no RLS rules")
	}
}

func TestInjectFilterIntoReadRel(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The filter should be injected into the ReadRel's Filter field
	readRel := getReadRel(t, plan)
	if readRel.Filter == nil {
		t.Fatal("expected filter on ReadRel")
	}
}

func TestFilterConditionMatchesRLSRule(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	readRel := getReadRel(t, plan)
	cond := readRel.Filter
	if cond == nil {
		t.Fatal("expected filter condition")
	}

	// The condition should be a scalar function (equal)
	sf := cond.GetScalarFunction()
	if sf == nil {
		t.Fatal("expected scalar function in filter condition")
	}

	// Should have 2 arguments: field_ref and literal
	if len(sf.Arguments) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(sf.Arguments))
	}

	// First arg should be a field reference to Pclass (index 2)
	fieldRef := sf.Arguments[0].GetValue().GetSelection()
	if fieldRef == nil {
		t.Fatal("expected field reference as first argument")
	}
	fieldIdx := fieldRef.GetDirectReference().GetStructField().GetField()
	if fieldIdx != 2 {
		t.Errorf("expected field index 2 (Pclass), got %d", fieldIdx)
	}

	// Second arg should be literal i64(1)
	lit := sf.Arguments[1].GetValue().GetLiteral()
	if lit == nil {
		t.Fatal("expected literal as second argument")
	}
	if lit.GetI64() != 1 {
		t.Errorf("expected literal value 1, got %d", lit.GetI64())
	}
}

func TestMultipleRLSRulesProduceAndCondition(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
			{Table: "titanic", Column: "Survived", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	readRel := getReadRel(t, plan)
	cond := readRel.Filter

	// The top-level condition should be an AND of two scalar functions
	sf := cond.GetScalarFunction()
	if sf == nil {
		t.Fatal("expected scalar function (and) at top level")
	}
	if len(sf.Arguments) != 2 {
		t.Fatalf("expected 2 arguments in AND, got %d", len(sf.Arguments))
	}

	// Each argument should be a scalar function (equal)
	for i, arg := range sf.Arguments {
		inner := arg.GetValue().GetScalarFunction()
		if inner == nil {
			t.Errorf("argument %d: expected scalar function", i)
		}
	}
}

func TestStringLiteralFilter(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Sex", Operator: policy.OpEqual, Value: "female"},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	readRel := getReadRel(t, plan)
	sf := readRel.Filter.GetScalarFunction()
	if sf == nil {
		t.Fatal("expected scalar function in filter")
	}

	// First arg: field ref to Sex (index 4)
	fieldIdx := sf.Arguments[0].GetValue().GetSelection().GetDirectReference().GetStructField().GetField()
	if fieldIdx != 4 {
		t.Errorf("expected field index 4 (Sex), got %d", fieldIdx)
	}

	// Second arg: string literal "female"
	lit := sf.Arguments[1].GetValue().GetLiteral()
	if lit == nil {
		t.Fatal("expected literal")
	}
	if lit.GetString_() != "female" {
		t.Errorf("expected 'female', got %q", lit.GetString_())
	}
}

func TestInt64LiteralFilter(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	readRel := getReadRel(t, plan)
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetI64() != 1 {
		t.Errorf("expected i64(1), got %v", lit)
	}
}

func TestPreservesExistingFilters(t *testing.T) {
	// Create a read with an existing filter
	readRel := titanicRead()
	readInner := readRel.GetRead()
	existingFilter := &pb.Expression{
		RexType: &pb.Expression_Literal_{
			Literal: &pb.Expression_Literal{
				LiteralType: &pb.Expression_Literal_Boolean{Boolean: true},
			},
		},
	}
	readInner.Filter = existingFilter

	plan := makePlan(projectRel(fetchRel(readRel, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The filter should be an AND of the existing filter and the new RLS filter
	read := getReadRel(t, plan)
	if read.Filter == nil {
		t.Fatal("expected filter on ReadRel")
	}

	// Should be an AND combining both
	andFunc := read.Filter.GetScalarFunction()
	if andFunc == nil {
		t.Fatal("expected AND scalar function combining existing and RLS filters")
	}
	if len(andFunc.Arguments) != 2 {
		t.Fatalf("expected 2 arguments in AND, got %d", len(andFunc.Arguments))
	}

	// First arg should be the original boolean literal
	origLit := andFunc.Arguments[0].GetValue().GetLiteral()
	if origLit == nil || !origLit.GetBoolean() {
		t.Error("first argument should be the original boolean(true) filter")
	}

	// Second arg should be the RLS equal function
	rlsFunc := andFunc.Arguments[1].GetValue().GetScalarFunction()
	if rlsFunc == nil {
		t.Error("second argument should be the RLS scalar function")
	}
}

func TestExtensionFunctionRegistered(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that extension functions were registered
	foundEqual := false
	for _, ext := range plan.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "equal:i64_i64" {
				foundEqual = true
			}
		}
	}
	if !foundEqual {
		t.Error("expected 'equal:i64_i64' extension function to be registered")
	}
}

func TestFilterInjectedIntoReadNotAbove(t *testing.T) {
	// Verify the filter is set on the ReadRel itself, not as a FilterRel wrapper
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Structure should still be: Root -> Project -> Fetch -> Read (no FilterRel inserted)
	root := plan.Relations[0].GetRoot()
	proj := root.Input.GetProject()
	fetch := proj.Input.GetFetch()

	// Should NOT have a FilterRel wrapper
	if fetch.Input.GetFilter() != nil {
		t.Error("should not have a FilterRel wrapper; filter should be injected into ReadRel.Filter")
	}

	// Should have ReadRel directly
	readRel := fetch.Input.GetRead()
	if readRel == nil {
		t.Fatal("expected ReadRel directly under FetchRel")
	}

	// ReadRel should have the filter
	if readRel.Filter == nil {
		t.Error("expected filter on ReadRel.Filter")
	}
}
