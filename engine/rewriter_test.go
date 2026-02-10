package engine

import (
	"strings"
	"testing"

	"duck-demo/policy"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	pbext "github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
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

func i32Type() *pb.Type {
	return &pb.Type{Kind: &pb.Type_I32_{I32: &pb.Type_I32{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
}

func stringType() *pb.Type {
	return &pb.Type{Kind: &pb.Type_String_{String_: &pb.Type_String{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
}

func fp64Type() *pb.Type {
	return &pb.Type{Kind: &pb.Type_Fp64{Fp64: &pb.Type_FP64{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
}

func boolType() *pb.Type {
	return &pb.Type{Kind: &pb.Type_Bool{Bool: &pb.Type_Boolean{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
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

// mustRewrite calls RewritePlan and fails the test on error.
func mustRewrite(t *testing.T, plan *pb.Plan, rules map[string][]policy.RLSRule) *pb.Plan {
	t.Helper()
	result, err := RewritePlan(plan, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return result
}

func TestNoRLSRulesLeavePlanUnchanged(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{}
	result := mustRewrite(t, plan, rules)

	// ReadRel should have no filter
	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	// The filter should be injected into the ReadRel's Filter field
	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	readRel := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	// The filter should be an AND of the existing filter and the new RLS filter
	read := getReadRel(t, result)
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
	result := mustRewrite(t, plan, rules)

	// Check that extension functions were registered
	foundEqual := false
	for _, ext := range result.Extensions {
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
	result := mustRewrite(t, plan, rules)

	// Structure should still be: Root -> Project -> Fetch -> Read (no FilterRel inserted)
	root := result.Relations[0].GetRoot()
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

func TestRewriteDoesNotMutateOriginal(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}
	result := mustRewrite(t, plan, rules)

	// Original plan should NOT have a filter
	origRead := getReadRel(t, plan)
	if origRead.Filter != nil {
		t.Error("original plan should not be modified by RewritePlan")
	}

	// Rewritten plan SHOULD have a filter
	rewrittenRead := getReadRel(t, result)
	if rewrittenRead.Filter == nil {
		t.Error("rewritten plan should have a filter")
	}
}

// --- Operator tests ---

func TestNotEqualOperator(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpNotEqual, Value: int64(3)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := getReadRel(t, result)
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	// Verify the registered function name contains "not_equal"
	found := false
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "not_equal:i64_i64" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'not_equal:i64_i64' extension function")
	}
}

func TestLessThanOperator(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpLessThan, Value: int64(3)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := getReadRel(t, result)
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	found := false
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "lt:i64_i64" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'lt:i64_i64' extension function")
	}
}

func TestLessEqualOperator(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpLessEqual, Value: int64(2)}},
	}
	result := mustRewrite(t, plan, rules)
	found := false
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "lte:i64_i64" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'lte:i64_i64' extension function")
	}
}

func TestGreaterThanOperator(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpGreaterThan, Value: int64(1)}},
	}
	result := mustRewrite(t, plan, rules)
	found := false
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "gt:i64_i64" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'gt:i64_i64' extension function")
	}
}

func TestGreaterEqualOperator(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpGreaterEqual, Value: int64(1)}},
	}
	result := mustRewrite(t, plan, rules)
	found := false
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == "gte:i64_i64" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'gte:i64_i64' extension function")
	}
}

// --- Additional type tests ---

func TestI32LiteralFilter(t *testing.T) {
	read := makeReadWithSchema("t",
		[]string{"col"},
		[]*pb.Type{i32Type()},
	)
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "col", Operator: policy.OpEqual, Value: int32(42)}},
	}
	result := mustRewrite(t, plan, rules)
	root := result.Relations[0].GetRoot()
	proj := root.Input.GetProject()
	fetch := proj.Input.GetFetch()
	readRel := fetch.Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetI32() != 42 {
		t.Errorf("expected i32(42), got %v", lit)
	}
}

func TestFp64LiteralFilter(t *testing.T) {
	read := makeReadWithSchema("t",
		[]string{"price"},
		[]*pb.Type{fp64Type()},
	)
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "price", Operator: policy.OpEqual, Value: float64(9.99)}},
	}
	result := mustRewrite(t, plan, rules)
	root := result.Relations[0].GetRoot()
	readRel := root.Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetFp64() != 9.99 {
		t.Errorf("expected fp64(9.99), got %v", lit.GetFp64())
	}
}

func TestBoolLiteralFilter(t *testing.T) {
	read := makeReadWithSchema("t",
		[]string{"active"},
		[]*pb.Type{boolType()},
	)
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "active", Operator: policy.OpEqual, Value: true}},
	}
	result := mustRewrite(t, plan, rules)
	root := result.Relations[0].GetRoot()
	readRel := root.Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if !lit.GetBoolean() {
		t.Errorf("expected boolean(true), got %v", lit.GetBoolean())
	}
}

func TestI16LiteralFilter(t *testing.T) {
	i16T := &pb.Type{Kind: &pb.Type_I16_{I16: &pb.Type_I16{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"small"}, []*pb.Type{i16T})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "small", Operator: policy.OpEqual, Value: int32(5)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetI16() != 5 {
		t.Errorf("expected i16(5), got %v", lit.GetI16())
	}
}

func TestI8LiteralFilter(t *testing.T) {
	i8T := &pb.Type{Kind: &pb.Type_I8_{I8: &pb.Type_I8{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"tiny"}, []*pb.Type{i8T})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "tiny", Operator: policy.OpEqual, Value: int32(1)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetI8() != 1 {
		t.Errorf("expected i8(1), got %v", lit.GetI8())
	}
}

func TestFp32LiteralFilter(t *testing.T) {
	fp32T := &pb.Type{Kind: &pb.Type_Fp32{Fp32: &pb.Type_FP32{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"temp"}, []*pb.Type{fp32T})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "temp", Operator: policy.OpEqual, Value: float32(3.14)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetFp32() != float32(3.14) {
		t.Errorf("expected fp32(3.14), got %v", lit.GetFp32())
	}
}

func TestDateLiteralFilter(t *testing.T) {
	dateT := &pb.Type{Kind: &pb.Type_Date_{Date: &pb.Type_Date{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"created"}, []*pb.Type{dateT})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	// 19723 = 2024-01-01 (days since epoch)
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "created", Operator: policy.OpGreaterEqual, Value: int32(19723)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetDate() != 19723 {
		t.Errorf("expected date(19723), got %v", lit.GetDate())
	}
}

func TestTimestampLiteralFilter(t *testing.T) {
	tsT := &pb.Type{Kind: &pb.Type_Timestamp_{Timestamp: &pb.Type_Timestamp{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"ts"}, []*pb.Type{tsT})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "ts", Operator: policy.OpEqual, Value: int64(1704067200000000)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	if lit.GetTimestamp() != 1704067200000000 {
		t.Errorf("expected timestamp(1704067200000000), got %v", lit.GetTimestamp())
	}
}

func TestVarCharLiteralFilter(t *testing.T) {
	vcT := &pb.Type{Kind: &pb.Type_Varchar{Varchar: &pb.Type_VarChar{Length: 100, Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"name"}, []*pb.Type{vcT})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "name", Operator: policy.OpEqual, Value: "alice"}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	lit := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral()
	vc := lit.GetVarChar()
	if vc == nil {
		t.Fatal("expected VarChar literal")
	}
	if vc.Value != "alice" {
		t.Errorf("expected 'alice', got %q", vc.Value)
	}
}

func TestDecimalLiteralFilter(t *testing.T) {
	decT := &pb.Type{Kind: &pb.Type_Decimal_{Decimal: &pb.Type_Decimal{Precision: 10, Scale: 2, Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"amount"}, []*pb.Type{decT})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	decVal := &DecimalValue{
		Value:     make([]byte, 16), // 0 value for simplicity
		Precision: 10,
		Scale:     2,
	}
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "amount", Operator: policy.OpEqual, Value: decVal}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Fatal("expected filter")
	}
	dec := readRel.Filter.GetScalarFunction().Arguments[1].GetValue().GetLiteral().GetDecimal()
	if dec == nil {
		t.Fatal("expected Decimal literal")
	}
	if dec.Precision != 10 || dec.Scale != 2 {
		t.Errorf("expected precision=10, scale=2, got precision=%d, scale=%d", dec.Precision, dec.Scale)
	}
}

// --- Error path tests ---

func TestUnsupportedOperatorError(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: "invalid_op", Value: int64(1)}},
	}
	_, err := RewritePlan(plan, rules)
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
	if !strings.Contains(err.Error(), "unsupported operator") {
		t.Errorf("expected 'unsupported operator' in error, got: %v", err)
	}
}

func TestUnsupportedColumnTypeError(t *testing.T) {
	// Use a type that buildComparisonComponents doesn't handle
	unknownType := &pb.Type{Kind: &pb.Type_Uuid{Uuid: &pb.Type_UUID{Nullability: pb.Type_NULLABILITY_NULLABLE}}}
	read := makeReadWithSchema("t", []string{"id"}, []*pb.Type{unknownType})
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "id", Operator: policy.OpEqual, Value: "some-uuid"}},
	}
	_, err := RewritePlan(plan, rules)
	if err == nil {
		t.Fatal("expected error for unsupported column type")
	}
	if !strings.Contains(err.Error(), "unsupported column type") {
		t.Errorf("expected 'unsupported column type' in error, got: %v", err)
	}
}

func TestColumnNotFoundError(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "nonexistent", Operator: policy.OpEqual, Value: int64(1)}},
	}
	_, err := RewritePlan(plan, rules)
	if err == nil {
		t.Fatal("expected error for column not found")
	}
	if !strings.Contains(err.Error(), "column \"nonexistent\" not found") {
		t.Errorf("expected column not found error, got: %v", err)
	}
}

func TestNilBaseSchemaError(t *testing.T) {
	// ReadRel with no BaseSchema
	read := &pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: []string{"t"},
					},
				},
				// BaseSchema intentionally nil
			},
		},
	}
	plan := makePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"t": {{Table: "t", Column: "col", Operator: policy.OpEqual, Value: int64(1)}},
	}
	_, err := RewritePlan(plan, rules)
	if err == nil {
		t.Fatal("expected error for nil base schema")
	}
	if !strings.Contains(err.Error(), "no base schema") {
		t.Errorf("expected 'no base schema' in error, got: %v", err)
	}
}

func TestValueTypeMismatchError(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))
	// Pclass is i64 but value is string
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: "wrong_type"}},
	}
	_, err := RewritePlan(plan, rules)
	if err == nil {
		t.Fatal("expected error for value type mismatch")
	}
	if !strings.Contains(err.Error(), "column type is i64 but value is") {
		t.Errorf("expected type mismatch error, got: %v", err)
	}
}

// --- Join rewriting tests ---

func TestRewriteThroughHashJoin(t *testing.T) {
	leftRead := makeReadWithSchema("left_tbl",
		[]string{"id", "val"},
		[]*pb.Type{i64Type(), i64Type()},
	)
	rightRead := makeReadWithSchema("right_tbl",
		[]string{"id", "score"},
		[]*pb.Type{i64Type(), i64Type()},
	)
	plan := makePlan(hashJoinRel(leftRead, rightRead))
	rules := map[string][]policy.RLSRule{
		"left_tbl":  {{Table: "left_tbl", Column: "val", Operator: policy.OpEqual, Value: int64(10)}},
		"right_tbl": {{Table: "right_tbl", Column: "score", Operator: policy.OpGreaterThan, Value: int64(50)}},
	}
	result := mustRewrite(t, plan, rules)

	hj := result.Relations[0].GetRoot().Input.GetHashJoin()
	if hj == nil {
		t.Fatal("expected HashJoinRel")
	}
	leftRd := hj.Left.GetRead()
	if leftRd == nil || leftRd.Filter == nil {
		t.Error("expected filter on left ReadRel inside HashJoin")
	}
	rightRd := hj.Right.GetRead()
	if rightRd == nil || rightRd.Filter == nil {
		t.Error("expected filter on right ReadRel inside HashJoin")
	}
}

func TestRewriteThroughMergeJoin(t *testing.T) {
	leftRead := makeReadWithSchema("tbl_a",
		[]string{"id", "x"},
		[]*pb.Type{i64Type(), i64Type()},
	)
	rightRead := makeReadWithSchema("tbl_b",
		[]string{"id", "y"},
		[]*pb.Type{i64Type(), i64Type()},
	)
	plan := makePlan(mergeJoinRel(leftRead, rightRead))
	rules := map[string][]policy.RLSRule{
		"tbl_a": {{Table: "tbl_a", Column: "x", Operator: policy.OpEqual, Value: int64(1)}},
	}
	result := mustRewrite(t, plan, rules)

	mj := result.Relations[0].GetRoot().Input.GetMergeJoin()
	if mj == nil {
		t.Fatal("expected MergeJoinRel")
	}
	leftRd := mj.Left.GetRead()
	if leftRd == nil || leftRd.Filter == nil {
		t.Error("expected filter on left ReadRel inside MergeJoin")
	}
	rightRd := mj.Right.GetRead()
	if rightRd == nil {
		t.Fatal("expected right ReadRel")
	}
	// right_tbl has no rules, so no filter
	if rightRd.Filter != nil {
		t.Error("right ReadRel should have no filter (no rules for tbl_b)")
	}
}

func TestRewriteThroughExtensionSingle(t *testing.T) {
	read := makeReadWithSchema("ext_tbl",
		[]string{"id", "val"},
		[]*pb.Type{i64Type(), i64Type()},
	)
	plan := makePlan(extensionSingleRel(read))
	rules := map[string][]policy.RLSRule{
		"ext_tbl": {{Table: "ext_tbl", Column: "val", Operator: policy.OpEqual, Value: int64(7)}},
	}
	result := mustRewrite(t, plan, rules)

	es := result.Relations[0].GetRoot().Input.GetExtensionSingle()
	if es == nil {
		t.Fatal("expected ExtensionSingleRel")
	}
	rd := es.Input.GetRead()
	if rd == nil || rd.Filter == nil {
		t.Error("expected filter on ReadRel inside ExtensionSingle")
	}
}

func TestRewriteThroughExtensionMulti(t *testing.T) {
	readA := makeReadWithSchema("multi_a",
		[]string{"id"},
		[]*pb.Type{i64Type()},
	)
	readB := makeReadWithSchema("multi_b",
		[]string{"id"},
		[]*pb.Type{i64Type()},
	)
	plan := makePlan(extensionMultiRel(readA, readB))
	rules := map[string][]policy.RLSRule{
		"multi_a": {{Table: "multi_a", Column: "id", Operator: policy.OpEqual, Value: int64(1)}},
		"multi_b": {{Table: "multi_b", Column: "id", Operator: policy.OpEqual, Value: int64(2)}},
	}
	result := mustRewrite(t, plan, rules)

	em := result.Relations[0].GetRoot().Input.GetExtensionMulti()
	if em == nil {
		t.Fatal("expected ExtensionMultiRel")
	}
	for i, input := range em.Inputs {
		rd := input.GetRead()
		if rd == nil || rd.Filter == nil {
			t.Errorf("expected filter on ReadRel %d inside ExtensionMulti", i)
		}
	}
}

func TestRewriteMultipleTablesWithDifferentRules(t *testing.T) {
	leftRead := makeReadWithSchema("orders",
		[]string{"id", "status"},
		[]*pb.Type{i64Type(), stringType()},
	)
	rightRead := makeReadWithSchema("products",
		[]string{"id", "category"},
		[]*pb.Type{i64Type(), stringType()},
	)
	plan := makePlan(joinRel(leftRead, rightRead))
	rules := map[string][]policy.RLSRule{
		"orders":   {{Table: "orders", Column: "status", Operator: policy.OpEqual, Value: "active"}},
		"products": {{Table: "products", Column: "category", Operator: policy.OpNotEqual, Value: "hidden"}},
	}
	result := mustRewrite(t, plan, rules)

	j := result.Relations[0].GetRoot().Input.GetJoin()
	if j == nil {
		t.Fatal("expected JoinRel")
	}
	ordersRd := j.Left.GetRead()
	if ordersRd == nil || ordersRd.Filter == nil {
		t.Error("expected filter on orders ReadRel")
	}
	productsRd := j.Right.GetRead()
	if productsRd == nil || productsRd.Filter == nil {
		t.Error("expected filter on products ReadRel")
	}
}

// --- Bare PlanRel_Rel test ---

func TestRewriteBarePlanRelRel(t *testing.T) {
	read := titanicRead()
	plan := makeBarePlan(projectRel(fetchRel(read, 10)))
	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)}},
	}
	result := mustRewrite(t, plan, rules)

	// Access through bare Rel path
	bareRel := result.Relations[0].GetRel()
	if bareRel == nil {
		t.Fatal("expected bare Rel in rewritten plan")
	}
	proj := bareRel.GetProject()
	if proj == nil {
		t.Fatal("expected ProjectRel")
	}
	fetch := proj.Input.GetFetch()
	if fetch == nil {
		t.Fatal("expected FetchRel")
	}
	rd := fetch.Input.GetRead()
	if rd == nil {
		t.Fatal("expected ReadRel")
	}
	if rd.Filter == nil {
		t.Error("expected filter on ReadRel in bare PlanRel_Rel")
	}
}

// --- Anchor allocation test ---

func TestAnchorAllocationWithExistingExtensions(t *testing.T) {
	read := titanicRead()
	plan := makePlan(projectRel(fetchRel(read, 10)))

	// Pre-populate with existing extensions at high anchor values
	plan.ExtensionUris = append(plan.ExtensionUris, &pbext.SimpleExtensionURI{
		ExtensionUriAnchor: 100,
		Uri:                "https://example.com/functions.yaml",
	})
	plan.Extensions = append(plan.Extensions, &pbext.SimpleExtensionDeclaration{
		MappingType: &pbext.SimpleExtensionDeclaration_ExtensionFunction_{
			ExtensionFunction: &pbext.SimpleExtensionDeclaration_ExtensionFunction{
				ExtensionUriReference: 100,
				FunctionAnchor:        200,
				Name:                  "existing_func",
			},
		},
	})

	rules := map[string][]policy.RLSRule{
		"titanic": {{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)}},
	}
	result := mustRewrite(t, plan, rules)

	// Verify no anchor collision
	anchors := map[uint32]string{}
	for _, ext := range result.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if existing, ok := anchors[ef.FunctionAnchor]; ok {
				t.Errorf("anchor collision: %d used by both %q and %q", ef.FunctionAnchor, existing, ef.Name)
			}
			anchors[ef.FunctionAnchor] = ef.Name
		}
	}
}

// --- Compound name in rewriter ---

func TestRewriteCompoundTableName(t *testing.T) {
	// ReadRel with compound name ["catalog", "schema", "titanic"]
	read := &pb.Rel{
		RelType: &pb.Rel_Read{
			Read: &pb.ReadRel{
				BaseSchema: &pb.NamedStruct{
					Names: []string{"id", "val"},
					Struct: &pb.Type_Struct{
						Types:       []*pb.Type{i64Type(), i64Type()},
						Nullability: pb.Type_NULLABILITY_REQUIRED,
					},
				},
				ReadType: &pb.ReadRel_NamedTable_{
					NamedTable: &pb.ReadRel_NamedTable{
						Names: []string{"catalog", "schema", "my_table"},
					},
				},
			},
		},
	}
	plan := makePlan(projectRel(fetchRel(read, 10)))
	// Rules keyed by the resolved table name (last element)
	rules := map[string][]policy.RLSRule{
		"my_table": {{Table: "my_table", Column: "val", Operator: policy.OpEqual, Value: int64(42)}},
	}
	result := mustRewrite(t, plan, rules)
	readRel := result.Relations[0].GetRoot().Input.GetProject().Input.GetFetch().Input.GetRead()
	if readRel.Filter == nil {
		t.Error("expected filter on ReadRel with compound table name")
	}
}

// --- Duplicate column names ---

func TestBuildColumnIndexFirstOccurrenceWins(t *testing.T) {
	schema := &pb.NamedStruct{
		Names: []string{"id", "name", "id"}, // duplicate "id"
		Struct: &pb.Type_Struct{
			Types: []*pb.Type{i64Type(), stringType(), i32Type()},
		},
	}
	idx := buildColumnIndex(schema)
	// First occurrence of "id" is at index 0, not 2
	if idx["id"] != 0 {
		t.Errorf("expected first occurrence index 0 for 'id', got %d", idx["id"])
	}
	if idx["name"] != 1 {
		t.Errorf("expected index 1 for 'name', got %d", idx["name"])
	}
}
