package engine

import (
	"fmt"

	"duck-demo/policy"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	pbext "github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/proto"
)

const (
	// DuckDB's extension URI prefix for substrait functions.
	extensionURIBase    = "https://github.com/substrait-io/substrait/blob/main/extensions/"
	extensionURIBoolean = "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml"
)

// DecimalValue represents a Substrait decimal literal.
// Value is a 16-byte little-endian two's-complement integer.
type DecimalValue struct {
	Value     []byte // 16 bytes, little-endian two's complement
	Precision int32
	Scale     int32
}

// RewritePlan clones the plan and traverses the copy, injecting filter
// expressions into ReadRel nodes according to the given RLS rules
// (keyed by table name). The original plan is never modified.
// Returns the rewritten plan, or the original plan if no rules apply.
func RewritePlan(plan *pb.Plan, rulesByTable map[string][]policy.RLSRule) (*pb.Plan, error) {
	if len(rulesByTable) == 0 {
		return plan, nil
	}

	// Clone so we never mutate the original
	planCopy := proto.Clone(plan).(*pb.Plan)

	// Track the next available function anchor
	maxAnchor := findMaxFuncAnchor(planCopy)
	anchorAlloc := &anchorAllocator{next: maxAnchor + 1}

	for _, rel := range planCopy.GetRelations() {
		if root := rel.GetRoot(); root != nil {
			newInput, err := rewriteRel(root.GetInput(), rulesByTable, planCopy, anchorAlloc)
			if err != nil {
				return nil, err
			}
			root.Input = newInput
		}
		if bareRel := rel.GetRel(); bareRel != nil {
			newRel, err := rewriteRel(bareRel, rulesByTable, planCopy, anchorAlloc)
			if err != nil {
				return nil, err
			}
			rel.RelType = &pb.PlanRel_Rel{Rel: newRel}
		}
	}

	return planCopy, nil
}

type anchorAllocator struct {
	next uint32
}

func (a *anchorAllocator) alloc() uint32 {
	v := a.next
	a.next++
	return v
}

// findMaxFuncAnchor finds the highest function anchor in the plan's extensions.
func findMaxFuncAnchor(plan *pb.Plan) uint32 {
	var max uint32
	for _, ext := range plan.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.FunctionAnchor > max {
				max = ef.FunctionAnchor
			}
		}
	}
	return max
}

// findOrCreateExtensionURI finds an existing extension URI or creates one.
func findOrCreateExtensionURI(plan *pb.Plan, uri string, anchorAlloc *anchorAllocator) uint32 {
	for _, u := range plan.ExtensionUris {
		if u.Uri == uri {
			return u.ExtensionUriAnchor
		}
	}
	anchor := anchorAlloc.alloc()
	plan.ExtensionUris = append(plan.ExtensionUris, &pbext.SimpleExtensionURI{
		ExtensionUriAnchor: anchor,
		Uri:                uri,
	})
	return anchor
}

// registerFunction registers an extension function and returns its anchor.
func registerFunction(plan *pb.Plan, name string, uriRef uint32, anchorAlloc *anchorAllocator) uint32 {
	// Check if already registered
	for _, ext := range plan.Extensions {
		if ef := ext.GetExtensionFunction(); ef != nil {
			if ef.Name == name && ef.ExtensionUriReference == uriRef {
				return ef.FunctionAnchor
			}
		}
	}

	anchor := anchorAlloc.alloc()
	plan.Extensions = append(plan.Extensions, &pbext.SimpleExtensionDeclaration{
		MappingType: &pbext.SimpleExtensionDeclaration_ExtensionFunction_{
			ExtensionFunction: &pbext.SimpleExtensionDeclaration_ExtensionFunction{
				ExtensionUriReference: uriRef,
				FunctionAnchor:        anchor,
				Name:                  name,
			},
		},
	})
	return anchor
}

// rewriteRel recursively traverses and rewrites Rel nodes, injecting
// FilterRel around ReadRel nodes that have matching RLS rules.
func rewriteRel(rel *pb.Rel, rulesByTable map[string][]policy.RLSRule, plan *pb.Plan, aa *anchorAllocator) (*pb.Rel, error) {
	if rel == nil {
		return nil, nil
	}

	switch r := rel.RelType.(type) {
	case *pb.Rel_Read:
		return rewriteRead(rel, r.Read, rulesByTable, plan, aa)

	case *pb.Rel_Project:
		newInput, err := rewriteRel(r.Project.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Project.Input = newInput
		return rel, nil

	case *pb.Rel_Filter:
		newInput, err := rewriteRel(r.Filter.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Filter.Input = newInput
		return rel, nil

	case *pb.Rel_Fetch:
		newInput, err := rewriteRel(r.Fetch.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Fetch.Input = newInput
		return rel, nil

	case *pb.Rel_Sort:
		newInput, err := rewriteRel(r.Sort.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Sort.Input = newInput
		return rel, nil

	case *pb.Rel_Aggregate:
		newInput, err := rewriteRel(r.Aggregate.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Aggregate.Input = newInput
		return rel, nil

	case *pb.Rel_Join:
		newLeft, err := rewriteRel(r.Join.GetLeft(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		newRight, err := rewriteRel(r.Join.GetRight(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Join.Left = newLeft
		r.Join.Right = newRight
		return rel, nil

	case *pb.Rel_Cross:
		newLeft, err := rewriteRel(r.Cross.GetLeft(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		newRight, err := rewriteRel(r.Cross.GetRight(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.Cross.Left = newLeft
		r.Cross.Right = newRight
		return rel, nil

	case *pb.Rel_Set:
		for i, input := range r.Set.GetInputs() {
			newInput, err := rewriteRel(input, rulesByTable, plan, aa)
			if err != nil {
				return nil, err
			}
			r.Set.Inputs[i] = newInput
		}
		return rel, nil

	case *pb.Rel_HashJoin:
		newLeft, err := rewriteRel(r.HashJoin.GetLeft(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		newRight, err := rewriteRel(r.HashJoin.GetRight(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.HashJoin.Left = newLeft
		r.HashJoin.Right = newRight
		return rel, nil

	case *pb.Rel_MergeJoin:
		newLeft, err := rewriteRel(r.MergeJoin.GetLeft(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		newRight, err := rewriteRel(r.MergeJoin.GetRight(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.MergeJoin.Left = newLeft
		r.MergeJoin.Right = newRight
		return rel, nil

	case *pb.Rel_ExtensionSingle:
		newInput, err := rewriteRel(r.ExtensionSingle.GetInput(), rulesByTable, plan, aa)
		if err != nil {
			return nil, err
		}
		r.ExtensionSingle.Input = newInput
		return rel, nil

	case *pb.Rel_ExtensionMulti:
		for i, input := range r.ExtensionMulti.GetInputs() {
			newInput, err := rewriteRel(input, rulesByTable, plan, aa)
			if err != nil {
				return nil, err
			}
			r.ExtensionMulti.Inputs[i] = newInput
		}
		return rel, nil
	}

	return rel, nil
}

// rewriteRead checks if the ReadRel's table has RLS rules and, if so,
// injects the filter into the ReadRel's Filter field. This is important
// because the ReadRel's filter operates on the full baseSchema before
// column projection is applied, avoiding "positional reference out of range"
// errors when the query selects only a subset of columns.
//
// If the ReadRel already has a filter, the existing filter is combined
// with the new RLS filter using AND.
func rewriteRead(rel *pb.Rel, read *pb.ReadRel, rulesByTable map[string][]policy.RLSRule, plan *pb.Plan, aa *anchorAllocator) (*pb.Rel, error) {
	nt := read.GetNamedTable()
	if nt == nil || len(nt.GetNames()) == 0 {
		return rel, nil
	}

	tableName := resolveTableName(nt.GetNames())
	if tableName == "" {
		return rel, nil
	}
	rules, ok := rulesByTable[tableName]
	if !ok || len(rules) == 0 {
		return rel, nil
	}

	// Resolve column names from the base schema
	schema := read.GetBaseSchema()
	if schema == nil {
		return nil, fmt.Errorf("table %q: ReadRel has no base schema, cannot apply RLS filters", tableName)
	}
	colIndex := buildColumnIndex(schema)

	// Build filter condition
	rlsCond, err := buildFilterCondition(rules, colIndex, read.GetBaseSchema(), plan, aa)
	if err != nil {
		return nil, fmt.Errorf("table %q: %w", tableName, err)
	}

	// If the ReadRel already has a filter, combine with AND
	if read.Filter != nil {
		uriRef := findOrCreateExtensionURI(plan, extensionURIBoolean, aa)
		andAnchor := registerFunction(plan, "and:bool?", uriRef, aa)

		read.Filter = &pb.Expression{
			RexType: &pb.Expression_ScalarFunction_{
				ScalarFunction: &pb.Expression_ScalarFunction{
					FunctionReference: andAnchor,
					OutputType: &pb.Type{
						Kind: &pb.Type_Bool{
							Bool: &pb.Type_Boolean{
								Nullability: pb.Type_NULLABILITY_NULLABLE,
							},
						},
					},
					Arguments: []*pb.FunctionArgument{
						{ArgType: &pb.FunctionArgument_Value{Value: read.Filter}},
						{ArgType: &pb.FunctionArgument_Value{Value: rlsCond}},
					},
				},
			},
		}
	} else {
		read.Filter = rlsCond
	}

	return rel, nil
}

// buildColumnIndex creates a map from column name to 0-based index.
// If duplicate column names exist, the first occurrence wins.
func buildColumnIndex(schema *pb.NamedStruct) map[string]int {
	idx := make(map[string]int)
	if schema == nil {
		return idx
	}
	for i, name := range schema.Names {
		if _, exists := idx[name]; !exists {
			idx[name] = i
		}
	}
	return idx
}

// buildFilterCondition builds a substrait Expression for the given RLS rules.
// Multiple rules are combined with AND.
func buildFilterCondition(rules []policy.RLSRule, colIndex map[string]int, schema *pb.NamedStruct, plan *pb.Plan, aa *anchorAllocator) (*pb.Expression, error) {
	var conditions []*pb.Expression

	for _, rule := range rules {
		idx, ok := colIndex[rule.Column]
		if !ok {
			return nil, fmt.Errorf("column %q not found in schema", rule.Column)
		}

		// Determine the function name and literal based on types
		funcName, literal, err := buildComparisonComponents(rule, schema, idx)
		if err != nil {
			return nil, err
		}

		// Register the extension function
		uriRef := findOrCreateExtensionURI(plan, extensionURIBase, aa)
		funcAnchor := registerFunction(plan, funcName, uriRef, aa)

		// Build the scalar function expression
		cond := &pb.Expression{
			RexType: &pb.Expression_ScalarFunction_{
				ScalarFunction: &pb.Expression_ScalarFunction{
					FunctionReference: funcAnchor,
					OutputType: &pb.Type{
						Kind: &pb.Type_Bool{
							Bool: &pb.Type_Boolean{
								Nullability: pb.Type_NULLABILITY_NULLABLE,
							},
						},
					},
					Arguments: []*pb.FunctionArgument{
						{
							ArgType: &pb.FunctionArgument_Value{
								Value: fieldReference(int32(idx)),
							},
						},
						{
							ArgType: &pb.FunctionArgument_Value{
								Value: literal,
							},
						},
					},
				},
			},
		}
		conditions = append(conditions, cond)
	}

	if len(conditions) == 1 {
		return conditions[0], nil
	}

	// Combine with AND
	uriRef := findOrCreateExtensionURI(plan, extensionURIBoolean, aa)
	andAnchor := registerFunction(plan, "and:bool?", uriRef, aa)

	args := make([]*pb.FunctionArgument, len(conditions))
	for i, c := range conditions {
		args[i] = &pb.FunctionArgument{
			ArgType: &pb.FunctionArgument_Value{Value: c},
		}
	}

	return &pb.Expression{
		RexType: &pb.Expression_ScalarFunction_{
			ScalarFunction: &pb.Expression_ScalarFunction{
				FunctionReference: andAnchor,
				OutputType: &pb.Type{
					Kind: &pb.Type_Bool{
						Bool: &pb.Type_Boolean{
							Nullability: pb.Type_NULLABILITY_NULLABLE,
						},
					},
				},
				Arguments: args,
			},
		},
	}, nil
}

// buildComparisonComponents determines the substrait function name and
// literal expression for a given RLS rule, based on the column type.
func buildComparisonComponents(rule policy.RLSRule, schema *pb.NamedStruct, colIdx int) (string, *pb.Expression, error) {
	if schema == nil || schema.Struct == nil {
		return "", nil, fmt.Errorf("schema is nil for column %q", rule.Column)
	}
	if colIdx < 0 || colIdx >= len(schema.Struct.Types) {
		return "", nil, fmt.Errorf("column index %d out of range (schema has %d types)", colIdx, len(schema.Struct.Types))
	}
	colType := schema.Struct.Types[colIdx]

	var opPrefix string
	switch rule.Operator {
	case policy.OpEqual:
		opPrefix = "equal"
	case policy.OpNotEqual:
		opPrefix = "not_equal"
	case policy.OpLessThan:
		opPrefix = "lt"
	case policy.OpLessEqual:
		opPrefix = "lte"
	case policy.OpGreaterThan:
		opPrefix = "gt"
	case policy.OpGreaterEqual:
		opPrefix = "gte"
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", rule.Operator)
	}

	switch colType.Kind.(type) {
	case *pb.Type_I64_:
		val, ok := rule.Value.(int64)
		if !ok {
			return "", nil, fmt.Errorf("column type is i64 but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:i64_i64", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_I64{I64: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_I32_:
		val, ok := rule.Value.(int32)
		if !ok {
			return "", nil, fmt.Errorf("column type is i32 but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:i32_i32", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_I32{I32: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_String_:
		val, ok := rule.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("column type is string but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:str_str", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_String_{String_: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Fp64:
		val, ok := rule.Value.(float64)
		if !ok {
			return "", nil, fmt.Errorf("column type is fp64 but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:fp64_fp64", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Fp64{Fp64: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Bool:
		val, ok := rule.Value.(bool)
		if !ok {
			return "", nil, fmt.Errorf("column type is bool but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:bool_bool", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Boolean{Boolean: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_I16_:
		val, ok := rule.Value.(int32)
		if !ok {
			return "", nil, fmt.Errorf("column type is i16 but value is %T (use int32)", rule.Value)
		}
		funcName := fmt.Sprintf("%s:i16_i16", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_I16{I16: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_I8_:
		val, ok := rule.Value.(int32)
		if !ok {
			return "", nil, fmt.Errorf("column type is i8 but value is %T (use int32)", rule.Value)
		}
		funcName := fmt.Sprintf("%s:i8_i8", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_I8{I8: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Fp32:
		val, ok := rule.Value.(float32)
		if !ok {
			return "", nil, fmt.Errorf("column type is fp32 but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:fp32_fp32", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Fp32{Fp32: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Date_:
		// Date values are int32 representing days since UNIX epoch.
		val, ok := rule.Value.(int32)
		if !ok {
			return "", nil, fmt.Errorf("column type is date but value is %T (use int32 days since epoch)", rule.Value)
		}
		funcName := fmt.Sprintf("%s:date_date", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Date{Date: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Timestamp_:
		// Timestamp values are int64 representing microseconds since UNIX epoch.
		val, ok := rule.Value.(int64)
		if !ok {
			return "", nil, fmt.Errorf("column type is timestamp but value is %T (use int64 microseconds since epoch)", rule.Value)
		}
		funcName := fmt.Sprintf("%s:ts_ts", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Timestamp{Timestamp: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Varchar:
		val, ok := rule.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("column type is varchar but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:vchar_vchar", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_VarChar_{
						VarChar: &pb.Expression_Literal_VarChar{
							Value:  val,
							Length: uint32(len(val)),
						},
					},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_FixedChar_:
		val, ok := rule.Value.(string)
		if !ok {
			return "", nil, fmt.Errorf("column type is fixedchar but value is %T", rule.Value)
		}
		funcName := fmt.Sprintf("%s:fchar_fchar", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_FixedChar{FixedChar: val},
				},
			},
		}
		return funcName, lit, nil

	case *pb.Type_Decimal_:
		// Decimal values must be provided as a DecimalValue struct.
		val, ok := rule.Value.(*DecimalValue)
		if !ok {
			return "", nil, fmt.Errorf("column type is decimal but value is %T (use *engine.DecimalValue)", rule.Value)
		}
		funcName := fmt.Sprintf("%s:dec_dec", opPrefix)
		lit := &pb.Expression{
			RexType: &pb.Expression_Literal_{
				Literal: &pb.Expression_Literal{
					LiteralType: &pb.Expression_Literal_Decimal_{
						Decimal: &pb.Expression_Literal_Decimal{
							Value:     val.Value,
							Precision: val.Precision,
							Scale:     val.Scale,
						},
					},
				},
			},
		}
		return funcName, lit, nil

	default:
		return "", nil, fmt.Errorf("unsupported column type: %T", colType.Kind)
	}
}

// fieldReference creates a substrait field reference expression.
func fieldReference(fieldIdx int32) *pb.Expression {
	return &pb.Expression{
		RexType: &pb.Expression_Selection{
			Selection: &pb.Expression_FieldReference{
				ReferenceType: &pb.Expression_FieldReference_DirectReference{
					DirectReference: &pb.Expression_ReferenceSegment{
						ReferenceType: &pb.Expression_ReferenceSegment_StructField_{
							StructField: &pb.Expression_ReferenceSegment_StructField{
								Field: fieldIdx,
							},
						},
					},
				},
				RootType: &pb.Expression_FieldReference_RootReference_{
					RootReference: &pb.Expression_FieldReference_RootReference{},
				},
			},
		},
	}
}
