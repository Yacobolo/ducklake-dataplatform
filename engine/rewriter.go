package engine

import (
	"fmt"

	"duck-demo/policy"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	pbext "github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
)

const (
	// DuckDB's extension URI prefix for substrait functions.
	extensionURIBase    = "https://github.com/substrait-io/substrait/blob/main/extensions/"
	extensionURIBoolean = "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml"
)

// RewritePlan traverses the plan and injects FilterRel nodes around ReadRel
// nodes according to the given RLS rules (keyed by table name).
// It modifies the plan in place.
func RewritePlan(plan *pb.Plan, rulesByTable map[string][]policy.RLSRule) error {
	if len(rulesByTable) == 0 {
		return nil
	}

	// Track the next available function anchor
	maxAnchor := findMaxFuncAnchor(plan)
	anchorAlloc := &anchorAllocator{next: maxAnchor + 1}

	for _, rel := range plan.GetRelations() {
		if root := rel.GetRoot(); root != nil {
			newInput, err := rewriteRel(root.GetInput(), rulesByTable, plan, anchorAlloc)
			if err != nil {
				return err
			}
			root.Input = newInput
		}
	}

	return nil
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

	tableName := nt.GetNames()[0]
	rules, ok := rulesByTable[tableName]
	if !ok || len(rules) == 0 {
		return rel, nil
	}

	// Resolve column names from the base schema
	colIndex := buildColumnIndex(read.GetBaseSchema())

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
func buildColumnIndex(schema *pb.NamedStruct) map[string]int {
	idx := make(map[string]int)
	if schema == nil {
		return idx
	}
	for i, name := range schema.Names {
		idx[name] = i
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
