package sqlrewrite

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// buildRuleExpr creates an A_Expr node representing a single RLS rule condition.
// For example: "Pclass" = 1 or t."Survived" = 1
func buildRuleExpr(rule RLSRule, tableAlias string) (*pg_query.Node, error) {
	// Left side: column reference (optionally qualified with table alias)
	colRef := makeColumnRef(rule.Column, tableAlias)

	// Right side: literal value
	literal, err := makeLiteral(rule.Value)
	if err != nil {
		return nil, fmt.Errorf("RLS rule for %s.%s: %w", rule.Table, rule.Column, err)
	}

	// Operator
	opName, err := operatorToSQL(rule.Operator)
	if err != nil {
		return nil, err
	}

	return &pg_query.Node{
		Node: &pg_query.Node_AExpr{
			AExpr: &pg_query.A_Expr{
				Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
				Name:  []*pg_query.Node{makeStringNode(opName)},
				Lexpr: colRef,
				Rexpr: literal,
			},
		},
	}, nil
}

// operatorToSQL converts a policy operator constant to a SQL operator string.
func operatorToSQL(op string) (string, error) {
	switch op {
	case OpEqual:
		return "=", nil
	case OpNotEqual:
		return "<>", nil
	case OpLessThan:
		return "<", nil
	case OpLessEqual:
		return "<=", nil
	case OpGreaterThan:
		return ">", nil
	case OpGreaterEqual:
		return ">=", nil
	default:
		return "", fmt.Errorf("unsupported operator: %q", op)
	}
}

// makeColumnRef creates a ColumnRef node. If tableAlias is non-empty,
// it creates a qualified reference (alias."column"), otherwise just ("column").
func makeColumnRef(column, tableAlias string) *pg_query.Node {
	var fields []*pg_query.Node
	if tableAlias != "" {
		fields = append(fields, makeStringNode(tableAlias))
	}
	fields = append(fields, makeStringNode(column))

	return &pg_query.Node{
		Node: &pg_query.Node_ColumnRef{
			ColumnRef: &pg_query.ColumnRef{
				Fields: fields,
			},
		},
	}
}

// makeLiteral creates an A_Const node for the given Go value.
func makeLiteral(v interface{}) (*pg_query.Node, error) {
	switch val := v.(type) {
	case int:
		return makeIntegerConst(int64(val)), nil
	case int8:
		return makeIntegerConst(int64(val)), nil
	case int16:
		return makeIntegerConst(int64(val)), nil
	case int32:
		return makeIntegerConst(int64(val)), nil
	case int64:
		return makeIntegerConst(val), nil
	case float32:
		return makeFloatConst(fmt.Sprintf("%g", val)), nil
	case float64:
		return makeFloatConst(fmt.Sprintf("%g", val)), nil
	case string:
		return makeStringConst(val), nil
	case bool:
		// PostgreSQL represents bools as string constants 'true'/'false' in some contexts,
		// but for WHERE clauses we use the keyword form via TypeCast.
		if val {
			return makeStringConst("true"), nil
		}
		return makeStringConst("false"), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", v)
	}
}

func makeIntegerConst(v int64) *pg_query.Node {
	// If value fits in int32, use Ival; otherwise use Fval (string representation)
	// to avoid silent overflow.
	if v >= -2147483648 && v <= 2147483647 {
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{Ival: int32(v)},
					},
				},
			},
		}
	}
	// Large values: represent as numeric string
	return makeFloatConst(fmt.Sprintf("%d", v))
}

func makeFloatConst(v string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Fval{
					Fval: &pg_query.Float{Fval: v},
				},
			},
		},
	}
}

func makeStringConst(v string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Sval{
					Sval: &pg_query.String{Sval: v},
				},
			},
		},
	}
}

func makeStringNode(s string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_String_{
			String_: &pg_query.String{Sval: s},
		},
	}
}

// combineWithAnd combines multiple expressions into a single BoolExpr AND.
// If there's only one expression, returns it directly.
func combineWithAnd(exprs []*pg_query.Node) *pg_query.Node {
	if len(exprs) == 1 {
		return exprs[0]
	}
	return &pg_query.Node{
		Node: &pg_query.Node_BoolExpr{
			BoolExpr: &pg_query.BoolExpr{
				Boolop: pg_query.BoolExprType_AND_EXPR,
				Args:   exprs,
			},
		},
	}
}

// makeAndExpr creates a BoolExpr AND combining two expressions.
func makeAndExpr(left, right *pg_query.Node) *pg_query.Node {
	// If either side is already an AND, flatten it
	var args []*pg_query.Node

	if be, ok := left.Node.(*pg_query.Node_BoolExpr); ok && be.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
		args = append(args, be.BoolExpr.Args...)
	} else {
		args = append(args, left)
	}

	if be, ok := right.Node.(*pg_query.Node_BoolExpr); ok && be.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
		args = append(args, be.BoolExpr.Args...)
	} else {
		args = append(args, right)
	}

	return &pg_query.Node{
		Node: &pg_query.Node_BoolExpr{
			BoolExpr: &pg_query.BoolExpr{
				Boolop: pg_query.BoolExprType_AND_EXPR,
				Args:   args,
			},
		},
	}
}

// QuoteIdentifier quotes a SQL identifier if it contains special characters
// or is a reserved word. Uses double quotes.
func QuoteIdentifier(s string) string {
	// Simple check: if it's all lowercase alphanumeric + underscore, no quoting needed
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
	}
	return s
}
