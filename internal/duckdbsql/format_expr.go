package duckdbsql

import "strings"

// formatExpr dispatches expression formatting by type.
func (f *formatter) formatExpr(e Expr) {
	if e == nil {
		return
	}

	switch expr := e.(type) {
	case *Literal:
		f.formatLiteral(expr)
	case *ColumnRef:
		f.formatColumnRef(expr)
	case *BinaryExpr:
		f.formatBinaryExpr(expr)
	case *UnaryExpr:
		f.formatUnaryExpr(expr)
	case *ParenExpr:
		f.formatParenExpr(expr)
	case *FuncCall:
		f.formatFuncCall(expr)
	case *CaseExpr:
		f.formatCaseExpr(expr)
	case *CastExpr:
		f.formatCastExpr(expr)
	case *TypeCastExpr:
		f.formatTypeCastExpr(expr)
	case *InExpr:
		f.formatInExpr(expr)
	case *BetweenExpr:
		f.formatBetweenExpr(expr)
	case *IsNullExpr:
		f.formatIsNullExpr(expr)
	case *IsBoolExpr:
		f.formatIsBoolExpr(expr)
	case *LikeExpr:
		f.formatLikeExpr(expr)
	case *GlobExpr:
		f.formatGlobExpr(expr)
	case *SimilarToExpr:
		f.formatSimilarToExpr(expr)
	case *ExtractExpr:
		f.formatExtractExpr(expr)
	case *ExistsExpr:
		f.formatExistsExpr(expr)
	case *SubqueryExpr:
		f.formatSubqueryExpr(expr)
	case *StarExpr:
		f.formatStarExpr(expr)
	case *IntervalExpr:
		f.formatIntervalExpr(expr)
	case *ColumnsExpr:
		f.formatColumnsExpr(expr)
	case *RawExpr:
		f.write(expr.SQL)
	case *LambdaExpr:
		f.formatLambdaExpr(expr)
	case *StructLiteral:
		f.formatStructLiteral(expr)
	case *ListLiteral:
		f.formatListLiteral(expr)
	case *IndexExpr:
		f.formatIndexExpr(expr)
	case *ParamExpr:
		f.formatParamExpr(expr)
	case *DefaultExpr:
		f.write("DEFAULT")
	case *IsDistinctExpr:
		f.formatIsDistinctExpr(expr)
	case *CollateExpr:
		f.formatCollateExpr(expr)
	case *MapLiteral:
		f.formatMapLiteral(expr)
	case *ListComprehension:
		f.formatListComprehension(expr)
	case *NamedArgExpr:
		f.formatNamedArgExpr(expr)
	case *GroupingExpr:
		f.formatGroupingExpr(expr)
	}
}

func (f *formatter) formatLiteral(lit *Literal) {
	switch lit.Type {
	case LiteralString:
		f.write("'")
		// Escape single quotes within the string value
		f.write(strings.ReplaceAll(lit.Value, "'", "''"))
		f.write("'")
	case LiteralBool:
		f.write(strings.ToUpper(lit.Value))
	case LiteralNull:
		f.write("NULL")
	default:
		// Number
		f.write(lit.Value)
	}
}

func (f *formatter) formatColumnRef(col *ColumnRef) {
	if col.Table != "" {
		f.writeIdent(col.Table)
		f.write(".")
	}
	f.writeIdent(col.Column)
}

func (f *formatter) formatBinaryExpr(expr *BinaryExpr) {
	f.formatExpr(expr.Left)
	f.space()
	f.write(operatorString(expr.Op))
	f.space()
	f.formatExpr(expr.Right)
}

// operatorString returns the SQL string for a token type used as an operator.
func operatorString(op TokenType) string {
	// Use SQL-standard <> for not-equal (DuckDB accepts both != and <>)
	if op == TOKEN_NE {
		return "<>"
	}
	if op == TOKEN_DBLEQ {
		return "="
	}
	if name, ok := tokenNames[op]; ok {
		return name
	}
	return "?"
}

func (f *formatter) formatUnaryExpr(expr *UnaryExpr) {
	switch expr.Op {
	case TOKEN_NOT:
		f.write("NOT ")
		f.formatExpr(expr.Expr)
	case TOKEN_MINUS:
		f.write("-")
		f.formatExpr(expr.Expr)
	case TOKEN_PLUS:
		f.write("+")
		f.formatExpr(expr.Expr)
	case TOKEN_TILDE:
		f.write("~")
		f.formatExpr(expr.Expr)
	default:
		f.write(operatorString(expr.Op))
		f.formatExpr(expr.Expr)
	}
}

func (f *formatter) formatParenExpr(paren *ParenExpr) {
	f.write("(")
	f.formatExpr(paren.Expr)
	f.write(")")
}

func (f *formatter) formatFuncCall(fn *FuncCall) {
	if fn.Schema != "" {
		f.writeIdent(fn.Schema)
		f.write(".")
	}
	// Function names are written unquoted in original case
	f.write(fn.Name)
	f.write("(")

	if fn.Distinct {
		f.write("DISTINCT ")
	}

	if fn.Star {
		f.write("*")
	} else {
		f.commaSep(len(fn.Args), func(i int) {
			f.formatExpr(fn.Args[i])
		})
	}

	// ORDER BY inside aggregate (e.g., array_agg(x ORDER BY y))
	if len(fn.OrderBy) > 0 {
		f.write(" ORDER BY ")
		f.commaSep(len(fn.OrderBy), func(i int) {
			f.formatOrderByItem(fn.OrderBy[i])
		})
	}

	f.write(")")

	// FILTER clause
	if fn.Filter != nil {
		f.write(" FILTER (WHERE ")
		f.formatExpr(fn.Filter)
		f.write(")")
	}

	// OVER clause (window)
	if fn.Window != nil {
		f.write(" OVER ")
		f.formatWindowSpec(fn.Window)
	}
}

func (f *formatter) formatWindowSpec(w *WindowSpec) {
	// Named window reference without details: emit without parens
	if w.Name != "" && len(w.PartitionBy) == 0 && len(w.OrderBy) == 0 && w.Frame == nil {
		f.writeIdent(w.Name)
		return
	}

	f.write("(")

	needSpace := false
	if w.Name != "" {
		f.writeIdent(w.Name)
		needSpace = true
	}

	if len(w.PartitionBy) > 0 {
		if needSpace {
			f.space()
		}
		f.write("PARTITION BY ")
		f.commaSep(len(w.PartitionBy), func(i int) {
			f.formatExpr(w.PartitionBy[i])
		})
		needSpace = true
	}

	if len(w.OrderBy) > 0 {
		if needSpace {
			f.space()
		}
		f.write("ORDER BY ")
		f.commaSep(len(w.OrderBy), func(i int) {
			f.formatOrderByItem(w.OrderBy[i])
		})
		needSpace = true
	}

	if w.Frame != nil {
		if needSpace {
			f.space()
		}
		f.formatFrameSpec(w.Frame)
	}

	f.write(")")
}

func (f *formatter) formatFrameSpec(fs *FrameSpec) {
	f.write(string(fs.Type))
	if fs.End != nil {
		f.write(" BETWEEN ")
		f.formatFrameBound(fs.Start)
		f.write(" AND ")
		f.formatFrameBound(fs.End)
	} else {
		f.space()
		f.formatFrameBound(fs.Start)
	}
}

func (f *formatter) formatFrameBound(b *FrameBound) {
	if b == nil {
		return
	}
	switch b.Type {
	case FrameUnboundedPreceding:
		f.write("UNBOUNDED PRECEDING")
	case FrameUnboundedFollowing:
		f.write("UNBOUNDED FOLLOWING")
	case FrameCurrentRow:
		f.write("CURRENT ROW")
	case FrameExprPreceding:
		f.formatExpr(b.Offset)
		f.write(" PRECEDING")
	case FrameExprFollowing:
		f.formatExpr(b.Offset)
		f.write(" FOLLOWING")
	}
}

func (f *formatter) formatCaseExpr(c *CaseExpr) {
	f.write("CASE")
	if c.Operand != nil {
		f.space()
		f.formatExpr(c.Operand)
	}
	for _, w := range c.Whens {
		f.write(" WHEN ")
		f.formatExpr(w.Condition)
		f.write(" THEN ")
		f.formatExpr(w.Result)
	}
	if c.Else != nil {
		f.write(" ELSE ")
		f.formatExpr(c.Else)
	}
	f.write(" END")
}

func (f *formatter) formatCastExpr(c *CastExpr) {
	if c.TryCast {
		f.write("TRY_CAST(")
	} else {
		f.write("CAST(")
	}
	f.formatExpr(c.Expr)
	f.write(" AS ")
	f.write(c.TypeName)
	f.write(")")
}

func (f *formatter) formatTypeCastExpr(c *TypeCastExpr) {
	f.formatExpr(c.Expr)
	f.write("::")
	f.write(c.TypeName)
}

func (f *formatter) formatInExpr(in *InExpr) {
	f.formatExpr(in.Expr)
	if in.Not {
		f.write(" NOT")
	}
	f.write(" IN (")
	if in.Query != nil {
		f.formatSelectStmt(in.Query)
	} else {
		f.commaSep(len(in.Values), func(i int) {
			f.formatExpr(in.Values[i])
		})
	}
	f.write(")")
}

func (f *formatter) formatBetweenExpr(b *BetweenExpr) {
	f.formatExpr(b.Expr)
	if b.Not {
		f.write(" NOT")
	}
	f.write(" BETWEEN ")
	f.formatExpr(b.Low)
	f.write(" AND ")
	f.formatExpr(b.High)
}

func (f *formatter) formatIsNullExpr(is *IsNullExpr) {
	f.formatExpr(is.Expr)
	if is.Not {
		f.write(" IS NOT NULL")
	} else {
		f.write(" IS NULL")
	}
}

func (f *formatter) formatIsBoolExpr(is *IsBoolExpr) {
	f.formatExpr(is.Expr)
	f.write(" IS ")
	if is.Not {
		f.write("NOT ")
	}
	if is.Value {
		f.write("TRUE")
	} else {
		f.write("FALSE")
	}
}

func (f *formatter) formatLikeExpr(like *LikeExpr) {
	f.formatExpr(like.Expr)
	if like.Not {
		f.write(" NOT")
	}
	if like.ILike {
		f.write(" ILIKE ")
	} else {
		f.write(" LIKE ")
	}
	f.formatExpr(like.Pattern)
	if like.Escape != nil {
		f.write(" ESCAPE ")
		f.formatExpr(like.Escape)
	}
}

func (f *formatter) formatGlobExpr(g *GlobExpr) {
	f.formatExpr(g.Expr)
	if g.Not {
		f.write(" NOT")
	}
	f.write(" GLOB ")
	f.formatExpr(g.Pattern)
}

func (f *formatter) formatSimilarToExpr(s *SimilarToExpr) {
	f.formatExpr(s.Expr)
	if s.Not {
		f.write(" NOT")
	}
	f.write(" SIMILAR TO ")
	f.formatExpr(s.Pattern)
}

func (f *formatter) formatExtractExpr(ext *ExtractExpr) {
	f.write("EXTRACT(")
	f.write(ext.Field)
	f.write(" FROM ")
	f.formatExpr(ext.Expr)
	f.write(")")
}

func (f *formatter) formatExistsExpr(ex *ExistsExpr) {
	if ex.Not {
		f.write("NOT ")
	}
	f.write("EXISTS (")
	f.formatSelectStmt(ex.Select)
	f.write(")")
}

func (f *formatter) formatSubqueryExpr(sq *SubqueryExpr) {
	f.write("(")
	f.formatSelectStmt(sq.Select)
	f.write(")")
}

func (f *formatter) formatStarExpr(star *StarExpr) {
	if star.Table != "" {
		f.writeIdent(star.Table)
		f.write(".")
	}
	f.write("*")
	f.formatStarModifiers(star.Modifiers)
}

func (f *formatter) formatStarModifiers(mods []StarModifier) {
	for _, mod := range mods {
		switch m := mod.(type) {
		case *ExcludeModifier:
			f.write(" EXCLUDE (")
			f.commaSep(len(m.Columns), func(i int) {
				f.writeIdent(m.Columns[i])
			})
			f.write(")")
		case *ReplaceModifier:
			f.write(" REPLACE (")
			f.commaSep(len(m.Items), func(i int) {
				f.formatExpr(m.Items[i].Expr)
				f.write(" AS ")
				f.writeIdent(m.Items[i].Alias)
			})
			f.write(")")
		case *RenameModifier:
			f.write(" RENAME (")
			f.commaSep(len(m.Items), func(i int) {
				f.writeIdent(m.Items[i].OldName)
				f.write(" AS ")
				f.writeIdent(m.Items[i].NewName)
			})
			f.write(")")
		}
	}
}

func (f *formatter) formatIntervalExpr(iv *IntervalExpr) {
	f.write("INTERVAL ")
	f.formatExpr(iv.Value)
	if iv.Unit != "" {
		f.space()
		f.write(iv.Unit)
	}
}

func (f *formatter) formatColumnsExpr(ce *ColumnsExpr) {
	f.write("COLUMNS(")
	f.formatExpr(ce.Pattern)
	f.write(")")
}

func (f *formatter) formatLambdaExpr(lambda *LambdaExpr) {
	if len(lambda.Params) == 1 {
		f.writeIdent(lambda.Params[0])
	} else {
		f.write("(")
		f.commaSep(len(lambda.Params), func(i int) {
			f.writeIdent(lambda.Params[i])
		})
		f.write(")")
	}
	f.write(" -> ")
	f.formatExpr(lambda.Body)
}

func (f *formatter) formatStructLiteral(s *StructLiteral) {
	f.write("{")
	f.commaSep(len(s.Fields), func(i int) {
		f.write("'")
		f.write(strings.ReplaceAll(s.Fields[i].Key, "'", "''"))
		f.write("': ")
		f.formatExpr(s.Fields[i].Value)
	})
	f.write("}")
}

func (f *formatter) formatListLiteral(list *ListLiteral) {
	f.write("[")
	f.commaSep(len(list.Elements), func(i int) {
		f.formatExpr(list.Elements[i])
	})
	f.write("]")
}

func (f *formatter) formatIndexExpr(idx *IndexExpr) {
	f.formatExpr(idx.Expr)
	f.write("[")
	if idx.IsSlice {
		if idx.Start != nil {
			f.formatExpr(idx.Start)
		}
		f.write(":")
		if idx.Stop != nil {
			f.formatExpr(idx.Stop)
		}
	} else {
		f.formatExpr(idx.Index)
	}
	f.write("]")
}

func (f *formatter) formatOrderByItem(item OrderByItem) {
	f.formatExpr(item.Expr)
	if item.Desc {
		f.write(" DESC")
	}
	if item.NullsFirst != nil {
		if *item.NullsFirst {
			f.write(" NULLS FIRST")
		} else {
			f.write(" NULLS LAST")
		}
	}
}

func (f *formatter) formatParamExpr(expr *ParamExpr) {
	if expr.Style == ParamQuestion {
		f.write("?")
	} else {
		f.write(expr.Name)
	}
}

func (f *formatter) formatIsDistinctExpr(expr *IsDistinctExpr) {
	f.formatExpr(expr.Left)
	if expr.Not {
		f.write(" IS NOT DISTINCT FROM ")
	} else {
		f.write(" IS DISTINCT FROM ")
	}
	f.formatExpr(expr.Right)
}

func (f *formatter) formatCollateExpr(expr *CollateExpr) {
	f.formatExpr(expr.Expr)
	f.write(" COLLATE ")
	f.write(expr.Collation)
}

func (f *formatter) formatMapLiteral(expr *MapLiteral) {
	f.write("MAP {")
	f.commaSep(len(expr.Entries), func(i int) {
		f.write("'")
		f.write(strings.ReplaceAll(expr.Entries[i].Key, "'", "''"))
		f.write("': ")
		f.formatExpr(expr.Entries[i].Value)
	})
	f.write("}")
}

func (f *formatter) formatListComprehension(expr *ListComprehension) {
	f.write("[")
	f.formatExpr(expr.Expr)
	f.write(" FOR ")
	f.writeIdent(expr.Var)
	f.write(" IN ")
	f.formatExpr(expr.List)
	if expr.Cond != nil {
		f.write(" IF ")
		f.formatExpr(expr.Cond)
	}
	f.write("]")
}

func (f *formatter) formatNamedArgExpr(expr *NamedArgExpr) {
	f.writeIdent(expr.Name)
	f.write(" := ")
	f.formatExpr(expr.Value)
}

func (f *formatter) formatGroupingExpr(expr *GroupingExpr) {
	f.write(expr.Type)
	f.write(" (")
	f.commaSep(len(expr.Groups), func(i int) {
		group := expr.Groups[i]
		if len(group) == 0 {
			f.write("()")
		} else if expr.Type == "GROUPING SETS" {
			f.write("(")
			f.commaSep(len(group), func(j int) {
				f.formatExpr(group[j])
			})
			f.write(")")
		} else {
			f.commaSep(len(group), func(j int) {
				f.formatExpr(group[j])
			})
		}
	})
	f.write(")")
}
