package duckdbsql

// formatStmt dispatches statement formatting by type.
func (f *formatter) formatStmt(stmt Stmt) {
	if stmt == nil {
		return
	}

	switch s := stmt.(type) {
	case *SelectStmt:
		f.formatSelectStmt(s)
	case *InsertStmt:
		f.formatInsertStmt(s)
	case *UpdateStmt:
		f.formatUpdateStmt(s)
	case *DeleteStmt:
		f.formatDeleteStmt(s)
	case *DDLStmt:
		f.write(s.Raw)
	case *UtilityStmt:
		f.write(s.Raw)
	}
}

// === SELECT ===

func (f *formatter) formatSelectStmt(stmt *SelectStmt) {
	if stmt == nil {
		return
	}
	if stmt.With != nil {
		f.formatWithClause(stmt.With)
	}
	if stmt.Body != nil {
		f.formatSelectBody(stmt.Body)
	}
}

func (f *formatter) formatWithClause(with *WithClause) {
	f.write("WITH ")
	if with.Recursive {
		f.write("RECURSIVE ")
	}
	f.commaSep(len(with.CTEs), func(i int) {
		cte := with.CTEs[i]
		f.writeIdent(cte.Name)
		f.write(" AS (")
		f.formatSelectStmt(cte.Select)
		f.write(")")
	})
	f.space()
}

func (f *formatter) formatSelectBody(body *SelectBody) {
	if body == nil {
		return
	}
	f.formatSelectCore(body.Left)

	if body.Op != SetOpNone {
		f.space()
		switch body.Op {
		case SetOpUnion:
			f.write("UNION")
		case SetOpUnionAll:
			f.write("UNION ALL")
		case SetOpIntersect:
			f.write("INTERSECT")
		case SetOpExcept:
			f.write("EXCEPT")
		}
		if body.All && body.Op != SetOpUnionAll {
			f.write(" ALL")
		}
		if body.ByName {
			f.write(" BY NAME")
		}
		f.space()
		f.formatSelectBody(body.Right)
	}
}

func (f *formatter) formatSelectCore(sc *SelectCore) {
	if sc == nil {
		return
	}

	f.write("SELECT ")
	if sc.Distinct {
		f.write("DISTINCT ")
	}

	// Columns
	f.commaSep(len(sc.Columns), func(i int) {
		f.formatSelectItem(sc.Columns[i])
	})

	// FROM
	if sc.From != nil {
		f.write(" FROM ")
		f.formatFromClause(sc.From)
	}

	// WHERE
	if sc.Where != nil {
		f.write(" WHERE ")
		f.formatExpr(sc.Where)
	}

	// GROUP BY
	if sc.GroupByAll {
		f.write(" GROUP BY ALL")
	} else if len(sc.GroupBy) > 0 {
		f.write(" GROUP BY ")
		f.commaSep(len(sc.GroupBy), func(i int) {
			f.formatExpr(sc.GroupBy[i])
		})
	}

	// HAVING
	if sc.Having != nil {
		f.write(" HAVING ")
		f.formatExpr(sc.Having)
	}

	// WINDOW
	if len(sc.Windows) > 0 {
		f.write(" WINDOW ")
		f.commaSep(len(sc.Windows), func(i int) {
			w := sc.Windows[i]
			f.writeIdent(w.Name)
			f.write(" AS ")
			f.formatWindowSpec(w.Spec)
		})
	}

	// QUALIFY
	if sc.Qualify != nil {
		f.write(" QUALIFY ")
		f.formatExpr(sc.Qualify)
	}

	// ORDER BY
	if sc.OrderByAll {
		f.write(" ORDER BY ALL")
		if sc.OrderByAllDesc {
			f.write(" DESC")
		}
	} else if len(sc.OrderBy) > 0 {
		f.write(" ORDER BY ")
		f.commaSep(len(sc.OrderBy), func(i int) {
			f.formatOrderByItem(sc.OrderBy[i])
		})
	}

	// LIMIT
	if sc.Limit != nil {
		f.write(" LIMIT ")
		f.formatExpr(sc.Limit)
	}

	// OFFSET
	if sc.Offset != nil {
		f.write(" OFFSET ")
		f.formatExpr(sc.Offset)
	}

	// FETCH
	if sc.Fetch != nil {
		f.formatFetchClause(sc.Fetch)
	}
}

func (f *formatter) formatSelectItem(item SelectItem) {
	if item.Star {
		f.write("*")
		f.formatStarModifiers(item.Modifiers)
		return
	}
	if item.TableStar != "" {
		f.writeIdent(item.TableStar)
		f.write(".*")
		f.formatStarModifiers(item.Modifiers)
		return
	}
	f.formatExpr(item.Expr)
	if item.Alias != "" {
		f.write(" AS ")
		f.writeIdent(item.Alias)
	}
}

func (f *formatter) formatFromClause(from *FromClause) {
	if from == nil {
		return
	}
	f.formatTableRef(from.Source)
	for _, join := range from.Joins {
		f.formatJoin(join)
	}
}

func (f *formatter) formatTableRef(ref TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *TableName:
		f.formatTableName(t)
	case *DerivedTable:
		f.formatDerivedTable(t)
	case *LateralTable:
		f.formatLateralTable(t)
	case *FuncTable:
		f.formatFuncTable(t)
	case *PivotTable:
		f.formatPivotTable(t)
	case *UnpivotTable:
		f.formatUnpivotTable(t)
	}
}

func (f *formatter) formatTableName(t *TableName) {
	if t.Catalog != "" {
		f.writeIdent(t.Catalog)
		f.write(".")
	}
	if t.Schema != "" {
		f.writeIdent(t.Schema)
		f.write(".")
	}
	f.writeIdent(t.Name)
	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
		f.formatColumnAliases(t.ColumnAliases)
	}
}

func (f *formatter) formatDerivedTable(t *DerivedTable) {
	f.write("(")
	f.formatSelectStmt(t.Select)
	f.write(")")
	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
	}
}

func (f *formatter) formatLateralTable(t *LateralTable) {
	f.write("LATERAL (")
	f.formatSelectStmt(t.Select)
	f.write(")")
	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
	}
}

func (f *formatter) formatFuncTable(t *FuncTable) {
	f.formatFuncCall(t.Func)
	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
		f.formatColumnAliases(t.ColumnAliases)
	}
}

func (f *formatter) formatColumnAliases(aliases []string) {
	if len(aliases) == 0 {
		return
	}
	f.write("(")
	f.commaSep(len(aliases), func(i int) {
		f.writeIdent(aliases[i])
	})
	f.write(")")
}

func (f *formatter) formatPivotTable(t *PivotTable) {
	f.formatTableRef(t.Source)
	f.write(" PIVOT (")

	// Aggregates
	f.commaSep(len(t.Aggregates), func(i int) {
		agg := t.Aggregates[i]
		f.formatFuncCall(agg.Func)
		if agg.Alias != "" {
			f.write(" AS ")
			f.writeIdent(agg.Alias)
		}
	})

	// FOR column IN (...)
	f.write(" FOR ")
	f.writeIdent(t.ForColumn)
	f.write(" IN ")
	if t.InStar {
		f.write("*")
	} else {
		f.write("(")
		f.commaSep(len(t.InValues), func(i int) {
			v := t.InValues[i]
			f.formatExpr(v.Value)
			if v.Alias != "" {
				f.write(" AS ")
				f.writeIdent(v.Alias)
			}
		})
		f.write(")")
	}

	f.write(")")
	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
	}
}

func (f *formatter) formatUnpivotTable(t *UnpivotTable) {
	f.formatTableRef(t.Source)
	f.write(" UNPIVOT (")

	// Value columns
	if len(t.ValueColumns) > 1 {
		f.write("(")
		f.commaSep(len(t.ValueColumns), func(i int) {
			f.writeIdent(t.ValueColumns[i])
		})
		f.write(")")
	} else if len(t.ValueColumns) == 1 {
		f.writeIdent(t.ValueColumns[0])
	}

	// FOR name_column IN (...)
	f.write(" FOR ")
	f.writeIdent(t.NameColumn)
	f.write(" IN (")
	f.commaSep(len(t.InColumns), func(i int) {
		group := t.InColumns[i]
		if len(group.Columns) > 1 {
			f.write("(")
			f.commaSep(len(group.Columns), func(j int) {
				f.writeIdent(group.Columns[j])
			})
			f.write(")")
		} else if len(group.Columns) == 1 {
			f.writeIdent(group.Columns[0])
		}
		if group.Alias != "" {
			f.write(" AS ")
			f.writeIdent(group.Alias)
		}
	})
	f.write("))")

	if t.Alias != "" {
		f.write(" ")
		f.writeIdent(t.Alias)
	}
}

func (f *formatter) formatJoin(join *Join) {
	if join == nil {
		return
	}

	if join.Type == JoinComma {
		f.write(", ")
		f.formatTableRef(join.Right)
		return
	}

	f.space()

	if join.Natural {
		f.write("NATURAL ")
	}

	// Join type keyword
	switch join.Type {
	case JoinInner:
		f.write("JOIN ")
	case JoinCross:
		f.write("CROSS JOIN ")
	default:
		f.write(string(join.Type))
		f.write(" JOIN ")
	}

	f.formatTableRef(join.Right)

	// ON condition
	if join.Condition != nil {
		f.write(" ON ")
		f.formatExpr(join.Condition)
	}

	// USING clause
	if len(join.Using) > 0 {
		f.write(" USING (")
		f.commaSep(len(join.Using), func(i int) {
			f.writeIdent(join.Using[i])
		})
		f.write(")")
	}
}

func (f *formatter) formatFetchClause(fetch *FetchClause) {
	f.write(" FETCH ")
	if fetch.First {
		f.write("FIRST")
	} else {
		f.write("NEXT")
	}
	if fetch.Count != nil {
		f.space()
		f.formatExpr(fetch.Count)
		if fetch.Percent {
			f.write(" PERCENT")
		}
	}
	f.write(" ROWS ")
	if fetch.WithTies {
		f.write("WITH TIES")
	} else {
		f.write("ONLY")
	}
}

// === INSERT ===

func (f *formatter) formatInsertStmt(stmt *InsertStmt) {
	f.write("INSERT INTO ")
	f.formatTableName(stmt.Table)

	// Column list
	if len(stmt.Columns) > 0 {
		f.write(" (")
		f.commaSep(len(stmt.Columns), func(i int) {
			f.writeIdent(stmt.Columns[i])
		})
		f.write(")")
	}

	// VALUES or SELECT
	if stmt.Query != nil {
		f.space()
		f.formatSelectStmt(stmt.Query)
	} else if len(stmt.Values) > 0 {
		f.write(" VALUES ")
		f.commaSep(len(stmt.Values), func(i int) {
			row := stmt.Values[i]
			f.write("(")
			f.commaSep(len(row), func(j int) {
				f.formatExpr(row[j])
			})
			f.write(")")
		})
	}

	// ON CONFLICT
	if stmt.OnConflict != nil {
		f.formatOnConflictClause(stmt.OnConflict)
	}

	// RETURNING
	if len(stmt.Returning) > 0 {
		f.write(" RETURNING ")
		f.commaSep(len(stmt.Returning), func(i int) {
			f.formatSelectItem(stmt.Returning[i])
		})
	}
}

func (f *formatter) formatOnConflictClause(oc *OnConflictClause) {
	f.write(" ON CONFLICT")
	if len(oc.Columns) > 0 {
		f.write(" (")
		f.commaSep(len(oc.Columns), func(i int) {
			f.writeIdent(oc.Columns[i])
		})
		f.write(")")
	}
	if oc.DoNothing {
		f.write(" DO NOTHING")
	} else if len(oc.DoUpdate) > 0 {
		f.write(" DO UPDATE SET ")
		f.formatSetClauses(oc.DoUpdate)
	}
}

// === UPDATE ===

func (f *formatter) formatUpdateStmt(stmt *UpdateStmt) {
	f.write("UPDATE ")
	f.formatTableName(stmt.Table)
	f.write(" SET ")
	f.formatSetClauses(stmt.Sets)

	// FROM clause
	if stmt.From != nil {
		f.write(" FROM ")
		f.formatFromClause(stmt.From)
	}

	// WHERE
	if stmt.Where != nil {
		f.write(" WHERE ")
		f.formatExpr(stmt.Where)
	}

	// RETURNING
	if len(stmt.Returning) > 0 {
		f.write(" RETURNING ")
		f.commaSep(len(stmt.Returning), func(i int) {
			f.formatSelectItem(stmt.Returning[i])
		})
	}
}

// === DELETE ===

func (f *formatter) formatDeleteStmt(stmt *DeleteStmt) {
	f.write("DELETE FROM ")
	f.formatTableName(stmt.Table)

	// USING clause
	if stmt.Using != nil {
		f.write(" USING ")
		f.formatFromClause(stmt.Using)
	}

	// WHERE
	if stmt.Where != nil {
		f.write(" WHERE ")
		f.formatExpr(stmt.Where)
	}

	// RETURNING
	if len(stmt.Returning) > 0 {
		f.write(" RETURNING ")
		f.commaSep(len(stmt.Returning), func(i int) {
			f.formatSelectItem(stmt.Returning[i])
		})
	}
}

// === Helpers ===

func (f *formatter) formatSetClauses(sets []SetClause) {
	f.commaSep(len(sets), func(i int) {
		f.writeIdent(sets[i].Column)
		f.write(" = ")
		f.formatExpr(sets[i].Value)
	})
}
