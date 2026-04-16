package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/ddl"
	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/duckdbsql"
)

const (
	appSystemCatalogAlias  = "__quack_system"
	appSystemCatalogSchema = "main"
)

// AttachSystemCatalog attaches the control-plane SQLite database under an
// internal alias that query rewriting can target for public system.* access.
func AttachSystemCatalog(ctx context.Context, db *sql.DB, sqlitePath string) error {
	if strings.TrimSpace(sqlitePath) == "" {
		return fmt.Errorf("system catalog sqlite path is required")
	}
	if IsCatalogAttached(ctx, db, appSystemCatalogAlias) {
		return nil
	}

	attachSQL := fmt.Sprintf(
		"ATTACH %s AS %s (TYPE sqlite, READ_ONLY)",
		ddl.QuoteLiteral(sqlitePath),
		ddl.QuoteIdentifier(appSystemCatalogAlias),
	)
	if _, err := db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("attach system catalog: %w", err)
	}
	return nil
}

func isSystemTableID(tableID string) bool {
	if domain.IsSystemTableObjectID(tableID) {
		return true
	}
	if _, _, rawTableID, ok := domain.ParseSyntheticCatalogTableID(tableID); ok {
		return domain.IsSystemTableObjectID(rawTableID)
	}
	return false
}

func rewriteAppSystemTableRefs(sqlQuery string) (string, error) {
	stmt, err := duckdbsql.Parse(sqlQuery)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}
	rewriteSystemRefsInStmt(stmt)
	return duckdbsql.Format(stmt), nil
}

func rewriteSystemRefsInStmt(stmt duckdbsql.Stmt) {
	switch s := stmt.(type) {
	case *duckdbsql.SelectStmt:
		rewriteSystemRefsInSelect(s)
	case *duckdbsql.InsertStmt:
		s.Table = rewriteTableNameRef(s.Table)
		rewriteSystemRefsInSelect(s.Query)
		for rowIdx := range s.Values {
			for exprIdx := range s.Values[rowIdx] {
				s.Values[rowIdx][exprIdx] = rewriteSystemRefsInExpr(s.Values[rowIdx][exprIdx])
			}
		}
	case *duckdbsql.UpdateStmt:
		s.Table = rewriteTableNameRef(s.Table)
		rewriteSystemRefsInFrom(s.From)
		for i := range s.Sets {
			s.Sets[i].Value = rewriteSystemRefsInExpr(s.Sets[i].Value)
		}
		s.Where = rewriteSystemRefsInExpr(s.Where)
	case *duckdbsql.DeleteStmt:
		s.Table = rewriteTableNameRef(s.Table)
		rewriteSystemRefsInFrom(s.Using)
		s.Where = rewriteSystemRefsInExpr(s.Where)
	}
}

func rewriteSystemRefsInSelect(sel *duckdbsql.SelectStmt) {
	if sel == nil {
		return
	}
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			rewriteSystemRefsInSelect(cte.Select)
		}
	}
	rewriteSystemRefsInBody(sel.Body)
}

func rewriteSystemRefsInBody(body *duckdbsql.SelectBody) {
	if body == nil {
		return
	}
	rewriteSystemRefsInCore(body.Left)
	rewriteSystemRefsInBody(body.Right)
}

func rewriteSystemRefsInCore(core *duckdbsql.SelectCore) {
	if core == nil {
		return
	}
	rewriteSystemRefsInFrom(core.From)
	core.Where = rewriteSystemRefsInExpr(core.Where)
	core.Having = rewriteSystemRefsInExpr(core.Having)
	core.Qualify = rewriteSystemRefsInExpr(core.Qualify)
	for i := range core.Columns {
		core.Columns[i].Expr = rewriteSystemRefsInExpr(core.Columns[i].Expr)
	}
	for i := range core.GroupBy {
		core.GroupBy[i] = rewriteSystemRefsInExpr(core.GroupBy[i])
	}
	for i := range core.OrderBy {
		core.OrderBy[i].Expr = rewriteSystemRefsInExpr(core.OrderBy[i].Expr)
	}
	core.Limit = rewriteSystemRefsInExpr(core.Limit)
	core.Offset = rewriteSystemRefsInExpr(core.Offset)
	if core.Fetch != nil {
		core.Fetch.Count = rewriteSystemRefsInExpr(core.Fetch.Count)
	}
	if core.Sample != nil {
		core.Sample.Size = rewriteSystemRefsInExpr(core.Sample.Size)
	}
	for rowIdx := range core.ValuesRows {
		for exprIdx := range core.ValuesRows[rowIdx] {
			core.ValuesRows[rowIdx][exprIdx] = rewriteSystemRefsInExpr(core.ValuesRows[rowIdx][exprIdx])
		}
	}
	for i := range core.Windows {
		if core.Windows[i].Spec != nil {
			for partIdx := range core.Windows[i].Spec.PartitionBy {
				core.Windows[i].Spec.PartitionBy[partIdx] = rewriteSystemRefsInExpr(core.Windows[i].Spec.PartitionBy[partIdx])
			}
			for orderIdx := range core.Windows[i].Spec.OrderBy {
				core.Windows[i].Spec.OrderBy[orderIdx].Expr = rewriteSystemRefsInExpr(core.Windows[i].Spec.OrderBy[orderIdx].Expr)
			}
			if core.Windows[i].Spec.Frame != nil {
				if core.Windows[i].Spec.Frame.Start != nil {
					core.Windows[i].Spec.Frame.Start.Offset = rewriteSystemRefsInExpr(core.Windows[i].Spec.Frame.Start.Offset)
				}
				if core.Windows[i].Spec.Frame.End != nil {
					core.Windows[i].Spec.Frame.End.Offset = rewriteSystemRefsInExpr(core.Windows[i].Spec.Frame.End.Offset)
				}
			}
		}
	}
}

func rewriteSystemRefsInFrom(from *duckdbsql.FromClause) {
	if from == nil {
		return
	}
	from.Source = rewriteSystemRefsInTableRef(from.Source)
	for i := range from.Joins {
		from.Joins[i].Right = rewriteSystemRefsInTableRef(from.Joins[i].Right)
		from.Joins[i].Condition = rewriteSystemRefsInExpr(from.Joins[i].Condition)
	}
}

func rewriteSystemRefsInTableRef(ref duckdbsql.TableRef) duckdbsql.TableRef {
	switch t := ref.(type) {
	case *duckdbsql.TableName:
		return rewriteTableNameRef(t)
	case *duckdbsql.DerivedTable:
		rewriteSystemRefsInSelect(t.Select)
	case *duckdbsql.LateralTable:
		rewriteSystemRefsInSelect(t.Select)
	case *duckdbsql.PivotTable:
		t.Source = rewriteSystemRefsInTableRef(t.Source)
		for i := range t.Aggregates {
			t.Aggregates[i].Func = rewriteFuncCall(t.Aggregates[i].Func)
		}
		for i := range t.InValues {
			t.InValues[i].Value = rewriteSystemRefsInExpr(t.InValues[i].Value)
		}
		for i := range t.GroupBy {
			t.GroupBy[i] = rewriteSystemRefsInExpr(t.GroupBy[i])
		}
	case *duckdbsql.UnpivotTable:
		t.Source = rewriteSystemRefsInTableRef(t.Source)
	}
	return ref
}

func rewriteTableNameRef(table *duckdbsql.TableName) *duckdbsql.TableName {
	if table == nil || !domain.IsAppSystemSchema(table.Schema) {
		return table
	}
	table.Catalog = appSystemCatalogAlias
	table.Schema = appSystemCatalogSchema
	return table
}

func rewriteSystemRefsInExpr(expr duckdbsql.Expr) duckdbsql.Expr {
	switch e := expr.(type) {
	case *duckdbsql.BinaryExpr:
		e.Left = rewriteSystemRefsInExpr(e.Left)
		e.Right = rewriteSystemRefsInExpr(e.Right)
	case *duckdbsql.UnaryExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.ParenExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.FuncCall:
		return rewriteFuncCall(e)
	case *duckdbsql.CaseExpr:
		e.Operand = rewriteSystemRefsInExpr(e.Operand)
		for i := range e.Whens {
			e.Whens[i].Condition = rewriteSystemRefsInExpr(e.Whens[i].Condition)
			e.Whens[i].Result = rewriteSystemRefsInExpr(e.Whens[i].Result)
		}
		e.Else = rewriteSystemRefsInExpr(e.Else)
	case *duckdbsql.CastExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.TypeCastExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.InExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
		for i := range e.Values {
			e.Values[i] = rewriteSystemRefsInExpr(e.Values[i])
		}
		rewriteSystemRefsInSelect(e.Query)
	case *duckdbsql.BetweenExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
		e.Low = rewriteSystemRefsInExpr(e.Low)
		e.High = rewriteSystemRefsInExpr(e.High)
	case *duckdbsql.IsNullExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.IsBoolExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.LikeExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
		e.Pattern = rewriteSystemRefsInExpr(e.Pattern)
		e.Escape = rewriteSystemRefsInExpr(e.Escape)
	case *duckdbsql.ExistsExpr:
		rewriteSystemRefsInSelect(e.Select)
	case *duckdbsql.SubqueryExpr:
		rewriteSystemRefsInSelect(e.Select)
	case *duckdbsql.IntervalExpr:
		e.Value = rewriteSystemRefsInExpr(e.Value)
	case *duckdbsql.ExtractExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
	case *duckdbsql.GlobExpr:
		e.Expr = rewriteSystemRefsInExpr(e.Expr)
		e.Pattern = rewriteSystemRefsInExpr(e.Pattern)
	}
	return expr
}

func rewriteFuncCall(fn *duckdbsql.FuncCall) *duckdbsql.FuncCall {
	if fn == nil {
		return nil
	}
	for i := range fn.Args {
		fn.Args[i] = rewriteSystemRefsInExpr(fn.Args[i])
	}
	for i := range fn.OrderBy {
		fn.OrderBy[i].Expr = rewriteSystemRefsInExpr(fn.OrderBy[i].Expr)
	}
	fn.Filter = rewriteSystemRefsInExpr(fn.Filter)
	if fn.Window != nil {
		for i := range fn.Window.PartitionBy {
			fn.Window.PartitionBy[i] = rewriteSystemRefsInExpr(fn.Window.PartitionBy[i])
		}
		for i := range fn.Window.OrderBy {
			fn.Window.OrderBy[i].Expr = rewriteSystemRefsInExpr(fn.Window.OrderBy[i].Expr)
		}
		if fn.Window.Frame != nil {
			if fn.Window.Frame.Start != nil {
				fn.Window.Frame.Start.Offset = rewriteSystemRefsInExpr(fn.Window.Frame.Start.Offset)
			}
			if fn.Window.Frame.End != nil {
				fn.Window.Frame.End.Offset = rewriteSystemRefsInExpr(fn.Window.Frame.End.Offset)
			}
		}
	}
	return fn
}
