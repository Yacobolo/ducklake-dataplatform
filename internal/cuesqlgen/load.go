package cuesqlgen

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"

	cuesql "duck-demo/pkg/cue-sql"
)

// LoadQueries loads CUE-authored query definitions from srcDir.
func LoadQueries(srcDir string) ([]cuesql.Query, error) {
	files, err := filepath.Glob(filepath.Join(srcDir, "*.cue"))
	if err != nil {
		return nil, fmt.Errorf("glob query definitions: %w", err)
	}
	sort.Strings(files)
	ctx := cuecontext.New()
	var queries []cuesql.Query
	seen := make(map[string]struct{})
	for _, file := range files {
		//nolint:gosec // The generator controls the query definition directory contents.
		contents, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", file, err)
		}
		value := ctx.CompileBytes(contents, cue.Filename(file))
		if err := value.Err(); err != nil {
			return nil, fmt.Errorf("compile %s: %w", filepath.Base(file), err)
		}
		field := value.LookupPath(cue.ParsePath("queries"))
		if err := field.Err(); err != nil {
			return nil, fmt.Errorf("%s: missing queries field: %w", filepath.Base(file), err)
		}
		var decoded []cuesql.Query
		if err := field.Decode(&decoded); err != nil {
			return nil, fmt.Errorf("decode %s queries: %w", filepath.Base(file), err)
		}
		for _, query := range decoded {
			if _, ok := seen[query.Name]; ok {
				return nil, fmt.Errorf("duplicate query name %q", query.Name)
			}
			seen[query.Name] = struct{}{}
			queries = append(queries, query)
		}
	}
	return queries, nil
}

// ValidateQueries validates query definitions against the migration-derived catalog.
func ValidateQueries(catalog Catalog, queries []cuesql.Query) error {
	for _, query := range queries {
		if err := query.ValidateShape(); err != nil {
			return err
		}
		if err := validateParams(query); err != nil {
			return err
		}
		if err := validateResult(catalog, query); err != nil {
			return err
		}
		if err := validateStatement(catalog, query); err != nil {
			return err
		}
	}
	return nil
}

func validateParams(query cuesql.Query) error {
	if query.ParamMode == "single" && len(query.Params) != 1 {
		return fmt.Errorf("query %s: paramMode single requires exactly one param", query.Name)
	}
	seen := make(map[string]struct{})
	for _, param := range query.Params {
		if param.Name == "" {
			return fmt.Errorf("query %s: param missing name", query.Name)
		}
		if param.Type == "" {
			return fmt.Errorf("query %s: param %s missing type", query.Name, param.Name)
		}
		if _, ok := seen[param.Name]; ok {
			return fmt.Errorf("query %s: duplicate param %s", query.Name, param.Name)
		}
		seen[param.Name] = struct{}{}
	}
	return nil
}

func validateResult(catalog Catalog, query cuesql.Query) error {
	result := query.Result
	if query.Kind == cuesql.KindExec || query.Kind == cuesql.KindExecResult || query.Kind == cuesql.KindExecRows {
		return nil
	}
	if result.Table == "" && result.Scalar == "" && len(result.Fields) == 0 {
		return fmt.Errorf("query %s: result is required for kind %s", query.Name, query.Kind)
	}
	if result.Table != "" {
		if _, err := catalog.MustTable(result.Table); err != nil {
			return fmt.Errorf("query %s: %w", query.Name, err)
		}
	}
	if result.Row != "" && len(result.Fields) == 0 {
		return fmt.Errorf("query %s: result.row requires fields", query.Name)
	}
	return nil
}

func validateStatement(catalog Catalog, query cuesql.Query) error {
	switch {
	case query.Select != nil:
		if _, err := catalog.MustTable(query.Select.From); err != nil {
			return fmt.Errorf("query %s: %w", query.Name, err)
		}
		for _, join := range query.Select.Joins {
			if _, err := catalog.MustTable(join.Table); err != nil {
				return fmt.Errorf("query %s: %w", query.Name, err)
			}
		}
		for _, predicate := range query.Select.Where {
			if err := validatePredicate(query, predicate); err != nil {
				return err
			}
		}
		if query.Select.LimitParam != "" && !hasParam(query, query.Select.LimitParam) {
			return fmt.Errorf("query %s: unknown limit param %s", query.Name, query.Select.LimitParam)
		}
		if query.Select.OffsetParam != "" && !hasParam(query, query.Select.OffsetParam) {
			return fmt.Errorf("query %s: unknown offset param %s", query.Name, query.Select.OffsetParam)
		}
	case query.Insert != nil:
		table, err := catalog.MustTable(query.Insert.Into)
		if err != nil {
			return fmt.Errorf("query %s: %w", query.Name, err)
		}
		if len(query.Insert.Columns) != len(query.Insert.Values) {
			return fmt.Errorf("query %s: insert columns/values mismatch", query.Name)
		}
		for _, column := range query.Insert.Columns {
			if !tableHasColumn(table, column) {
				return fmt.Errorf("query %s: unknown column %s on table %s", query.Name, column, table.Name)
			}
		}
		for _, value := range query.Insert.Values {
			if value.Param != "" && !hasParam(query, value.Param) {
				return fmt.Errorf("query %s: unknown value param %s", query.Name, value.Param)
			}
		}
	case query.Update != nil:
		table, err := catalog.MustTable(query.Update.Table)
		if err != nil {
			return fmt.Errorf("query %s: %w", query.Name, err)
		}
		for _, assignment := range query.Update.Set {
			if !tableHasColumn(table, assignment.Column) {
				return fmt.Errorf("query %s: unknown update column %s on table %s", query.Name, assignment.Column, table.Name)
			}
			if assignment.Value.Param != "" && !hasParam(query, assignment.Value.Param) {
				return fmt.Errorf("query %s: unknown update param %s", query.Name, assignment.Value.Param)
			}
		}
		for _, predicate := range query.Update.Where {
			if err := validatePredicate(query, predicate); err != nil {
				return err
			}
		}
	case query.Delete != nil:
		if _, err := catalog.MustTable(query.Delete.From); err != nil {
			return fmt.Errorf("query %s: %w", query.Name, err)
		}
		for _, predicate := range query.Delete.Where {
			if err := validatePredicate(query, predicate); err != nil {
				return err
			}
		}
	case query.Raw != nil:
		for _, param := range query.Raw.Bind {
			if !hasParam(query, param) {
				return fmt.Errorf("query %s: raw query references unknown param %s", query.Name, param)
			}
		}
	default:
		return fmt.Errorf("query %s: unsupported statement", query.Name)
	}
	return nil
}

func validatePredicate(query cuesql.Query, predicate cuesql.Predicate) error {
	if predicate.RawSQL != "" {
		return nil
	}
	if predicate.Param != "" && !hasParam(query, predicate.Param) {
		return fmt.Errorf("query %s: unknown predicate param %s", query.Name, predicate.Param)
	}
	if strings.TrimSpace(predicate.Column) == "" && strings.TrimSpace(predicate.Expr) == "" {
		return fmt.Errorf("query %s: predicate missing column/expr", query.Name)
	}
	return nil
}

func hasParam(query cuesql.Query, name string) bool {
	for _, param := range query.Params {
		if param.Name == name {
			return true
		}
	}
	return false
}

func tableHasColumn(table Table, name string) bool {
	for _, column := range table.Columns {
		if column.Name == name {
			return true
		}
	}
	return false
}
