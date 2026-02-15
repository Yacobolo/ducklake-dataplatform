package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func newDescribeCmd(client *gen.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe [object-path]",
		Short: "Describe a catalog object with progressive detail",
		Long: `Progressive disclosure: the level of detail depends on the object path depth.

  duck describe                          → platform overview (all catalogs)
  duck describe <catalog>                → catalog detail with schemas
  duck describe <catalog.schema>         → schema detail with tables, views, volumes
  duck describe <catalog.schema.table>   → table detail with columns, stats, security policies

This is designed as the agent's "cat" for reading detailed metadata about any object.`,
		Example: `  # Platform overview
  duck describe

  # Catalog detail
  duck describe main

  # Schema with all its tables
  duck describe main.analytics

  # Full table detail with columns and security
  duck describe main.analytics.orders --output json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			isJSON := getOutputFormat(cmd) == "json"
			if len(args) == 0 {
				return describePlatform(client, isJSON)
			}
			parts := strings.Split(args[0], ".")
			switch len(parts) {
			case 1:
				return describeCatalog(client, parts[0], isJSON)
			case 2:
				return describeSchema(client, parts[0], parts[1], isJSON)
			case 3:
				return describeTable(client, parts[0], parts[1], parts[2], isJSON)
			default:
				return fmt.Errorf("invalid object path %q: expected catalog[.schema[.table]]", args[0])
			}
		},
	}
	return cmd
}

// describePlatform shows all registered catalogs.
func describePlatform(client *gen.Client, isJSON bool) error {
	resp, err := client.Do("GET", "/catalogs", nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(resp); err != nil {
		return err
	}
	body, err := gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if isJSON {
		var pretty interface{}
		_ = json.Unmarshal(body, &pretty)
		return gen.PrintJSON(os.Stdout, pretty)
	}

	var data struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	fmt.Fprintln(os.Stdout, "PLATFORM OVERVIEW")
	fmt.Fprintf(os.Stdout, "catalogs: %d\n\n", len(data.Data))
	columns := []string{"name", "status", "is_default", "metastore_type"}
	rows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(data.Data)}, columns)
	gen.PrintTable(os.Stdout, columns, rows)
	return nil
}

// describeCatalog shows catalog info and its schemas.
func describeCatalog(client *gen.Client, catalog string, isJSON bool) error {
	// Fetch catalog registration info
	regResp, err := client.Do("GET", "/catalogs/"+url.PathEscape(catalog), nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(regResp); err != nil {
		return err
	}
	regBody, err := gen.ReadBody(regResp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Fetch schemas
	schemasResp, err := client.Do("GET", "/catalogs/"+url.PathEscape(catalog)+"/schemas", nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(schemasResp); err != nil {
		return err
	}
	schemasBody, err := gen.ReadBody(schemasResp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if isJSON {
		var reg, schemas interface{}
		_ = json.Unmarshal(regBody, &reg)
		_ = json.Unmarshal(schemasBody, &schemas)
		return gen.PrintJSON(os.Stdout, map[string]interface{}{
			"catalog": reg,
			"schemas": schemas,
		})
	}

	var regData map[string]interface{}
	_ = json.Unmarshal(regBody, &regData)
	fmt.Fprintf(os.Stdout, "CATALOG: %s\n", catalog)
	gen.PrintDetail(os.Stdout, regData)

	var schemasData struct {
		Data []map[string]interface{} `json:"data"`
	}
	_ = json.Unmarshal(schemasBody, &schemasData)
	fmt.Fprintf(os.Stdout, "\nSCHEMAS (%d):\n", len(schemasData.Data))
	columns := []string{"name", "comment"}
	rows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(schemasData.Data)}, columns)
	gen.PrintTable(os.Stdout, columns, rows)
	return nil
}

// describeSchema shows schema detail and its tables, views, volumes.
func describeSchema(client *gen.Client, catalog, schema string, isJSON bool) error {
	base := "/catalogs/" + url.PathEscape(catalog) + "/schemas/" + url.PathEscape(schema)

	// Fetch schema detail
	schemaResp, err := client.Do("GET", base, nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(schemaResp); err != nil {
		return err
	}
	schemaBody, err := gen.ReadBody(schemaResp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Fetch tables
	tablesResp, err := client.Do("GET", base+"/tables", nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(tablesResp); err != nil {
		return err
	}
	tablesBody, err := gen.ReadBody(tablesResp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Fetch views
	viewsResp, err := client.Do("GET", base+"/views", nil, nil)
	if err != nil {
		return err
	}
	var viewsBody []byte
	if viewsResp.StatusCode < 300 {
		viewsBody, _ = gen.ReadBody(viewsResp)
	} else {
		_ = viewsResp.Body.Close()
	}

	// Fetch volumes
	volumesResp, err := client.Do("GET", base+"/volumes", nil, nil)
	if err != nil {
		return err
	}
	var volumesBody []byte
	if volumesResp.StatusCode < 300 {
		volumesBody, _ = gen.ReadBody(volumesResp)
	} else {
		_ = volumesResp.Body.Close()
	}

	if isJSON {
		var schemaData, tablesData, viewsData, volumesData interface{}
		_ = json.Unmarshal(schemaBody, &schemaData)
		_ = json.Unmarshal(tablesBody, &tablesData)
		if viewsBody != nil {
			_ = json.Unmarshal(viewsBody, &viewsData)
		}
		if volumesBody != nil {
			_ = json.Unmarshal(volumesBody, &volumesData)
		}
		return gen.PrintJSON(os.Stdout, map[string]interface{}{
			"schema":  schemaData,
			"tables":  tablesData,
			"views":   viewsData,
			"volumes": volumesData,
		})
	}

	var schemaDetail map[string]interface{}
	_ = json.Unmarshal(schemaBody, &schemaDetail)
	fmt.Fprintf(os.Stdout, "SCHEMA: %s.%s\n", catalog, schema)
	gen.PrintDetail(os.Stdout, schemaDetail)

	var tablesData struct {
		Data []map[string]interface{} `json:"data"`
	}
	_ = json.Unmarshal(tablesBody, &tablesData)
	fmt.Fprintf(os.Stdout, "\nTABLES (%d):\n", len(tablesData.Data))
	columns := []string{"name", "table_type", "comment"}
	rows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(tablesData.Data)}, columns)
	gen.PrintTable(os.Stdout, columns, rows)

	if viewsBody != nil {
		var viewsData struct {
			Data []map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(viewsBody, &viewsData)
		if len(viewsData.Data) > 0 {
			fmt.Fprintf(os.Stdout, "\nVIEWS (%d):\n", len(viewsData.Data))
			vColumns := []string{"name", "comment"}
			vRows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(viewsData.Data)}, vColumns)
			gen.PrintTable(os.Stdout, vColumns, vRows)
		}
	}

	if volumesBody != nil {
		var volumesData struct {
			Data []map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(volumesBody, &volumesData)
		if len(volumesData.Data) > 0 {
			fmt.Fprintf(os.Stdout, "\nVOLUMES (%d):\n", len(volumesData.Data))
			volColumns := []string{"name", "volume_type"}
			volRows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(volumesData.Data)}, volColumns)
			gen.PrintTable(os.Stdout, volColumns, volRows)
		}
	}

	return nil
}

// describeTable shows full table detail with columns, stats, and security policies.
func describeTable(client *gen.Client, catalog, schema, table string, isJSON bool) error {
	base := "/catalogs/" + url.PathEscape(catalog) + "/schemas/" + url.PathEscape(schema) + "/tables/" + url.PathEscape(table)

	// Fetch table detail (includes columns)
	tableResp, err := client.Do("GET", base, nil, nil)
	if err != nil {
		return err
	}
	if err := gen.CheckError(tableResp); err != nil {
		return err
	}
	tableBody, err := gen.ReadBody(tableResp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Parse table detail to extract table_id for security queries
	var tableDetail map[string]interface{}
	if err := json.Unmarshal(tableBody, &tableDetail); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	// Try to fetch row filters and column masks (best-effort)
	tableID := gen.ExtractField(tableDetail, "table_id")
	var rowFiltersBody, columnMasksBody []byte
	if tableID != "" {
		q := url.Values{}
		if rfResp, err := client.Do("GET", "/tables/"+url.PathEscape(tableID)+"/row-filters", q, nil); err == nil {
			if rfResp.StatusCode < 300 {
				rowFiltersBody, _ = gen.ReadBody(rfResp)
			} else {
				_ = rfResp.Body.Close()
			}
		}
		if cmResp, err := client.Do("GET", "/tables/"+url.PathEscape(tableID)+"/column-masks", q, nil); err == nil {
			if cmResp.StatusCode < 300 {
				columnMasksBody, _ = gen.ReadBody(cmResp)
			} else {
				_ = cmResp.Body.Close()
			}
		}
	}

	if isJSON {
		result := map[string]interface{}{
			"table": tableDetail,
		}
		if rowFiltersBody != nil {
			var rf interface{}
			_ = json.Unmarshal(rowFiltersBody, &rf)
			result["row_filters"] = rf
		}
		if columnMasksBody != nil {
			var cm interface{}
			_ = json.Unmarshal(columnMasksBody, &cm)
			result["column_masks"] = cm
		}
		return gen.PrintJSON(os.Stdout, result)
	}

	// Human-friendly output
	fmt.Fprintf(os.Stdout, "TABLE: %s.%s.%s\n", catalog, schema, table)

	// Print key fields
	for _, key := range []string{"table_type", "owner", "comment", "storage_path", "created_at", "updated_at"} {
		if v := gen.ExtractField(tableDetail, key); v != "" {
			fmt.Fprintf(os.Stdout, "%-14s %s\n", key+":", v)
		}
	}

	// Statistics
	if stats, ok := tableDetail["statistics"].(map[string]interface{}); ok {
		fmt.Fprintln(os.Stdout, "\nSTATISTICS:")
		for _, key := range []string{"row_count", "size_bytes", "column_count", "last_profiled_at"} {
			if v := gen.ExtractField(stats, key); v != "" {
				fmt.Fprintf(os.Stdout, "%-18s %s\n", key+":", v)
			}
		}
	}

	// Columns
	if cols, ok := tableDetail["columns"].([]interface{}); ok && len(cols) > 0 {
		fmt.Fprintf(os.Stdout, "\nCOLUMNS (%d):\n", len(cols))
		colColumns := []string{"name", "type", "nullable", "comment"}
		var colRows [][]string
		for _, col := range cols {
			if m, ok := col.(map[string]interface{}); ok {
				row := make([]string, len(colColumns))
				for i, c := range colColumns {
					row[i] = gen.ExtractField(m, c)
				}
				colRows = append(colRows, row)
			}
		}
		gen.PrintTable(os.Stdout, colColumns, colRows)
	}

	// Tags
	if tags, ok := tableDetail["tags"].([]interface{}); ok && len(tags) > 0 {
		fmt.Fprintf(os.Stdout, "\nTAGS (%d):\n", len(tags))
		tagColumns := []string{"key", "value"}
		var tagRows [][]string
		for _, tag := range tags {
			if m, ok := tag.(map[string]interface{}); ok {
				tagRows = append(tagRows, []string{gen.ExtractField(m, "key"), gen.ExtractField(m, "value")})
			}
		}
		gen.PrintTable(os.Stdout, tagColumns, tagRows)
	}

	// Row Filters
	if rowFiltersBody != nil {
		var rf struct {
			Data []map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(rowFiltersBody, &rf)
		if len(rf.Data) > 0 {
			fmt.Fprintf(os.Stdout, "\nROW FILTERS (%d):\n", len(rf.Data))
			rfColumns := []string{"id", "filter_sql", "description"}
			rfRows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(rf.Data)}, rfColumns)
			gen.PrintTable(os.Stdout, rfColumns, rfRows)
		}
	}

	// Column Masks
	if columnMasksBody != nil {
		var cm struct {
			Data []map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(columnMasksBody, &cm)
		if len(cm.Data) > 0 {
			fmt.Fprintf(os.Stdout, "\nCOLUMN MASKS (%d):\n", len(cm.Data))
			cmColumns := []string{"column_name", "mask_expression", "description"}
			cmRows := gen.ExtractRows(map[string]interface{}{"data": toInterfaceSlice(cm.Data)}, cmColumns)
			gen.PrintTable(os.Stdout, cmColumns, cmRows)
		}
	}

	return nil
}

// toInterfaceSlice converts []map[string]interface{} to []interface{} for gen.ExtractRows.
func toInterfaceSlice(in []map[string]interface{}) []interface{} {
	out := make([]interface{}, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}
