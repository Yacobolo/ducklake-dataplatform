package legacy

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type governanceManifestPageData struct {
	Principal         domain.ContextPrincipal
	CatalogName       string
	SchemaName        string
	TableName         string
	Result            *query.ManifestResult
	CSRFFieldProvider func() Node
}

func governanceManifestPage(d governanceManifestPageData) Node {
	resultNode := Node(nil)
	if d.Result != nil {
		columnRows := make([]Node, 0, len(d.Result.Columns))
		for i := range d.Result.Columns {
			columnRows = append(columnRows, Tr(Td(Text(d.Result.Columns[i].Name)), Td(Text(d.Result.Columns[i].Type))))
		}
		fileRows := make([]Node, 0, len(d.Result.Files))
		for i := range d.Result.Files {
			fileRows = append(fileRows, Li(Code(Text(d.Result.Files[i]))))
		}
		filterRows := make([]Node, 0, len(d.Result.RowFilters))
		for i := range d.Result.RowFilters {
			filterRows = append(filterRows, Li(Code(Text(d.Result.RowFilters[i]))))
		}
		maskRows := []Node{}
		for column, expr := range d.Result.ColumnMasks {
			maskRows = append(maskRows, Tr(Td(Text(column)), Td(Code(Text(expr)))))
		}
		resultNode = Group([]Node{
			Div(Class(cardClass()), H2(Text("Manifest summary")), P(Text("Table: "+d.Result.Schema+"."+d.Result.Table)), P(Text("Expires at: "+formatTime(d.Result.ExpiresAt)))),
			Div(Class(cardClass(tableWrapClass())), H2(Text("Columns")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Type")))), TBody(Group(columnRows)))),
			Div(Class(cardClass()), H2(Text("Files")), Ul(Group(fileRows))),
			Div(Class(cardClass()), H2(Text("Row filters")), Ul(Group(filterRows))),
			Div(Class(cardClass(tableWrapClass())), H2(Text("Column masks")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Column")), Th(Text("Mask")))), TBody(Group(maskRows)))),
		})
	}
	return appPage(
		"Manifest",
		"governance",
		d.Principal,
		governanceSectionNav("manifest"),
		Div(
			Class(cardClass()),
			H2(Text("Generate manifest")),
			Form(
				Method("post"),
				Action("/ui/governance/manifest"),
				d.CSRFFieldProvider(),
				Label(Text("Catalog")),
				Input(Name("catalog_name"), Value(d.CatalogName)),
				Label(Text("Schema")),
				Input(Name("schema_name"), Value(d.SchemaName), Required()),
				Label(Text("Table")),
				Input(Name("table_name"), Value(d.TableName), Required()),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Generate manifest")),
			),
		),
		resultNode,
	)
}
