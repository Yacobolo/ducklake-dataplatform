package ui

import (
	"strconv"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type sqlAsyncJobPageData struct {
	Principal         domain.ContextPrincipal
	JobID             string
	Status            string
	RequestedCompute  string
	ResolvedCompute   string
	RequestID         string
	SQLText           string
	Columns           []string
	Rows              [][]interface{}
	RowCount          int
	ErrorText         string
	AttemptCount      int
	MaxAttempts       int
	LastHeartbeatText string
	NextRetryText     string
	CreatedAtText     string
	StartedAtText     string
	CompletedAtText   string
	CancelURL         string
	DeleteURL         string
	EditorURL         string
	CSRFFieldProvider func() Node
}

type sqlAsyncJobRowData struct {
	JobID       string
	URL         string
	Status      string
	Compute     string
	RequestID   string
	RowCount    string
	CreatedAt   string
	CompletedAt string
}

type sqlAsyncJobsListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []sqlAsyncJobRowData
	Page      domain.PageRequest
	Total     int64
}

func sqlAsyncJobsListPage(d sqlAsyncJobsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			Td(A(Href(row.URL), Text(row.JobID))),
			Td(statusLabel(row.Status, sqlAsyncJobTone(row.Status))),
			Td(Text(row.Compute)),
			Td(Text(row.RequestID)),
			Td(Text(row.RowCount)),
			Td(Text(row.CreatedAt)),
			Td(Text(row.CompletedAt)),
		))
	}
	tableNode := Node(emptyStateCard("No async query jobs yet.", "Open SQL editor", "/ui/sql"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Job ID")), Th(Text("Status")), Th(Text("Compute")), Th(Text("Request ID")), Th(Text("Rows")), Th(Text("Created")), Th(Text("Completed")))), TBody(Group(rows))))
	}
	return appPage("Async Query Jobs", "sql", d.Principal, pageToolbar("/ui/sql", "Open SQL editor"), tableNode, paginationCard("/ui/sql/jobs", d.Page, d.Total))
}

func sqlAsyncJobPage(d sqlAsyncJobPageData) Node {
	headers := make([]Node, 0, len(d.Columns))
	for i := range d.Columns {
		headers = append(headers, Th(Text(d.Columns[i])))
	}
	displayRows := d.Rows
	if len(displayRows) > sqlEditorMaxRows {
		displayRows = displayRows[:sqlEditorMaxRows]
	}
	rows := make([]Node, 0, len(displayRows))
	for i := range displayRows {
		cells := make([]Node, 0, len(displayRows[i]))
		for j := range displayRows[i] {
			cells = append(cells, Td(Text(sqlCellString(displayRows[i][j]))))
		}
		rows = append(rows, Tr(Group(cells)))
	}

	resultNode := Node(P(Class(mutedClass()), Text("Results are not available yet.")))
	if len(d.Columns) > 0 {
		resultNode = Div(Class(cardClass(tableWrapClass())), H2(Text("Results")), P(Class(mutedClass()), Text(strconv.Itoa(d.RowCount)+" row(s)")), Table(Class(dataTableClass()), THead(Tr(Group(headers))), TBody(Group(rows))))
	}

	return appPage(
		"Async Query Job",
		"sql",
		d.Principal,
		pageToolbar("/ui/sql/jobs", "Back to jobs"),
		Div(
			Class(cardClass()),
			P(Text("Job ID: "+d.JobID)),
			P(Text("Status: "), statusLabel(d.Status, sqlAsyncJobTone(d.Status))),
			P(Text("Requested compute: "+d.RequestedCompute)),
			P(Text("Resolved compute: "+d.ResolvedCompute)),
			P(Text("Request ID: "+d.RequestID)),
			P(Text("Attempts: "+strconv.Itoa(d.AttemptCount)+"/"+strconv.Itoa(d.MaxAttempts))),
			P(Text("Last heartbeat: "+d.LastHeartbeatText)),
			P(Text("Next retry: "+d.NextRetryText)),
			P(Text("Created: "+d.CreatedAtText)),
			P(Text("Started: "+d.StartedAtText)),
			P(Text("Completed: "+d.CompletedAtText)),
			P(Text("Error: "+d.ErrorText)),
			Div(Class(buttonRowClass()),
				A(Href(d.EditorURL), Class(secondaryButtonClass()), Text("Open in SQL editor")),
				Form(Method("post"), Action(d.CancelURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(secondaryButtonClass()), Text("Cancel job"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete job"))),
			),
		),
		Div(Class(cardClass()), H2(Text("SQL")), Pre(Text(d.SQLText))),
		resultNode,
	)
}

func sqlAsyncJobTone(status string) string {
	switch status {
	case string(domain.QueryJobStatusSucceeded):
		return "success"
	case string(domain.QueryJobStatusFailed), string(domain.QueryJobStatusCanceled):
		return "danger"
	case string(domain.QueryJobStatusRunning), string(domain.QueryJobStatusQueued):
		return "attention"
	default:
		return "neutral"
	}
}
