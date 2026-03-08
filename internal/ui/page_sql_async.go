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
		resultNode = Div(Class(cardClass("table-wrap")), H2(Text("Results")), P(Class(mutedClass()), Text(strconv.Itoa(d.RowCount)+" row(s)")), Table(Class("data-table"), THead(Tr(Group(headers))), TBody(Group(rows))))
	}

	return appPage(
		"Async Query Job",
		"sql",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Job ID: "+d.JobID)),
			P(Text("Status: "), statusLabel(d.Status, sqlAsyncJobTone(d.Status))),
			P(Text("Request ID: "+d.RequestID)),
			P(Text("Attempts: "+strconv.Itoa(d.AttemptCount)+"/"+strconv.Itoa(d.MaxAttempts))),
			P(Text("Last heartbeat: "+d.LastHeartbeatText)),
			P(Text("Next retry: "+d.NextRetryText)),
			P(Text("Created: "+d.CreatedAtText)),
			P(Text("Started: "+d.StartedAtText)),
			P(Text("Completed: "+d.CompletedAtText)),
			P(Text("Error: "+d.ErrorText)),
			Div(Class("BtnGroup"),
				A(Href(d.EditorURL), Class(secondaryButtonClass()), Text("Open in SQL editor")),
				Form(Method("post"), Action(d.CancelURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(secondaryButtonClass()), Text("Cancel job"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete job"))),
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
