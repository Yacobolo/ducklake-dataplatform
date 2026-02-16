package domain

import "time"

// LineageEdge represents a relationship between tables discovered from query execution.
type LineageEdge struct {
	ID            string
	SourceTable   string
	TargetTable   *string
	SourceSchema  string
	TargetSchema  string
	EdgeType      string // "READ", "WRITE", "READ_WRITE"
	PrincipalName string
	QueryHash     *string
	CreatedAt     time.Time
}

// LineageNode represents a table and its upstream/downstream lineage edges.
type LineageNode struct {
	TableName  string
	Upstream   []LineageEdge
	Downstream []LineageEdge
}

// === Column-Level Lineage ===

// TransformType classifies how a source column contributes to a target column.
type TransformType string

const (
	// TransformDirect means the output column is a direct pass-through of the source.
	TransformDirect TransformType = "DIRECT"
	// TransformExpression means the output column is computed from an expression.
	TransformExpression TransformType = "EXPRESSION"
)

// ColumnLineageEntry is the analyzer output for one output column.
// It groups all source references for a single target column.
type ColumnLineageEntry struct {
	TargetColumn  string
	TransformType TransformType
	Function      string // e.g. "SUM", "CASE", "" for DIRECT
	Sources       []ColumnSource
}

// ColumnSource identifies a single source column.
type ColumnSource struct {
	Schema string
	Table  string
	Column string
}

// ColumnLineageEdge is a persisted column-level lineage record.
type ColumnLineageEdge struct {
	ID            int64
	LineageEdgeID string // FK to LineageEdge.ID
	TargetColumn  string
	SourceSchema  string
	SourceTable   string
	SourceColumn  string
	TransformType TransformType
	Function      string
}
