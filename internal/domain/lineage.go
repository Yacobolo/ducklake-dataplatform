package domain

import "time"

// LineageEdge represents a relationship between tables discovered from query execution.
type LineageEdge struct {
	ID            int64
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
