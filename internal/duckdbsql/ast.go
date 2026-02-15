package duckdbsql

// Node is the base interface for all AST nodes.
type Node interface {
	node()
}

// Expr is a marker interface for expression nodes.
type Expr interface {
	Node
	exprNode()
}

// Stmt is a marker interface for statement nodes.
type Stmt interface {
	Node
	stmtNode()
}

// TableRef is a marker interface for table reference nodes.
type TableRef interface {
	Node
	tableRefNode()
}
