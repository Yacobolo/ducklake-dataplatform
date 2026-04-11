package querydefs

#CountAll: {
	name:   string
	_table: string
	kind:   "one"
	result: {scalar: "int64"}
	select: {
		from: _table
		columns: [{expr: "COUNT(*)", alias: "cnt"}]
		...
	}
	...
}

#GetByID: {
	name:   string
	_table: string
	kind:   "one"
	params: [{name: "id", type: "string"}]
	result: {table: _table}
	select: {
		from: _table
		where: [{column: "id", op: "=", param: "id"}]
		...
	}
	...
}

#DeleteByID: {
	name:   string
	_table: string
	kind:   "exec"
	params: [{name: "id", type: "string"}]
	delete: {
		from:  _table
		where: [{column: "id", op: "=", param: "id"}]
		...
	}
	...
}

#GetByStringField: {
	name:   string
	_table: string
	_field: string
	_param: string
	kind:   "one"
	params: [{name: _param, type: "string"}]
	result: {table: _table}
	select: {
		from: _table
		where: [{column: _field, op: "=", param: _param}]
		...
	}
	...
}

#GetByTwoStringFields: {
	name:    string
	_table:  string
	_field1: string
	_param1: string
	_field2: string
	_param2: string
	kind:    "one"
	params: [
		{name: _param1, type: "string"},
		{name: _param2, type: "string"},
	]
	result: {table: _table}
	select: {
		from: _table
		where: [
			{column: _field1, op: "=", param: _param1},
			{column: _field2, op: "=", param: _param2},
		]
		...
	}
	...
}

#ListAllOrdered: {
	name:   string
	_table: string
	_order: [..._]
	kind:   "many"
	result: {table: _table}
	select: {
		from:    _table
		orderBy: _order
		...
	}
	...
}

#CountFiltered: {
	name:    string
	_table:  string
	_params: [..._]
	_where:  [..._]
	kind:    "one"
	params:  _params
	result:  {scalar: "int64"}
	select: {
		from:    _table
		columns: [{expr: "COUNT(*)", alias: "cnt"}]
		where:   _where
		...
	}
	...
}

#ListPaginatedOrdered: {
	name:   string
	_table: string
	_order: [..._]
	kind:   "many"
	params: [
		{name: "Limit", type: "int64"},
		{name: "Offset", type: "int64"},
	]
	result: {table: _table}
	select: {
		from:        _table
		orderBy:     _order
		limitParam:  "Limit"
		offsetParam: "Offset"
		...
	}
	...
}

#ListFilteredPaginatedOrdered: {
	name:    string
	_table:  string
	_params: [..._]
	_where:  [..._]
	_order:  [..._]
	kind:    "many"
	params: [
		for p in _params {p},
		{name: "Limit", type: "int64"},
		{name: "Offset", type: "int64"},
	]
	result: {table: _table}
	select: {
		from:        _table
		where:       _where
		orderBy:     _order
		limitParam:  "Limit"
		offsetParam: "Offset"
		...
	}
	...
}

#InsertReturningTable: {
	name:   string
	_table: string
	kind:   "one"
	result: {table: _table}
	insert: {
		into:      _table
		returning: true
		...
	}
	...
}

#UpdateByIDTouch: {
	name:   string
	_table: string
	_kind:  "exec" | "one" | *"exec"
	_set:   [..._]
	kind:   _kind
	params: [..._]
	update: {
		table: _table
		set: [
			for item in _set {item},
			{column: "updated_at", value: {sql: "datetime('now')"}},
		]
		where: [{column: "id", op: "=", param: "ID"}]
	}
	if _kind == "one" {
		result: {table: _table}
		update: returning: true
	}
	...
}
