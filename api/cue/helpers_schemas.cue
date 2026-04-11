package api

#stringProperty: {
	schema: {
		type: "string"
	}
}

#int32Property: {
	schema: {
		type:   "integer"
		format: "int32"
	}
}

#int64Property: {
	schema: {
		type:   "integer"
		format: "int64"
	}
}

#arrayRefProperty: {
	#ref: string

	schema: {
		type: "array"
		items: {
			ref: #ref
		}
	}
}

#paginatedItemsSchema: {
	#item_ref: string

	type:           "object"
	property_order: ["data", "next_page_token"]
	properties: {
		"data": #arrayRefProperty & {
			#ref: #item_ref
		}
		"next_page_token": #stringProperty
	}
	required: ["data"]
}
