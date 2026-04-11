package api

#stringProperty: {
	schema: {
		type: "string"
	}
}

#idProperty:          #stringProperty
#nameProperty:        #stringProperty
#descriptionProperty: #stringProperty
#commentProperty:     #stringProperty
#ownerProperty:       #stringProperty
#statusProperty:      #stringProperty

#boolProperty: {
	schema: {
		type: "boolean"
	}
}

#enabledProperty: #boolProperty

#dateTimeProperty: {
	schema: {
		type:   "string"
		format: "date-time"
	}
}

#createdAtProperty: #dateTimeProperty
#updatedAtProperty: #dateTimeProperty
#expiresAtProperty: #dateTimeProperty

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

#numberProperty: {
	schema: {
		type: "number"
	}
}

#refProperty: {
	#ref: string

	schema: {
		ref: #ref
	}
}

#principalIDProperty:   #stringProperty
#principalNameProperty: #stringProperty

#arrayProperty: {
	#items: #SchemaRef

	schema: {
		type: "array"
		items: #items
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

#stringArrayProperty: {
	schema: {
		type: "array"
		items: {
			type: "string"
		}
	}
}

#stringMapProperty: {
	schema: {
		type: "object"
		additional_properties: {
			schema: {
				type: "string"
			}
		}
	}
}

#objectSchema: {
	type: "object"
	title?: string
	description?: string
	#fields: [string]: #SchemaProperty
	properties: #fields
	#required?: [...string]
	if #required != _|_ {
		required: #required
	}
}

#enumSchema: {
	type: "string"
	title?: string
	description?: string
	#values: [...string]
	enum: #values
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
