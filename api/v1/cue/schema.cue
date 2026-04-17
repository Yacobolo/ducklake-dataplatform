package api

// #Source defines the authored CUE contract consumed by APIGen.
#SchemaRef: {
	ref?: string
	type?: string
	format?: string
	items?: #SchemaRef
	additional_properties?: #AdditionalProperties
}

#AdditionalProperties: {
	any?: bool
	schema?: #SchemaRef
}

#SchemaProperty: {
	description?: string
	example?: _
	schema: #SchemaRef
}

#Schema: {
	type: string
	title?: string
	description?: string
	example?: _
	properties?: [string]: #SchemaProperty
	property_order?: [...string]
	required?: [...string]
	items?: #SchemaRef
	enum?: [...string]
}

#Header: {
	name: string
	required?: bool
	description?: string
	schema: #SchemaRef
}

#Response: {
	status_code: int
	description: string
	headers?: [...#Header]
	content_type?: string
	example?: _
	schema?: #SchemaRef
	any_of?: [...#SchemaRef]
	extensions?: [string]: _
}

#Parameter: {
	name: string
	in: string
	required?: bool
	description?: string
	example?: _
	explode?: bool
	schema: #SchemaRef
}

#RequestBody: {
	required?: bool
	description?: string
	content_type?: string
	example?: _
	schema: #SchemaRef
}

#SecurityRequirement: [string]: [...string]

#SecurityScheme: {
	type: string
	in?: string
	name?: string
	scheme?: string
}

#ServerVariable: {
	default?: string
	description?: string
	enum?: [...string]
}

#Server: {
	url: string
	description?: string
	variables?: [string]: #ServerVariable
}

#OpenAPI: {
	version?: string
	tag_order?: [...string]
	security?: [...#SecurityRequirement]
	security_schemes?: [string]: #SecurityScheme
}

#Endpoint: {
	method: string
	path: string
	operation_id: string
	summary?: string
	description?: string
	tags?: [...string]
	parameters?: [...#Parameter]
	request_body?: #RequestBody
	responses: [...#Response]
	extensions?: [string]: _
}

#Source: {
	schema_version: string
	api: {
		base_path: string
	}
	info: {
		title: string
		version: string
		description?: string
	}
	openapi?: #OpenAPI
	servers?: [...#Server]
	tags?: [..._]
	schemas?: [string]: #Schema
	endpoints: [...#Endpoint]
	extensions?: [string]: _
}
