package api

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
	schema: #SchemaRef
}

#Schema: {
	type: string
	title?: string
	description?: string
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
	schema?: #SchemaRef
	extensions?: [string]: _
}

#Parameter: {
	name: string
	in: string
	required?: bool
	description?: string
	explode?: bool
	schema: #SchemaRef
}

#RequestBody: {
	required?: bool
	description?: string
	content_type?: string
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

#Source: {
	schema_version: string
	info: {
		title: string
		version: string
		description?: string
	}
	openapi?: #OpenAPI
	servers?: [...#Server]
	tags?: [..._]
	schemas?: [string]: #Schema
	openapi_extra_schemas?: [string]: #Schema
	endpoints: [...#Endpoint]
	openapi_extra_endpoints?: [...#Endpoint]
	extensions?: [string]: _
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
