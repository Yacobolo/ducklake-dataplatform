package api

import "list"

#authenticatedSecurity: [
	{
		ApiKeyAuth: []
	},
	{
		BearerAuth: []
	},
]

#authenticatedExtensions: {
	#cli_command: string

	"security": #authenticatedSecurity
	"x-authz": {
		mode: "authenticated"
	}
	"x-cli-command": #cli_command
}

// High-level authored operation specs. These stay close to API intent and are
// lowered into concrete endpoint objects by the authored CUE modules.
#getResource: {
	kind: "wrapped_resource"

	method:  "get"
	op:      string
	path:    string
	summary: string
	description?: string
	cli?:    string
	deprecated?: *false | true
	returns: string
	params:  [...#Parameter]
}

#postWrapped: {
	kind: "wrapped_mutating"

	method:           "post"
	op:               string
	path:             string
	summary:          string
	description?:     string
	cli?:             string
	deprecated?:      *false | true
	returns:          string
	body_ref:         string
	body_required:    *true | false
	body_description?: string
}

#deleteNoContent: {
	kind: "no_content_mutating"

	method:  "delete"
	op:      string
	path:    string
	summary: string
	description?: string
	cli?:    string
	deprecated?: *false | true
	params:  [...#Parameter]
}

#pathStringParameter: {
	#name: string

	name:     #name
	in:       "path"
	required: true
	schema: {
		type: "string"
	}
}

#queryStringParameter: {
	#name: string

	name:    #name
	in:      "query"
	explode: false
	schema: {
		type: "string"
	}
}

#queryInt32Parameter: {
	#name: string

	name:    #name
	in:      "query"
	explode: false
	schema: {
		type:   "integer"
		format: "int32"
	}
}

#queryInt64Parameter: {
	#name: string

	name:    #name
	in:      "query"
	explode: false
	schema: {
		type:   "integer"
		format: "int64"
	}
}

#pathInt32Parameter: {
	#name: string

	name:     #name
	in:       "path"
	required: true
	schema: {
		type:   "integer"
		format: "int32"
	}
}

#paginationParameters: [
	#queryInt32Parameter & {
		#name: "max_results"
	},
	#queryStringParameter & {
		#name: "page_token"
	},
]

// Shared error/response building blocks used by both the generic DSL and the
// smaller legacy helper path that still powers lineage-style authored specs.
#errorResponse: {
	#status_code: int
	#description: string

	status_code: #status_code
	description: #description
	schema: {
		ref: "Error"
	}
	any_of?: [...#SchemaRef]
}

#mutatingErrorTemplates: [
	{
		status_code: 400
		description: "The server could not understand the request due to invalid syntax."
	},
	{
		status_code: 401
		description: "Access is unauthorized."
	},
	{
		status_code: 403
		description: "Access is forbidden."
	},
	{
		status_code: 429
		description: "Client error"
	},
	{
		status_code: 500
		description: "Server error"
	},
]

#standardErrorTemplates: [
	#mutatingErrorTemplates[0],
	#mutatingErrorTemplates[1],
	#mutatingErrorTemplates[3],
	#mutatingErrorTemplates[4],
]

#standardPlainErrorResponses: [
	#errorResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
	},
	#errorResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
	},
	#errorResponse & {
		#status_code: 429
		#description: "Client error"
	},
	#errorResponse & {
		#status_code: 500
		#description: "Server error"
	},
]

#resourceErrorTemplates: list.Concat([
	#mutatingErrorTemplates,
	[
		{
			status_code: 404
			description: "The server cannot find the requested resource."
		},
	],
])

#lookupErrorTemplates: [
	{
		status_code: 400
		description: "The server could not understand the request due to invalid syntax."
	},
	{
		status_code: 401
		description: "Access is unauthorized."
	},
	{
		status_code: 404
		description: "The server cannot find the requested resource."
	},
	{
		status_code: 429
		description: "Client error"
	},
	{
		status_code: 500
		description: "Server error"
	},
]

#conflictErrorTemplate: {
	status_code: 409
	description: "The request conflicts with the current state of the server."
}

#mutatingErrorResponses: [
	#errorResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
	},
	#errorResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
	},
	#errorResponse & {
		#status_code: 403
		#description: "Access is forbidden."
	},
	#errorResponse & {
		#status_code: 429
		#description: "Client error"
	},
	#errorResponse & {
		#status_code: 500
		#description: "Server error"
	},
]

#wrappedJSONResponse: {
	#status_code: int
	#description: string
	#body_type:   string
	#schema_ref:  string

	status_code: #status_code
	description: #description
	schema: {
		ref: #schema_ref
	}
	any_of?: [...#SchemaRef]
	extensions: {
		"x-apigen-response-shape": {
			body_type: #body_type
			kind:      "wrapped_json"
		}
	}
}

#wrappedJSONSuccessResponse: {
	#body_type: string

	status_code: 200
	description: "The request has succeeded."
	schema: {
		ref: #body_type
	}
	any_of?: [...#SchemaRef]
	extensions: {
		"x-apigen-response-shape": {
			body_type: #body_type
			kind:      "wrapped_json"
		}
	}
}

#wrappedJSONCreatedResponse: {
	#body_type: string

	status_code: 201
	description: "The request has succeeded and a new resource has been created as a result."
	schema: {
		ref: #body_type
	}
	any_of?: [...#SchemaRef]
	extensions: {
		"x-apigen-response-shape": {
			body_type: #body_type
			kind:      "wrapped_json"
		}
	}
}

#wrappedJSONAcceptedResponse: {
	#body_type: string

	status_code: 202
	description: "The request has been accepted for processing, but processing has not yet completed."
	schema: {
		ref: #body_type
	}
	any_of?: [...#SchemaRef]
	extensions: {
		"x-apigen-response-shape": {
			body_type: #body_type
			kind:      "wrapped_json"
		}
	}
}

#authenticatedAuthz: {
	mode: "authenticated"
}

#adminOnlyAuthz: {
	mode: "admin_only"
}

// Generic operation lowering is the main authored DSL path used by the compact
// domain files. Keep new domains on this path unless they benefit from the
// smaller lineage-style helper surface below.
#genericOperationSpec: {
	kind: "response" | "no_content" | "created_empty"

	method:         string
	op:             string
	path:           string
	summary:        string
	description?:   string
	cli?:           string
	returns?:       string
	success_schema?: #SchemaRef
	success_status: *200 | 201 | 202
	wrapped:        *true | false
	error_family:   "standard" | "lookup" | "guarded_read" | "mutating" | "resource" | "mutating_conflict" | "resource_conflict"
	params?:        [...#Parameter]
	body_ref?:      string
	body_required:  *true | false
	body_description?: string
	response_any_of?: [string]: [...#SchemaRef]
	authz_default:  *true | false
	authz?:         _
	security?:      _
	deprecated?:    *false | true
}

#endpointFromGenericOperation: {
	tag:  string
	spec: #genericOperationSpec

	endpoint: {
		method:       spec.method
		path:         spec.path
		operation_id: spec.op
		summary:      spec.summary
		tags:         [tag]
		deprecated:   spec.deprecated
		if spec.description != _|_ {
			description: spec.description
		}
		deprecated:   spec.deprecated
		if spec.description != _|_ {
			description: spec.description
		}
		if spec.params != _|_ {
			parameters: spec.params
		}
		if spec.body_ref != _|_ {
			request_body: {
				required: spec.body_required
				if spec.body_description != _|_ {
					description: spec.body_description
				}
				schema: {
					ref: spec.body_ref
				}
			}
		}
		if spec.kind == "response" {
			if spec.wrapped {
				responses: list.Concat([
					[
						if spec.success_status == 200 {
							#wrappedJSONSuccessResponse & {
								#body_type: spec.returns
								if spec.response_any_of != _|_ && spec.response_any_of["200"] != _|_ {
									any_of: spec.response_any_of["200"]
								}
							}
						},
						if spec.success_status == 201 {
							#wrappedJSONCreatedResponse & {
								#body_type: spec.returns
								if spec.response_any_of != _|_ && spec.response_any_of["201"] != _|_ {
									any_of: spec.response_any_of["201"]
								}
							}
						},
						if spec.success_status == 202 {
							#wrappedJSONAcceptedResponse & {
								#body_type: spec.returns
								if spec.response_any_of != _|_ && spec.response_any_of["202"] != _|_ {
									any_of: spec.response_any_of["202"]
								}
							}
						},
					],
					if spec.error_family == "standard" {
						[
							for template in #standardErrorTemplates {
								#wrappedJSONResponse & {
									#status_code: template.status_code
									#description: template.description
									#schema_ref:  "Error"
									#body_type:   spec.returns
									if spec.response_any_of != _|_ && spec.response_any_of["\(template.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(template.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "lookup" {
						[
							for template in #lookupErrorTemplates {
								#wrappedJSONResponse & {
									#status_code: template.status_code
									#description: template.description
									#schema_ref:  "Error"
									#body_type:   spec.returns
									if spec.response_any_of != _|_ && spec.response_any_of["\(template.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(template.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "guarded_read" || spec.error_family == "mutating" || spec.error_family == "mutating_conflict" {
						[
							for template in #mutatingErrorTemplates {
								#wrappedJSONResponse & {
									#status_code: template.status_code
									#description: template.description
									#schema_ref:  "Error"
									#body_type:   spec.returns
									if spec.response_any_of != _|_ && spec.response_any_of["\(template.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(template.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "resource" || spec.error_family == "resource_conflict" {
						[
							for template in #resourceErrorTemplates {
								#wrappedJSONResponse & {
									#status_code: template.status_code
									#description: template.description
									#schema_ref:  "Error"
									#body_type:   spec.returns
									if spec.response_any_of != _|_ && spec.response_any_of["\(template.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(template.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "mutating_conflict" || spec.error_family == "resource_conflict" {
						[
							#wrappedJSONResponse & {
								#status_code: #conflictErrorTemplate.status_code
								#description: #conflictErrorTemplate.description
								#schema_ref:  "Error"
								#body_type:   spec.returns
								if spec.response_any_of != _|_ && spec.response_any_of["409"] != _|_ {
									any_of: spec.response_any_of["409"]
								}
							},
						]
					},
				])
			}
			if spec.wrapped == false {
				responses: list.Concat([
					[
						{
							status_code: spec.success_status
							if spec.success_status == 200 {
								description: "The request has succeeded."
							}
							if spec.success_status == 201 {
								description: "The request has succeeded and a new resource has been created as a result."
							}
							if spec.success_status == 202 {
								description: "The request has been accepted for processing, but processing has not yet completed."
							}
							if spec.success_schema != _|_ {
								schema: spec.success_schema
							}
							if spec.success_schema == _|_ {
								schema: {
									ref: spec.returns
								}
							}
							if spec.response_any_of != _|_ && spec.response_any_of["\(spec.success_status)"] != _|_ {
								any_of: spec.response_any_of["\(spec.success_status)"]
							}
						},
					],
					if spec.error_family == "standard" {
						[
							for response in #standardPlainErrorResponses {
								response & {
									if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(response.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "lookup" {
						[
							for response in #lookupPlainErrorResponses {
								response & {
									if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(response.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "guarded_read" || spec.error_family == "mutating" || spec.error_family == "mutating_conflict" {
						[
							for response in #mutatingErrorResponses {
								response & {
									if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(response.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "resource" || spec.error_family == "resource_conflict" {
						[
							for response in #resourcePlainErrorResponses {
								response & {
									if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
										any_of: spec.response_any_of["\(response.status_code)"]
									}
								}
							},
						]
					},
					if spec.error_family == "mutating_conflict" || spec.error_family == "resource_conflict" {
						[
							#errorResponse & {
								#status_code: 409
								#description: "The request conflicts with the current state of the server."
								if spec.response_any_of != _|_ && spec.response_any_of["409"] != _|_ {
									any_of: spec.response_any_of["409"]
								}
							},
						]
					},
				])
			}
		}
		if spec.kind == "no_content" {
			responses: list.Concat([
				[
					#noContentResponse & {
						#status_code: 204
						#description: "There is no content to send for this request, but the headers may be useful."
					},
				],
				if spec.error_family == "resource" || spec.error_family == "resource_conflict" {
					[
						for response in #resourcePlainErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "mutating" || spec.error_family == "mutating_conflict" {
					[
						for response in #mutatingErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "resource_conflict" || spec.error_family == "mutating_conflict" {
					[
						#errorResponse & {
							#status_code: 409
							#description: "The request conflicts with the current state of the server."
							if spec.response_any_of != _|_ && spec.response_any_of["409"] != _|_ {
								any_of: spec.response_any_of["409"]
							}
						},
					]
				},
			])
		}
		if spec.kind == "created_empty" {
			responses: list.Concat([
				[
					{
						status_code: 201
						description: "The request has succeeded and a new resource has been created as a result."
					},
				],
				if spec.error_family == "standard" {
					[
						for response in #standardPlainErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "lookup" {
					[
						for response in #lookupPlainErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "guarded_read" || spec.error_family == "mutating" || spec.error_family == "mutating_conflict" {
					[
						for response in #mutatingErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "resource" || spec.error_family == "resource_conflict" {
					[
						for response in #resourcePlainErrorResponses {
							response & {
								if spec.response_any_of != _|_ && spec.response_any_of["\(response.status_code)"] != _|_ {
									any_of: spec.response_any_of["\(response.status_code)"]
								}
							}
						},
					]
				},
				if spec.error_family == "mutating_conflict" || spec.error_family == "resource_conflict" {
					[
						#errorResponse & {
							#status_code: 409
							#description: "The request conflicts with the current state of the server."
							if spec.response_any_of != _|_ && spec.response_any_of["409"] != _|_ {
								any_of: spec.response_any_of["409"]
							}
						},
					]
				},
			])
		}
		extensions: {
			if spec.security != _|_ {
				"security": spec.security
			}
			if spec.security == _|_ {
				"security": #authenticatedSecurity
			}
			if spec.authz != _|_ {
				"x-authz": spec.authz
			}
			if spec.authz == _|_ && spec.authz_default {
				"x-authz": #authenticatedAuthz
			}
			if spec.cli != _|_ {
				"x-cli-command": spec.cli
			}
		}
	}
}

// Legacy compact helpers retained for lineage-style authored specs. These are
// intentionally smaller than the generic operation DSL, but they lower through
// the same shared response/auth conventions.
#endpointFromOperation: {
	tag:  string
	spec: #getResource | #postWrapped | #deleteNoContent

	endpoint: {
		method:       spec.method
		path:         spec.path
		operation_id: spec.op
		summary:      spec.summary
		tags:         [tag]
		if spec.params != _|_ {
			parameters: spec.params
		}
		if spec.kind == "wrapped_resource" {
			responses: list.Concat([
				[
					#wrappedJSONSuccessResponse & {
						#body_type: spec.returns
					},
				],
				[
					for template in #resourceErrorTemplates {
						#wrappedJSONResponse & {
							#status_code: template.status_code
							#description: template.description
							#schema_ref:  "Error"
							#body_type:   spec.returns
						}
					},
				],
			])
		}
		if spec.kind == "wrapped_mutating" {
			request_body: {
				required: spec.body_required
				if spec.body_description != _|_ {
					description: spec.body_description
				}
				schema: {
					ref: spec.body_ref
				}
			}
			responses: list.Concat([
				[
					#wrappedJSONSuccessResponse & {
						#body_type: spec.returns
					},
				],
				[
					for template in #mutatingErrorTemplates {
						#wrappedJSONResponse & {
							#status_code: template.status_code
							#description: template.description
							#schema_ref:  "Error"
							#body_type:   spec.returns
						}
					},
				],
			])
		}
		if spec.kind == "no_content_mutating" {
			responses: list.Concat([
				[
					#noContentResponse & {
						#status_code: 204
						#description: "There is no content to send for this request, but the headers may be useful."
					},
				],
				#mutatingErrorResponses,
			])
		}
		extensions: {
			"security": #authenticatedSecurity
			"x-authz": {
				mode: "authenticated"
			}
			if spec.cli != _|_ {
				"x-cli-command": spec.cli
			}
		}
	}
}

#resourcePlainErrorResponses: [
	#errorResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
	},
	#errorResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
	},
	#errorResponse & {
		#status_code: 403
		#description: "Access is forbidden."
	},
	#errorResponse & {
		#status_code: 404
		#description: "The server cannot find the requested resource."
	},
	#errorResponse & {
		#status_code: 429
		#description: "Client error"
	},
	#errorResponse & {
		#status_code: 500
		#description: "Server error"
	},
]

#lookupPlainErrorResponses: [
	#errorResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
	},
	#errorResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
	},
	#errorResponse & {
		#status_code: 404
		#description: "The server cannot find the requested resource."
	},
	#errorResponse & {
		#status_code: 429
		#description: "Client error"
	},
	#errorResponse & {
		#status_code: 500
		#description: "Server error"
	},
]

#noContentResponse: {
	#status_code: int
	#description: string

	status_code: #status_code
	description: #description
}
