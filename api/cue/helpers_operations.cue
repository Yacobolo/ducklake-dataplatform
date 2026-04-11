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
	cli:     string
	returns: string
	params:  [...#Parameter]
}

#postWrapped: {
	kind: "wrapped_mutating"

	method:           "post"
	op:               string
	path:             string
	summary:          string
	cli:              string
	returns:          string
	body_ref:         string
	body_required?:   bool
	body_description?: string
}

#deleteNoContent: {
	kind: "no_content_mutating"

	method:  "delete"
	op:      string
	path:    string
	summary: string
	cli:     string
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

#paginationParameters: [
	#queryInt32Parameter & {
		#name: "max_results"
	},
	#queryStringParameter & {
		#name: "page_token"
	},
]

#errorResponse: {
	#status_code: int
	#description: string

	status_code: #status_code
	description: #description
	schema: {
		ref: "Error"
	}
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

#resourceErrorTemplates: list.Concat([
	#mutatingErrorTemplates,
	[
		{
			status_code: 404
			description: "The server cannot find the requested resource."
		},
	],
])

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
	extensions: {
		"x-apigen-response-shape": {
			body_type: #body_type
			kind:      "wrapped_json"
		}
	}
}

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
				required: *true | bool
				required: *true | spec.body_required
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
		extensions: #authenticatedExtensions & {
			#cli_command: spec.cli
		}
	}
}

#noContentResponse: {
	#status_code: int
	#description: string

	status_code: #status_code
	description: #description
}
