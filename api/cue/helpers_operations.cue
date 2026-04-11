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

#authenticatedExtensionsFor: {
	cli_command: string

	value: #authenticatedExtensions & {
		#cli_command: cli_command
	}
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

#wrappedJSONMutatingErrorResponses: {
	#body_type: string

	responses: [
		#wrappedJSONResponse & {
			#status_code: 400
			#description: "The server could not understand the request due to invalid syntax."
			#schema_ref:  "Error"
			#body_type:   #body_type
		},
		#wrappedJSONResponse & {
			#status_code: 401
			#description: "Access is unauthorized."
			#schema_ref:  "Error"
			#body_type:   #body_type
		},
		#wrappedJSONResponse & {
			#status_code: 403
			#description: "Access is forbidden."
			#schema_ref:  "Error"
			#body_type:   #body_type
		},
		#wrappedJSONResponse & {
			#status_code: 429
			#description: "Client error"
			#schema_ref:  "Error"
			#body_type:   #body_type
		},
		#wrappedJSONResponse & {
			#status_code: 500
			#description: "Server error"
			#schema_ref:  "Error"
			#body_type:   #body_type
		},
	]
}

#wrappedJSONResourceErrorResponses: {
	#body_type: string

	responses: list.Concat([
		(#wrappedJSONMutatingErrorResponses & {
			#body_type: #body_type
		}).responses,
		[
			#wrappedJSONResponse & {
				#status_code: 404
				#description: "The server cannot find the requested resource."
				#schema_ref:  "Error"
				#body_type:   #body_type
			},
		],
	])
}


#noContentResponse: {
	#status_code: int
	#description: string

	status_code: #status_code
	description: #description
}


#authenticatedWrappedResourceOperation: #Endpoint & {
	#method:       string
	#path:         string
	#operation_id: string
	#summary:      string
	#tag:          string
	#cli_command:  string
	#body_type:    string

	method:       #method
	path:         #path
	operation_id: #operation_id
	summary:      #summary
	tags:         [#tag]
	extensions: #authenticatedExtensions & {
		#cli_command: #cli_command
	}
	responses: list.Concat([
		[
			#wrappedJSONSuccessResponse & {
				#body_type: #body_type
			},
		],
		(#wrappedJSONResourceErrorResponses & {
			#body_type: #body_type
		}).responses,
	])
}

#authenticatedWrappedMutatingOperation: #Endpoint & {
	#method:       string
	#path:         string
	#operation_id: string
	#summary:      string
	#tag:          string
	#cli_command:  string
	#body_type:    string

	method:       #method
	path:         #path
	operation_id: #operation_id
	summary:      #summary
	tags:         [#tag]
	extensions: #authenticatedExtensions & {
		#cli_command: #cli_command
	}
	responses: list.Concat([
		[
			#wrappedJSONSuccessResponse & {
				#body_type: #body_type
			},
		],
		(#wrappedJSONMutatingErrorResponses & {
			#body_type: #body_type
		}).responses,
	])
}

#authenticatedNoContentMutatingOperation: #Endpoint & {
	#method:       string
	#path:         string
	#operation_id: string
	#summary:      string
	#tag:          string
	#cli_command:  string

	method:       #method
	path:         #path
	operation_id: #operation_id
	summary:      #summary
	tags:         [#tag]
	extensions: #authenticatedExtensions & {
		#cli_command: #cli_command
	}
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
