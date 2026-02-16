package cli

// ResponsePattern classifies the response of an operation.
type ResponsePattern int

// Response pattern constants for classifying API responses.
const (
	PaginatedList  ResponsePattern = iota // data array + next_page_token
	SingleResource                        // 200/201 with named $ref
	NoContent                             // 204 only
	CustomResult                          // fallback
	ArrayResult                           // bare JSON array response
)

func (p ResponsePattern) String() string {
	switch p {
	case PaginatedList:
		return "PaginatedList"
	case SingleResource:
		return "SingleResource"
	case NoContent:
		return "NoContent"
	case CustomResult:
		return "CustomResult"
	case ArrayResult:
		return "ArrayResult"
	default:
		return "Unknown"
	}
}

// GroupModel represents a top-level CLI command group.
type GroupModel struct {
	Name     string         // e.g. "catalog"
	Short    string         // short description
	Commands []CommandModel // commands in this group
}

// CommandModel is the intermediate representation of a CLI command.
type CommandModel struct {
	OperationID    string
	GroupName      string
	CommandPath    []string // e.g. ["schemas"]
	Verb           string   // e.g. "list", "create", "get", "delete", "update"
	Use            string   // computed Cobra Use string
	Short          string   // from spec summary
	Long           string   // from spec description
	Examples       []string // from config
	Method         string   // HTTP method: GET, POST, PUT, PATCH, DELETE
	URLPath        string   // e.g. "/v1/catalog/schemas"
	PathParams     []ParamModel
	QueryParams    []ParamModel
	BodyFields     []FieldModel // flattened request body fields
	HasBody        bool
	PositionalArgs []string    // parameter names that are positional
	Flags          []FlagModel // all non-positional parameters/fields
	Confirm        bool
	Response       ResponseModel
	FlattenFields  []string
	CompoundFlags  map[string]CompoundFlagConfig
}

// ParamModel represents a path or query parameter.
type ParamModel struct {
	Name     string
	GoName   string // PascalCase
	Type     string // "string", "integer", "boolean"
	GoType   string // "string", "int64", "bool"
	Required bool
	Default  string
	In       string // "path", "query"
}

// FieldModel represents a request body field.
type FieldModel struct {
	Name         string
	GoName       string
	Type         string // "string", "integer", "boolean", "array", "object"
	GoType       string
	Required     bool
	Default      string
	IsArray      bool
	IsMap        bool
	IsNested     bool
	NestedFields []FieldModel
}

// FlagModel represents a CLI flag.
type FlagModel struct {
	Name           string // kebab-case flag name
	Short          string // single-char alias
	GoName         string // PascalCase
	GoType         string // "string", "int64", "bool", "[]string"
	CobraType      string // "String", "Int64", "Bool", "StringSlice", "StringToString"
	Required       bool
	Default        string
	Usage          string
	IsBody         bool   // true if this flag maps to a request body field
	FieldName      string // original field/param name
	IsCompound     bool
	CompoundFields []string
	CompoundSep    string
}

// ResponseModel describes how to handle the command's response.
type ResponseModel struct {
	Pattern      ResponsePattern
	GoTypeName   string   // e.g. "PaginatedSchemaDetails"
	ItemTypeName string   // for paginated: "SchemaDetail"
	TableColumns []string // from config
	SuccessCode  int
}

// APIEndpointModel represents a single API endpoint for the api registry.
type APIEndpointModel struct {
	OperationID string
	Method      string
	Path        string
	Summary     string
	Description string
	Tags        []string
	Parameters  []APIParamModel
	BodyFields  []APIFieldModel
	CLICommand  string // corresponding CLI command path, e.g. "catalog schemas create"
}

// APIParamModel represents a parameter in the API registry.
type APIParamModel struct {
	Name     string
	In       string // "path", "query"
	Type     string
	Required bool
	Enum     []string
}

// APIFieldModel represents a request body field in the API registry.
type APIFieldModel struct {
	Name     string
	Type     string
	Required bool
	Enum     []string
}
