package cli

import (
	"github.com/spf13/cobra"

	cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

var generatedCommandsErr error

func addRuntimeGeneratedCommands(rootCmd *cobra.Command, client *apiruntime.Client) {
	if err := cobraruntime.AddGeneratedCommands(rootCmd, client, generatedRuntimeEndpoints(allAPIEndpoints())); err != nil {
		generatedCommandsErr = err
	}
}

func generatedRuntimeEndpoints(endpoints []gen.ReferenceOperation) []cobraruntime.Endpoint {
	converted := make([]cobraruntime.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		params := make([]cobraruntime.Param, 0, len(endpoint.Parameters))
		for _, parameter := range endpoint.Parameters {
			params = append(params, cobraruntime.Param{
				Name:        parameter.Name,
				In:          parameter.In,
				Type:        parameter.Type,
				Description: parameter.Description,
				Required:    parameter.Required,
				Enum:        append([]string(nil), parameter.Enum...),
			})
		}

		fields := make([]cobraruntime.Field, 0, len(endpoint.BodyFields))
		for _, field := range endpoint.BodyFields {
			fields = append(fields, cobraruntime.Field{
				Name:        field.Name,
				Type:        field.Type,
				Description: field.Description,
				Required:    field.Required,
				Enum:        append([]string(nil), field.Enum...),
			})
		}

		converted = append(converted, cobraruntime.Endpoint{
			OperationID: endpoint.OperationID,
			Method:      endpoint.Method,
			Path:        endpoint.Path,
			Summary:     endpoint.Summary,
			Description: endpoint.Description,
			Tags:        append([]string(nil), endpoint.Tags...),
			Parameters:  params,
			BodyFields:  fields,
			CLICommand:  endpoint.CLICommand,
		})
	}

	return converted
}
