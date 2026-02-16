package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func init() {
	// Override getPrincipal to resolve name→UUID before calling the API.
	gen.RegisterRunOverride("getPrincipal", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			id, err := resolvePrincipalArg(client, args[0])
			if err != nil {
				return err
			}

			resp, err := client.Do("GET", "/principals/"+url.PathEscape(id), nil, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}

			respBody, err := gen.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}

			outputFlag, _ := cmd.Flags().GetString("output")
			switch gen.OutputFormat(outputFlag) {
			case gen.OutputJSON:
				var pretty interface{}
				json.Unmarshal(respBody, &pretty)
				return gen.PrintJSON(os.Stdout, pretty)
			default:
				var data map[string]interface{}
				if err := json.Unmarshal(respBody, &data); err != nil {
					return fmt.Errorf("parse response: %w", err)
				}
				gen.PrintDetail(os.Stdout, data)
			}
			return nil
		}
	})

	// Override deletePrincipal to resolve name→UUID before calling the API.
	gen.RegisterRunOverride("deletePrincipal", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("yes") {
				if !gen.ConfirmPrompt("Are you sure?") {
					return nil
				}
			}

			id, err := resolvePrincipalArg(client, args[0])
			if err != nil {
				return err
			}

			resp, err := client.Do("DELETE", "/principals/"+url.PathEscape(id), nil, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}

			outputFlag, _ := cmd.Root().PersistentFlags().GetString("output")
			if gen.OutputFormat(outputFlag) == gen.OutputJSON {
				return gen.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			fmt.Fprintln(os.Stdout, "Done.")
			return nil
		}
	})
}

// resolvePrincipalArg resolves a principal argument that may be a name or UUID.
// If the argument looks like a UUID (contains hyphens), it is returned as-is.
// Otherwise, it is treated as a name and resolved via the ListPrincipals API.
func resolvePrincipalArg(client *gen.Client, arg string) (string, error) {
	// Heuristic: UUIDs contain hyphens, names typically don't.
	// If it looks like a UUID, use it directly.
	if isLikelyUUID(arg) {
		return arg, nil
	}

	// Resolve name to UUID via ListPrincipals with pagination.
	var pageToken string
	for {
		q := url.Values{}
		q.Set("max_results", "100")
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}

		resp, err := client.Do("GET", "/principals", q, nil)
		if err != nil {
			return "", fmt.Errorf("list principals: %w", err)
		}
		if err := gen.CheckError(resp); err != nil {
			return "", fmt.Errorf("list principals: %w", err)
		}

		body, err := gen.ReadBody(resp)
		if err != nil {
			return "", fmt.Errorf("read principals response: %w", err)
		}

		var result struct {
			Data []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"data"`
			NextPageToken *string `json:"next_page_token"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("parse principals response: %w", err)
		}

		for _, p := range result.Data {
			if p.Name == arg {
				return p.ID, nil
			}
		}

		if result.NextPageToken == nil || *result.NextPageToken == "" {
			break
		}
		pageToken = *result.NextPageToken
	}

	return "", fmt.Errorf("principal %q not found", arg)
}

// isLikelyUUID returns true if the string looks like a UUID (contains hyphens
// and is the right length).
func isLikelyUUID(s string) bool {
	if len(s) < 32 {
		return false
	}
	hyphens := 0
	for _, c := range s {
		if c == '-' {
			hyphens++
		}
	}
	return hyphens >= 4
}
