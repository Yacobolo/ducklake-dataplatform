package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func addPrincipalRuntimeOptions(opts *apiruntime.RuntimeOptions) {
	if opts.RunOverrides == nil {
		opts.RunOverrides = map[string]func(*apiruntime.Client) func(*cobra.Command, []string) error{}
	}

	// Override getPrincipal to resolve name→UUID before calling the API.
	opts.RunOverrides["getPrincipal"] = func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			id, err := resolvePrincipalArg(client, args[0])
			if err != nil {
				return err
			}

			resp, err := client.Do("GET", generatedAPIPath("/principals/"+url.PathEscape(id)), nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}

			respBody, err := apiruntime.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}

			outputFlag, _ := cmd.Flags().GetString("output")
			switch apiruntime.OutputFormat(outputFlag) {
			case apiruntime.OutputJSON:
				var pretty interface{}
				if err := json.Unmarshal(respBody, &pretty); err != nil {
					return fmt.Errorf("parse response: %w", err)
				}
				return apiruntime.PrintJSON(os.Stdout, pretty)
			default:
				var data map[string]interface{}
				if err := json.Unmarshal(respBody, &data); err != nil {
					return fmt.Errorf("parse response: %w", err)
				}
				apiruntime.PrintDetail(os.Stdout, data)
			}
			return nil
		}
	}

	// Override deletePrincipal to resolve name→UUID before calling the API.
	opts.RunOverrides["deletePrincipal"] = func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("yes") {
				if !apiruntime.ConfirmPrompt("Are you sure?") {
					return nil
				}
			}

			id, err := resolvePrincipalArg(client, args[0])
			if err != nil {
				return err
			}

			resp, err := client.Do("DELETE", generatedAPIPath("/principals/"+url.PathEscape(id)), nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}

			outputFlag, _ := cmd.Root().PersistentFlags().GetString("output")
			if apiruntime.OutputFormat(outputFlag) == apiruntime.OutputJSON {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			if _, err := fmt.Fprintln(os.Stdout, "Done."); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
			return nil
		}
	}
}

// resolvePrincipalArg resolves a principal argument that may be a name or UUID.
// If the argument looks like a UUID (contains hyphens), it is returned as-is.
// Otherwise, it is treated as a name and resolved via the ListPrincipals API.
func resolvePrincipalArg(client *apiruntime.Client, arg string) (string, error) {
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

		resp, err := client.Do("GET", generatedAPIPath("/principals"), q, nil)
		if err != nil {
			return "", fmt.Errorf("list principals: %w", err)
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return "", fmt.Errorf("list principals: %w", err)
		}

		body, err := apiruntime.ReadBody(resp)
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
