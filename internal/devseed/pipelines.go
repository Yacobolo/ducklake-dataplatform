package devseed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	apitypes "github.com/Yacobolo/quackstack/internal/api"
	"github.com/Yacobolo/quackstack/pkg/cli"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

const (
	defaultSeedNotebookOwner       = "dev_admin"
	defaultSeedNotebookName        = "nyc_taxi_explore"
	defaultSeedPipelineName        = "nyc_taxi_demo"
	defaultSeedPipelineDescription = "Seeded demo pipeline for the NYC taxi dev workspace"
	defaultSeedPipelineJobName     = "run_nyc_taxi_explore"
)

// SeedPipelinesOptions configures demo pipeline creation for task dev:seeded.
type SeedPipelinesOptions struct {
	Host                string
	Profile             string
	NotebookOwner       string
	NotebookName        string
	JobName             string
	PipelineName        string
	PipelineDescription string
	TriggerRun          bool
}

// SeededPipelines describes the created or discovered dev-seeded pipeline resources.
type SeededPipelines struct {
	NotebookID string
	PipelineID string
	JobID      string
	RunID      string
}

// SeedPipelines creates a stable demo pipeline around the seeded notebook.
func SeedPipelines(ctx context.Context, opts SeedPipelinesOptions) (*SeededPipelines, error) {
	client, err := pipelineSeedClient(opts)
	if err != nil {
		return nil, err
	}

	notebookOwner := strings.TrimSpace(opts.NotebookOwner)
	if notebookOwner == "" {
		notebookOwner = defaultSeedNotebookOwner
	}
	notebookName := strings.TrimSpace(opts.NotebookName)
	if notebookName == "" {
		notebookName = defaultSeedNotebookName
	}
	pipelineName := strings.TrimSpace(opts.PipelineName)
	if pipelineName == "" {
		pipelineName = defaultSeedPipelineName
	}
	pipelineDescription := strings.TrimSpace(opts.PipelineDescription)
	if pipelineDescription == "" {
		pipelineDescription = defaultSeedPipelineDescription
	}
	jobName := strings.TrimSpace(opts.JobName)
	if jobName == "" {
		jobName = defaultSeedPipelineJobName
	}

	notebook, err := findNotebookByOwnerAndName(ctx, client, notebookOwner, notebookName)
	if err != nil {
		return nil, err
	}

	pipelineResource, err := ensurePipeline(ctx, client, pipelineName, pipelineDescription, notebook.FolderId)
	if err != nil {
		return nil, err
	}

	job, err := ensurePipelineJob(ctx, client, pipelineName, notebook, jobName)
	if err != nil {
		return nil, err
	}

	result := &SeededPipelines{
		NotebookID: requiredAPIString(notebook.Id),
		PipelineID: requiredAPIString(pipelineResource.Id),
		JobID:      requiredAPIString(job.Id),
	}

	if opts.TriggerRun {
		run, err := ensureInitialRun(ctx, client, pipelineName)
		if err != nil {
			return nil, err
		}
		if run != nil {
			result.RunID = requiredAPIString(run.Id)
		}
	}

	return result, nil
}

func pipelineSeedClient(opts SeedPipelinesOptions) (*apiruntime.Client, error) {
	cfg, err := cli.LoadUserConfig()
	if err != nil {
		return nil, fmt.Errorf("load CLI auth config: %w", err)
	}

	profile, err := cfg.ActiveProfile(strings.TrimSpace(opts.Profile))
	if err != nil {
		return nil, fmt.Errorf("resolve CLI profile: %w", err)
	}

	host := strings.TrimSpace(opts.Host)
	if host == "" {
		host = strings.TrimSpace(profile.Host)
	}
	if host == "" {
		return nil, fmt.Errorf("seed pipelines: host is required")
	}

	return apiruntime.NewClient(host, profile.APIKey, profile.Token), nil
}

func findNotebookByOwnerAndName(ctx context.Context, client *apiruntime.Client, owner, name string) (*apitypes.Notebook, error) {
	pageToken := ""
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		query := url.Values{}
		query.Set("owner", owner)
		query.Set("max_results", "200")
		if pageToken != "" {
			query.Set("page_token", pageToken)
		}

		var notebooks apitypes.PaginatedNotebooks
		if err := apiRequest(client, http.MethodGet, "/notebooks", query, nil, &notebooks); err != nil {
			return nil, fmt.Errorf("list notebooks: %w", err)
		}
		for i := range notebooks.Data {
			notebook := notebooks.Data[i]
			if strings.EqualFold(requiredAPIString(notebook.Name), name) {
				if requiredAPIString(notebook.Id) == "" {
					return nil, fmt.Errorf("seed notebook %q is missing an ID", name)
				}
				return &notebook, nil
			}
		}
		if notebooks.NextPageToken == nil || strings.TrimSpace(*notebooks.NextPageToken) == "" {
			break
		}
		pageToken = strings.TrimSpace(*notebooks.NextPageToken)
	}

	return nil, fmt.Errorf("seed notebook %q owned by %q not found", name, owner)
}

func ensurePipeline(ctx context.Context, client *apiruntime.Client, pipelineName, description string, folderID *string) (*apitypes.Pipeline, error) {
	var pipelineResource apitypes.Pipeline
	err := apiRequest(client, http.MethodGet, "/pipelines/"+url.PathEscape(pipelineName), nil, nil, &pipelineResource)
	if err == nil {
		return &pipelineResource, nil
	}

	var apiErr *apiruntime.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusNotFound {
		return nil, fmt.Errorf("get pipeline %q: %w", pipelineName, err)
	}

	req := apitypes.CreatePipelineRequest{
		Name:             pipelineName,
		Description:      stringPtr(description),
		FolderId:         folderID,
		ConcurrencyLimit: int32Ptr(1),
	}
	if err := apiRequest(client, http.MethodPost, "/pipelines", nil, req, &pipelineResource); err != nil {
		return nil, fmt.Errorf("create pipeline %q: %w", pipelineName, err)
	}
	return &pipelineResource, nil
}

func ensurePipelineJob(ctx context.Context, client *apiruntime.Client, pipelineName string, notebook *apitypes.Notebook, jobName string) (*apitypes.PipelineJob, error) {
	var jobs apitypes.PipelineJobList
	if err := apiRequest(client, http.MethodGet, "/pipelines/"+url.PathEscape(pipelineName)+"/jobs", nil, nil, &jobs); err != nil {
		return nil, fmt.Errorf("list pipeline jobs for %q: %w", pipelineName, err)
	}

	for i := range jobs.Data {
		job := jobs.Data[i]
		if strings.EqualFold(requiredAPIString(job.Name), jobName) {
			return &job, nil
		}
	}

	req := apitypes.CreatePipelineJobRequest{
		Name:       jobName,
		NotebookId: notebook.Id,
		JobType:    pipelineJobTypePtr(apitypes.PipelineJobJobTypeNOTEBOOK),
		JobOrder:   int32Ptr(1),
		RetryCount: int32Ptr(0),
	}

	var job apitypes.PipelineJob
	if err := apiRequest(client, http.MethodPost, "/pipelines/"+url.PathEscape(pipelineName)+"/jobs", nil, req, &job); err != nil {
		return nil, fmt.Errorf("create pipeline job %q: %w", jobName, err)
	}
	return &job, nil
}

func ensureInitialRun(ctx context.Context, client *apiruntime.Client, pipelineName string) (*apitypes.PipelineRun, error) {
	query := url.Values{}
	query.Set("max_results", "1")

	var runs apitypes.PaginatedPipelineRuns
	if err := apiRequest(client, http.MethodGet, "/pipelines/"+url.PathEscape(pipelineName)+"/runs", query, nil, &runs); err != nil {
		return nil, fmt.Errorf("list pipeline runs for %q: %w", pipelineName, err)
	}
	if len(runs.Data) > 0 {
		return &runs.Data[0], nil
	}

	var run apitypes.PipelineRun
	if err := apiRequest(client, http.MethodPost, "/pipelines/"+url.PathEscape(pipelineName)+"/runs", nil, map[string]any{}, &run); err != nil {
		return nil, fmt.Errorf("trigger pipeline run for %q: %w", pipelineName, err)
	}
	return &run, nil
}

func apiRequest(client *apiruntime.Client, method, path string, query url.Values, body any, out any) error {
	resp, err := client.Do(method, path, query, body)
	if err != nil {
		return err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return err
	}
	if out == nil {
		_, _ = apiruntime.ReadBody(resp)
		return nil
	}

	payload, err := apiruntime.ReadBody(resp)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return fmt.Errorf("empty response body for %s %s", method, path)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}

func requiredAPIString(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func stringPtr(v string) *string {
	return &v
}

func int32Ptr(v int32) *int32 {
	return &v
}

func pipelineJobTypePtr(v apitypes.PipelineJobJobType) *apitypes.PipelineJobJobType {
	return &v
}
