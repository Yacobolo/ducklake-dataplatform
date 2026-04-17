package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (s *Service) buildRunProvenance(ctx context.Context, pipelineDef *domain.Pipeline, jobs []domain.PipelineJob, triggerType, triggeredBy, effectivePrincipal string) (*domain.PipelineRunProvenance, *string, error) {
	provenance := &domain.PipelineRunProvenance{
		TriggerType:               triggerType,
		TriggeredBy:               triggeredBy,
		EffectivePrincipal:        effectivePrincipal,
		PipelineDefinitionVersion: pipelineDef.UpdatedAt.UTC().Format(timeLayoutRFC3339),
		Notebooks:                 []domain.PipelineNotebookProvenance{},
		Models:                    []domain.PipelineModelProvenance{},
	}

	var gitCommitHash *string
	for _, job := range jobs {
		switch job.JobType {
		case "", domain.PipelineJobTypeNotebook:
			entry, commitSHA, err := s.buildNotebookProvenance(ctx, job.NotebookID)
			if err != nil {
				return nil, nil, err
			}
			provenance.Notebooks = append(provenance.Notebooks, *entry)
			if gitCommitHash == nil && commitSHA != nil {
				gitCommitHash = commitSHA
			}
		case domain.PipelineJobTypeModelRun:
			entry, err := s.buildModelProvenance(ctx, job.ModelSelector)
			if err != nil {
				return nil, nil, err
			}
			provenance.Models = append(provenance.Models, *entry)
		}
	}

	return provenance, gitCommitHash, nil
}

func (s *Service) buildNotebookProvenance(ctx context.Context, notebookID string) (*domain.PipelineNotebookProvenance, *string, error) {
	entry := &domain.PipelineNotebookProvenance{NotebookID: notebookID}
	if s.notebookRepo == nil {
		return entry, nil, nil
	}
	nb, err := s.notebookRepo.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, nil, fmt.Errorf("load notebook provenance for %s: %w", notebookID, err)
	}
	entry.LastUpdatedAt = &nb.UpdatedAt
	entry.GitRepoID = nb.GitRepoID
	if nb.GitRepoID == nil || s.gitRepos == nil {
		return entry, nil, nil
	}
	repo, repoErr := s.gitRepos.GetByID(ctx, strings.TrimSpace(*nb.GitRepoID))
	if repoErr == nil {
		entry.GitCommitSHA = repo.LastCommit
		return entry, repo.LastCommit, nil
	}
	s.logger.Warn("load notebook git provenance failed", "notebook_id", notebookID, "git_repo_id", strings.TrimSpace(*nb.GitRepoID), "error", repoErr)
	return entry, nil, nil
}

func (s *Service) buildModelProvenance(ctx context.Context, selector string) (*domain.PipelineModelProvenance, error) {
	entry := &domain.PipelineModelProvenance{Selector: selector}
	if s.models == nil {
		return entry, nil
	}
	parts := strings.Split(strings.TrimSpace(selector), ".")
	if len(parts) != 2 {
		return entry, nil
	}
	model, modelErr := s.models.GetByName(ctx, parts[0], parts[1])
	if modelErr == nil {
		entry.ModelID = &model.ID
		entry.LastUpdatedAt = &model.UpdatedAt
		return entry, nil
	}
	s.logger.Warn("load model provenance failed", "selector", selector, "error", modelErr)
	return entry, nil
}

const timeLayoutRFC3339 = "2006-01-02T15:04:05Z07:00"
