package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (s *Service) recordRunEvent(ctx context.Context, runID string, jobRunID *string, eventType string, message *string, errorCode *string, metadata map[string]any) {
	if s.runs == nil {
		return
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := s.runs.CreateRunEvent(ctx, &domain.PipelineRunEvent{
		ID:        domain.NewID(),
		RunID:     runID,
		JobRunID:  jobRunID,
		EventType: eventType,
		Message:   message,
		ErrorCode: errorCode,
		Metadata:  metadata,
	})
	if err != nil {
		s.logger.Warn("record pipeline run event failed", "run_id", runID, "event_type", eventType, "error", err)
	}
}

func (s *Service) notifyRunEvent(_ context.Context, pipeline *domain.Pipeline, run *domain.PipelineRun, eventType string) {
	if pipeline == nil || run == nil || len(pipeline.NotificationWebhooks) == 0 {
		return
	}
	payload := map[string]any{
		"event_type":          eventType,
		"run_id":              run.ID,
		"pipeline_id":         pipeline.ID,
		"pipeline_name":       pipeline.Name,
		"status":              run.Status,
		"trigger_type":        run.TriggerType,
		"triggered_by":        run.TriggeredBy,
		"effective_principal": run.EffectivePrincipal,
		"error_summary":       valueOrEmpty(run.ErrorMessage),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		s.logger.Warn("marshal pipeline webhook payload failed", "run_id", run.ID, "event_type", eventType, "error", err)
		return
	}
	client := s.httpClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	for _, hook := range pipeline.NotificationWebhooks {
		hook := hook
		if !pipelineWebhookWantsEvent(hook, eventType) {
			continue
		}
		go func() {
			for attempt := 1; attempt <= 3; attempt++ {
				reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				req, reqErr := http.NewRequestWithContext(reqCtx, http.MethodPost, hook.URL, bytes.NewReader(body))
				if reqErr != nil {
					cancel()
					s.logger.Warn("build pipeline webhook request failed", "url", hook.URL, "error", reqErr)
					return
				}
				req.Header.Set("Content-Type", "application/json")
				resp, doErr := client.Do(req)
				cancel()
				if doErr == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
					if resp.Body != nil {
						_ = resp.Body.Close()
					}
					return
				}
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				if attempt == 3 {
					s.logger.Warn("pipeline webhook delivery failed", "url", hook.URL, "event_type", eventType, "run_id", run.ID, "error", doErr)
					return
				}
				time.Sleep(time.Duration(1<<(attempt-1)) * time.Second)
			}
		}()
	}
}

func pipelineWebhookWantsEvent(hook domain.PipelineNotificationWebhook, eventType string) bool {
	if len(hook.Events) == 0 {
		return true
	}
	for _, candidate := range hook.Events {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(eventType)) {
			return true
		}
	}
	return false
}

func valueOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func classifyPipelineErrorCode(err error) *string {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return pipelineMessagePtr("CANCELLED")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return pipelineMessagePtr("TIMEOUT")
	}
	return pipelineMessagePtr("EXECUTION_ERROR")
}

func (s *Service) emitRunNotificationIfNeeded(ctx context.Context, pipelineID string, runID string, eventType string) {
	if s.pipelines == nil || s.runs == nil {
		return
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, pipelineID)
	if err != nil {
		s.logger.Warn("load pipeline for notification failed", "pipeline_id", pipelineID, "run_id", runID, "error", err)
		return
	}
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		s.logger.Warn("load run for notification failed", "run_id", runID, "error", err)
		return
	}
	s.notifyRunEvent(ctx, pipelineDef, run, eventType)
}

func (s *Service) logRunEventAndNotify(ctx context.Context, run *domain.PipelineRun, eventType string, message *string, errorCode *string, metadata map[string]any) {
	if run == nil {
		return
	}
	s.recordRunEvent(ctx, run.ID, nil, eventType, message, errorCode, metadata)
	switch eventType {
	case domain.PipelineRunEventSucceeded, domain.PipelineRunEventFailed, domain.PipelineRunEventCancelled, domain.PipelineRunEventSLABreach, domain.PipelineRunEventRepaired:
		s.emitRunNotificationIfNeeded(ctx, run.PipelineID, run.ID, eventType)
		if eventType == domain.PipelineRunEventFailed && run.RepairedFromRunID != nil {
			s.emitRunNotificationIfNeeded(ctx, run.PipelineID, run.ID, domain.PipelineRunEventRepairFailed)
		}
	}
}

func (s *Service) logJobRunEvent(ctx context.Context, runID string, jobRunID string, eventType string, message *string, errorCode *string, metadata map[string]any) {
	s.recordRunEvent(ctx, runID, pipelineMessagePtr(jobRunID), eventType, message, errorCode, metadata)
}

func (s *Service) runEventsPage(ctx context.Context, runID string, page domain.PageRequest) ([]domain.PipelineRunEvent, int64, error) {
	if err := s.requireRunsRepo(); err != nil {
		return nil, 0, err
	}
	return s.runs.ListRunEvents(ctx, runID, page)
}
