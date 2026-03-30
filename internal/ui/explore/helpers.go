package explore

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	"github.com/starfederation/datastar-go/datastar"
	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

const defaultPageSize = 30

func pageFromRequest(r *http.Request, defaultPageSize int) domain.PageRequest {
	maxResults := defaultPageSize
	if maxResults <= 0 {
		maxResults = 25
	}
	if raw := r.URL.Query().Get("max_results"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			maxResults = parsed
		}
	}
	if maxResults < 1 {
		maxResults = 1
	}
	if maxResults > 200 {
		maxResults = 200
	}
	return domain.PageRequest{MaxResults: maxResults, PageToken: r.URL.Query().Get("page_token")}
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
}

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	value := formString(values, key)
	if value == "" {
		return nil
	}
	return &value
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func formBool(values map[string][]string, key string) bool {
	if values == nil {
		return false
	}
	value := strings.TrimSpace(strings.ToLower(first(values[key])))
	return value == "true" || value == "1" || value == "on" || value == "yes"
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(ts)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(*ts)
}

func strOrDash(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func valueOrDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func principalName(r *http.Request) string {
	principal := core.PrincipalFromContext(r.Context())
	if principal.Name == "" {
		return "unknown"
	}
	return principal.Name
}

func normalizeKinds(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		switch strings.TrimSpace(value) {
		case "", domain.ExploreKindAll:
			return nil
		case domain.ExploreKindFolder, domain.ExploreKindNotebook, domain.ExploreKindModel, domain.ExploreKindMacro,
			domain.ExploreKindDashboard, domain.ExploreKindPipeline, domain.ExploreKindSemanticModel:
			value = strings.TrimSpace(value)
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			normalized = append(normalized, value)
		}
	}
	return normalized
}

func normalizeOwners(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized
}

func selectedKindsFromRequest(r *http.Request) []string {
	return normalizeKinds(r.URL.Query()["kind"])
}

func selectedOwnersFromRequest(r *http.Request) []string {
	return normalizeOwners(r.URL.Query()["owner"])
}

func selectedQueryFromRequest(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("q"))
}

func normalizeValues(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	return normalized
}

type updateParams struct {
	FolderID string
	Kinds    []string
	Owners   []string
	Query    string
}

func decodeUpdateParams(r *http.Request) (updateParams, error) {
	type urlParamsPayload struct {
		FolderID string   `json:"folder_id"`
		Kinds    []string `json:"kind"`
		Owners   []string `json:"owner"`
		Query    string   `json:"q"`
	}
	type wrapper struct {
		URLParams urlParamsPayload `json:"urlParams"`
	}
	var signals wrapper
	if err := datastar.ReadSignals(r, &signals); err != nil {
		return updateParams{}, fmt.Errorf("read explore update signals: %w", err)
	}
	return updateParams{
		FolderID: strings.TrimSpace(signals.URLParams.FolderID),
		Kinds:    normalizeValues(signals.URLParams.Kinds),
		Owners:   normalizeValues(signals.URLParams.Owners),
		Query:    strings.TrimSpace(signals.URLParams.Query),
	}, nil
}

func textMatch(query string, values ...string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return true
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(strings.TrimSpace(value)), query) {
			return true
		}
	}
	return false
}

func folderKindAllowed(selectedKinds []string) bool {
	if len(selectedKinds) == 0 {
		return true
	}
	return containsString(selectedKinds, domain.ExploreKindFolder)
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func filterOwners(folders []domain.Folder, items []domain.ExploreItem) []string {
	seen := map[string]struct{}{}
	owners := make([]string, 0, len(folders)+len(items))
	for _, folder := range folders {
		owner := strings.TrimSpace(folder.Owner)
		if owner == "" {
			continue
		}
		if _, ok := seen[owner]; ok {
			continue
		}
		seen[owner] = struct{}{}
		owners = append(owners, owner)
	}
	for _, item := range items {
		owner := strings.TrimSpace(item.Owner)
		if owner == "" {
			continue
		}
		if _, ok := seen[owner]; ok {
			continue
		}
		seen[owner] = struct{}{}
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	return owners
}

func folderShareRows(folderID string, shares []domain.FolderShare) []accessShareRow {
	rows := make([]accessShareRow, 0, len(shares))
	for i := range shares {
		share := shares[i]
		rows = append(rows, accessShareRow{
			Principal: share.PrincipalName,
			Role:      share.Role,
			DeleteURL: "/ui/explore/folders/" + folderID + "/shares/" + url.PathEscape(share.PrincipalName) + "/delete",
		})
	}
	return rows
}

func folderDisplayPathMap(folders []domain.Folder) map[string]string {
	byID := make(map[string]domain.Folder, len(folders))
	for i := range folders {
		byID[folders[i].ID] = folders[i]
	}
	paths := make(map[string]string, len(folders))
	var build func(string) string
	build = func(id string) string {
		if path, ok := paths[id]; ok {
			return path
		}
		folder, ok := byID[id]
		if !ok {
			return ""
		}
		label := strings.TrimSpace(folder.Name)
		if label == "" {
			label = id
		}
		parentID := stringValue(folder.ParentFolderID)
		if parentID == "" {
			paths[id] = label
			return label
		}
		parent, ok := byID[parentID]
		if !ok {
			paths[id] = label
			return label
		}
		parentPath := build(parentID)
		if parent.SystemRole != nil && *parent.SystemRole == domain.FolderSystemRolePersonalRoot {
			paths[id] = label
			return label
		}
		if parentPath == "" {
			paths[id] = label
			return label
		}
		paths[id] = parentPath + " / " + label
		return paths[id]
	}
	for id := range byID {
		build(id)
	}
	return paths
}

func itemURL(item domain.ExploreItem) string {
	switch item.Kind {
	case domain.ExploreKindNotebook:
		return "/ui/notebooks/" + item.ID
	case domain.ExploreKindDashboard:
		return "/ui/dashboards/" + item.ID
	case domain.ExploreKindPipeline:
		return "/ui/pipelines/" + item.Name
	case domain.ExploreKindModel:
		if item.ProjectName != nil && strings.TrimSpace(*item.ProjectName) != "" {
			return "/ui/models/" + *item.ProjectName + "/" + item.Name
		}
	case domain.ExploreKindMacro:
		return "/ui/macros/" + item.Name
	case domain.ExploreKindSemanticModel:
		if item.ProjectName != nil && strings.TrimSpace(*item.ProjectName) != "" {
			return "/ui/semantic/models/" + *item.ProjectName + "/" + item.Name
		}
	}
	return "#"
}

func pageURL(page domain.PageRequest, kinds []string, owners []string, searchQuery string, folderID string) string {
	q := url.Values{}
	if page.Limit() != defaultPageSize {
		q.Set("max_results", fmt.Sprintf("%d", page.Limit()))
	}
	if strings.TrimSpace(page.PageToken) != "" {
		q.Set("page_token", page.PageToken)
	}
	for _, kind := range normalizeKinds(kinds) {
		q.Add("kind", kind)
	}
	for _, owner := range normalizeOwners(owners) {
		q.Add("owner", owner)
	}
	if strings.TrimSpace(searchQuery) != "" {
		q.Set("q", strings.TrimSpace(searchQuery))
	}
	if strings.TrimSpace(folderID) != "" {
		q.Set("folder_id", folderID)
	}
	if len(q) == 0 {
		return "/ui/explore"
	}
	return "/ui/explore?" + q.Encode()
}

func folderSelectNodes(options []folderSelectOption) []Node {
	nodes := make([]Node, 0, len(options))
	for i := range options {
		option := options[i]
		label := option.Label
		if option.Description != "" {
			label += " - " + option.Description
		}
		if option.Selected {
			nodes = append(nodes, Option(Value(option.ID), Selected(), Text(label)))
			continue
		}
		nodes = append(nodes, Option(Value(option.ID), Text(label)))
	}
	return nodes
}

func gitRepoSelectNodes(options []gitRepoSelectOption) []Node {
	nodes := make([]Node, 0, len(options))
	for i := range options {
		option := options[i]
		if option.Selected {
			nodes = append(nodes, Option(Value(option.ID), Selected(), Text(option.Label)))
			continue
		}
		nodes = append(nodes, Option(Value(option.ID), Text(option.Label)))
	}
	return nodes
}

func selectGitRepo(options []gitRepoSelectOption, selectedID string) []gitRepoSelectOption {
	items := make([]gitRepoSelectOption, 0, len(options))
	for i := range options {
		option := options[i]
		option.Selected = option.ID == selectedID
		items = append(items, option)
	}
	return items
}

func filterFolderOptions(options []folderSelectOption, excludeIDs ...string) []folderSelectOption {
	exclude := make(map[string]struct{}, len(excludeIDs))
	for _, id := range excludeIDs {
		if strings.TrimSpace(id) != "" {
			exclude[id] = struct{}{}
		}
	}
	filtered := make([]folderSelectOption, 0, len(options))
	for _, option := range options {
		if _, skip := exclude[option.ID]; skip {
			continue
		}
		filtered = append(filtered, option)
	}
	return filtered
}

func shareRoleSelectNodes(selected string) []Node {
	roles := []struct {
		Value string
		Label string
	}{
		{Value: domain.FolderShareRoleViewer, Label: "Viewer"},
		{Value: domain.FolderShareRoleEditor, Label: "Editor"},
		{Value: domain.FolderShareRoleManager, Label: "Manager"},
	}
	nodes := make([]Node, 0, len(roles)+1)
	nodes = append(nodes, Name("role"))
	for _, role := range roles {
		option := Option(Value(role.Value), Text(role.Label))
		if selected == role.Value {
			option = Option(Value(role.Value), Selected(), Text(role.Label))
		}
		nodes = append(nodes, option)
	}
	return nodes
}

func csvString(rows [][]string) string {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	_ = writer.WriteAll(rows)
	writer.Flush()
	return buf.String()
}
