package semantic

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"duck-demo/internal/domain"
)

const (
	semanticFlowColumnXLeft   = 40
	semanticFlowColumnXCenter = 420
	semanticFlowColumnXRight  = 800
	semanticFlowColumnYTop    = 48
	semanticFlowNodeGapY      = 220
)

var (
	simpleJoinPattern  = regexp.MustCompile(`^\s*([A-Za-z_][\w\.]*)\s*=\s*([A-Za-z_][\w\.]*)\s*$`)
	semanticFieldIDSan = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
)

type semanticFlowFieldData struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Meta     string `json:"meta,omitempty"`
	Kind     string `json:"kind"`
	Sortable bool   `json:"sortable"`
}

type semanticFlowNodeData struct {
	ID                   string                  `json:"id"`
	Label                string                  `json:"label"`
	Role                 string                  `json:"role"`
	BaseModelRef         string                  `json:"baseModelRef,omitempty"`
	DefaultTimeDimension string                  `json:"defaultTimeDimension,omitempty"`
	Fields               []semanticFlowFieldData `json:"fields"`
	Position             map[string]int          `json:"position"`
}

type semanticFlowEdgeData struct {
	ID            string `json:"id"`
	Source        string `json:"source"`
	Target        string `json:"target"`
	SourceHandle  string `json:"sourceHandle"`
	TargetHandle  string `json:"targetHandle"`
	Name          string `json:"name"`
	Cardinality   string `json:"cardinality"`
	TypeLabel     string `json:"typeLabel"`
	JoinLabel     string `json:"joinLabel"`
	SourceField   string `json:"sourceField"`
	TargetField   string `json:"targetField"`
	SourceFieldID string `json:"sourceFieldId"`
	TargetFieldID string `json:"targetFieldId"`
	IsDefault     bool   `json:"isDefault"`
}

type semanticModelFlowData struct {
	Nodes []semanticFlowNodeData `json:"nodes"`
	Edges []semanticFlowEdgeData `json:"edges"`
}

type semanticRelatedRelationshipRowData struct {
	Name           string
	ConnectedModel string
	Type           string
	Cardinality    string
	JoinLabel      string
	JoinSQL        string
	SourceField    string
	TargetField    string
	IsDefault      bool
	EditURL        string
}

type semanticNeighborDirection struct {
	Incoming bool
	Outgoing bool
}

type semanticFieldRegistry struct {
	byModel map[string]map[string]semanticFlowFieldData
	order   map[string][]string
}

func buildSemanticModelFlowData(current domain.SemanticModel, models []domain.SemanticModel, relationships []domain.SemanticRelationship) (semanticModelFlowData, []semanticRelatedRelationshipRowData) {
	modelByID := map[string]domain.SemanticModel{
		current.ID: current,
	}
	modelNames := map[string]string{
		current.ID: semanticModelLabel(current),
	}
	for i := range models {
		modelByID[models[i].ID] = models[i]
		modelNames[models[i].ID] = semanticModelLabel(models[i])
	}

	directRelationships := make([]domain.SemanticRelationship, 0)
	neighborDirections := map[string]*semanticNeighborDirection{}
	for i := range relationships {
		relationship := relationships[i]
		switch {
		case relationship.FromSemanticID == current.ID && relationship.ToSemanticID == current.ID:
			directRelationships = append(directRelationships, relationship)
		case relationship.FromSemanticID == current.ID:
			directRelationships = append(directRelationships, relationship)
			state := ensureNeighborDirection(neighborDirections, relationship.ToSemanticID)
			state.Outgoing = true
		case relationship.ToSemanticID == current.ID:
			directRelationships = append(directRelationships, relationship)
			state := ensureNeighborDirection(neighborDirections, relationship.FromSemanticID)
			state.Incoming = true
		}
	}

	sort.Slice(directRelationships, func(i, j int) bool {
		return directRelationships[i].Name < directRelationships[j].Name
	})

	fields := newSemanticFieldRegistry()
	rows := make([]semanticRelatedRelationshipRowData, 0, len(directRelationships))
	edges := make([]semanticFlowEdgeData, 0, len(directRelationships))
	for i := range directRelationships {
		relationship := directRelationships[i]
		sourceExpr, targetExpr, parsed := parseSemanticJoinOperands(relationship.JoinSQL)
		if !parsed {
			sourceExpr = compactWhitespace(relationship.JoinSQL)
			targetExpr = compactWhitespace(relationship.JoinSQL)
		}
		sourceField := fields.Add(relationship.FromSemanticID, sourceExpr, "join")
		targetField := fields.Add(relationship.ToSemanticID, targetExpr, "join")

		connectedModelID := relationship.ToSemanticID
		if relationship.ToSemanticID == current.ID {
			connectedModelID = relationship.FromSemanticID
		}

		rows = append(rows, semanticRelatedRelationshipRowData{
			Name:           relationship.Name,
			ConnectedModel: defaultString(modelNames[connectedModelID], connectedModelID),
			Type:           relationship.RelationshipType,
			Cardinality:    semanticRelationshipCardinality(relationship.RelationshipType),
			JoinLabel:      semanticJoinLabel(relationship.JoinSQL),
			JoinSQL:        compactWhitespace(relationship.JoinSQL),
			SourceField:    sourceField.Label,
			TargetField:    targetField.Label,
			IsDefault:      relationship.IsDefault,
			EditURL:        "/ui/semantic/models/" + current.ProjectName + "/" + current.Name + "/edit",
		})

		if relationship.FromSemanticID == relationship.ToSemanticID {
			continue
		}
		edges = append(edges, semanticFlowEdgeData{
			ID:            relationship.Name,
			Source:        relationship.FromSemanticID,
			Target:        relationship.ToSemanticID,
			SourceHandle:  "source:" + sourceField.ID,
			TargetHandle:  "target:" + targetField.ID,
			Name:          relationship.Name,
			Cardinality:   semanticRelationshipCardinality(relationship.RelationshipType),
			TypeLabel:     relationship.RelationshipType,
			JoinLabel:     semanticJoinLabel(relationship.JoinSQL),
			SourceField:   sourceField.Label,
			TargetField:   targetField.Label,
			SourceFieldID: sourceField.ID,
			TargetFieldID: targetField.ID,
			IsDefault:     relationship.IsDefault,
		})
	}

	nodes := buildSemanticFlowNodes(current, modelByID, modelNames, neighborDirections, fields)
	return semanticModelFlowData{
		Nodes: nodes,
		Edges: edges,
	}, rows
}

func buildSemanticFlowNodes(current domain.SemanticModel, modelByID map[string]domain.SemanticModel, modelNames map[string]string, neighborDirections map[string]*semanticNeighborDirection, registry *semanticFieldRegistry) []semanticFlowNodeData {
	nodes := make([]semanticFlowNodeData, 0, len(neighborDirections)+1)
	nodes = append(nodes, semanticFlowNodeData{
		ID:                   current.ID,
		Label:                semanticModelLabel(current),
		Role:                 "current",
		BaseModelRef:         current.BaseModelRef,
		DefaultTimeDimension: current.DefaultTimeDimension,
		Fields:               semanticNodeFields(current.ID, registry, current.DefaultTimeDimension),
		Position:             map[string]int{"x": semanticFlowColumnXCenter, "y": semanticFlowColumnYTop + semanticFlowNodeGapY/2},
	})

	incomingIDs := make([]string, 0)
	outgoingIDs := make([]string, 0)
	mixedIDs := make([]string, 0)
	for modelID, direction := range neighborDirections {
		switch {
		case direction.Incoming && direction.Outgoing:
			mixedIDs = append(mixedIDs, modelID)
		case direction.Incoming:
			incomingIDs = append(incomingIDs, modelID)
		default:
			outgoingIDs = append(outgoingIDs, modelID)
		}
	}

	sortSemanticFlowIDs(incomingIDs, modelNames)
	sortSemanticFlowIDs(outgoingIDs, modelNames)
	sortSemanticFlowIDs(mixedIDs, modelNames)

	nodes = append(nodes, semanticFlowNodesForIDs(incomingIDs, modelByID, modelNames, registry, "incoming", semanticFlowColumnXLeft, semanticFlowColumnYTop)...)
	nodes = append(nodes, semanticFlowNodesForIDs(outgoingIDs, modelByID, modelNames, registry, "outgoing", semanticFlowColumnXRight, semanticFlowColumnYTop)...)
	nodes = append(nodes, semanticFlowNodesForIDs(mixedIDs, modelByID, modelNames, registry, "mixed", semanticFlowColumnXCenter, semanticFlowColumnYTop+semanticFlowNodeGapY)...)
	return nodes
}

func semanticFlowNodesForIDs(ids []string, modelByID map[string]domain.SemanticModel, modelNames map[string]string, registry *semanticFieldRegistry, role string, xStart, yStart int) []semanticFlowNodeData {
	nodes := make([]semanticFlowNodeData, 0, len(ids))
	for i := range ids {
		model := modelByID[ids[i]]
		nodes = append(nodes, semanticFlowNodeData{
			ID:                   ids[i],
			Label:                defaultString(modelNames[ids[i]], ids[i]),
			Role:                 role,
			BaseModelRef:         model.BaseModelRef,
			DefaultTimeDimension: model.DefaultTimeDimension,
			Fields:               semanticNodeFields(ids[i], registry, model.DefaultTimeDimension),
			Position:             map[string]int{"x": xStart, "y": yStart + i*semanticFlowNodeGapY},
		})
	}
	return nodes
}

func semanticNodeFields(modelID string, registry *semanticFieldRegistry, defaultTimeDimension string) []semanticFlowFieldData {
	fields := registry.Fields(modelID)
	if strings.TrimSpace(defaultTimeDimension) != "" {
		fields = append(fields, semanticFieldFromExpression(defaultTimeDimension, "time"))
	}
	if len(fields) == 0 {
		fields = append(fields, semanticFlowFieldData{
			ID:       "no_fields",
			Label:    "No join columns yet",
			Kind:     "empty",
			Sortable: false,
		})
	}
	return dedupeSemanticFields(fields)
}

func dedupeSemanticFields(fields []semanticFlowFieldData) []semanticFlowFieldData {
	seen := map[string]struct{}{}
	out := make([]semanticFlowFieldData, 0, len(fields))
	for i := range fields {
		if _, ok := seen[fields[i].ID]; ok {
			continue
		}
		seen[fields[i].ID] = struct{}{}
		out = append(out, fields[i])
	}
	return out
}

func sortSemanticFlowIDs(ids []string, modelNames map[string]string) {
	sort.Slice(ids, func(i, j int) bool {
		return defaultString(modelNames[ids[i]], ids[i]) < defaultString(modelNames[ids[j]], ids[j])
	})
}

func semanticFlowJSON(flow semanticModelFlowData) (string, string) {
	nodesJSON, err := json.Marshal(flow.Nodes)
	if err != nil {
		return "[]", "[]"
	}
	edgesJSON, err := json.Marshal(flow.Edges)
	if err != nil {
		return string(nodesJSON), "[]"
	}
	return string(nodesJSON), string(edgesJSON)
}

func semanticJoinLabel(joinSQL string) string {
	normalized := compactWhitespace(joinSQL)
	if normalized == "" {
		return "-"
	}
	sourceExpr, targetExpr, parsed := parseSemanticJoinOperands(normalized)
	if !parsed {
		return normalized
	}
	return semanticFieldLabel(sourceExpr) + " = " + semanticFieldLabel(targetExpr)
}

func parseSemanticJoinOperands(joinSQL string) (string, string, bool) {
	matches := simpleJoinPattern.FindStringSubmatch(compactWhitespace(joinSQL))
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func compactWhitespace(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func semanticRelationshipCardinality(value string) string {
	switch strings.TrimSpace(value) {
	case domain.RelationshipTypeOneToOne:
		return "1:1"
	case domain.RelationshipTypeOneToMany:
		return "1:N"
	case domain.RelationshipTypeManyToOne:
		return "N:1"
	case domain.RelationshipTypeManyMany:
		return "N:N"
	default:
		return valueOrDash(value)
	}
}

func semanticModelLabel(model domain.SemanticModel) string {
	return model.ProjectName + "." + model.Name
}

func ensureNeighborDirection(items map[string]*semanticNeighborDirection, modelID string) *semanticNeighborDirection {
	if direction, ok := items[modelID]; ok {
		return direction
	}
	direction := &semanticNeighborDirection{}
	items[modelID] = direction
	return direction
}

func newSemanticFieldRegistry() *semanticFieldRegistry {
	return &semanticFieldRegistry{
		byModel: map[string]map[string]semanticFlowFieldData{},
		order:   map[string][]string{},
	}
}

func (r *semanticFieldRegistry) Add(modelID, expression, kind string) semanticFlowFieldData {
	expression = compactWhitespace(expression)
	if expression == "" {
		expression = "join_condition"
	}
	if _, ok := r.byModel[modelID]; !ok {
		r.byModel[modelID] = map[string]semanticFlowFieldData{}
	}
	field := semanticFieldFromExpression(expression, kind)
	if _, ok := r.byModel[modelID][field.ID]; !ok {
		r.byModel[modelID][field.ID] = field
		r.order[modelID] = append(r.order[modelID], field.ID)
	}
	return r.byModel[modelID][field.ID]
}

func (r *semanticFieldRegistry) Fields(modelID string) []semanticFlowFieldData {
	ids := r.order[modelID]
	fields := make([]semanticFlowFieldData, 0, len(ids))
	for i := range ids {
		fields = append(fields, r.byModel[modelID][ids[i]])
	}
	return fields
}

func semanticFieldFromExpression(expression, kind string) semanticFlowFieldData {
	return semanticFlowFieldData{
		ID:       semanticFieldID(expression),
		Label:    semanticFieldLabel(expression),
		Meta:     semanticFieldMeta(expression),
		Kind:     kind,
		Sortable: kind != "empty",
	}
}

func semanticFieldID(expression string) string {
	sanitized := semanticFieldIDSan.ReplaceAllString(strings.ToLower(strings.TrimSpace(expression)), "_")
	sanitized = strings.Trim(sanitized, "_")
	if sanitized == "" {
		return "field"
	}
	return sanitized
}

func semanticFieldLabel(expression string) string {
	expression = strings.TrimSpace(expression)
	parts := strings.Split(expression, ".")
	if len(parts) == 0 {
		return expression
	}
	return parts[len(parts)-1]
}

func semanticFieldMeta(expression string) string {
	expression = strings.TrimSpace(expression)
	lastDot := strings.LastIndex(expression, ".")
	if lastDot <= 0 {
		return ""
	}
	return expression[:lastDot]
}
