package ui

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func dependencyAdjacencyView(assetKey string, edges []assetDependencyEdgeData) Node {
	if len(edges) == 0 {
		return Div(Class("mb-2"), P(Class("color-fg-muted"), Text("No dependency edges recorded.")))
	}

	graphData := buildAssetGraphData(assetKey, edges)
	nodesJSON, edgesJSON := assetGraphJSON(graphData)
	lines := make([]string, 0, len(edges)+1)
	lines = append(lines, "graph LR")
	edgeItems := make([]Node, 0, len(edges))
	for i := range edges {
		edge := edges[i]
		fromID := "n" + strconv.Itoa(i*2)
		toID := "n" + strconv.Itoa(i*2+1)
		lines = append(lines, "    "+fromID+"[\""+escapeMermaidLabel(edge.FromKey)+"\"] --> "+toID+"[\""+escapeMermaidLabel(edge.ToKey)+"\"]")
		edgeItems = append(edgeItems, Li(Text(edge.FromKey+" -> "+edge.ToKey)))
	}

	return Div(Class("mb-3"),
		P(Class("color-fg-muted"), Text("Interactive dependency map for "+assetKey+":")),
		El("asset-graph-view",
			Class("asset-graph-host"),
			Attr("nodes", nodesJSON),
			Attr("edges", edgesJSON),
		),
		Details(
			Class("mb-2"),
			Summary(Text("Adjacency list")),
			Ul(Class("mb-2"), Group(edgeItems)),
		),
		Details(
			Class("mb-2"),
			Summary(Text("Mermaid view")),
			Pre(Class("Box p-2"), Code(Text(strings.Join(lines, "\n")))),
		),
	)
}

func buildAssetGraphData(assetKey string, edges []assetDependencyEdgeData) assetGraphPayload {
	graphEdges := make([]assetGraphEdgeData, 0, len(edges))
	roles := map[string]string{assetKey: "current"}
	upstream := make([]string, 0, len(edges))
	downstream := make([]string, 0, len(edges))
	for i := range edges {
		edge := edges[i]
		graphEdges = append(graphEdges, assetGraphEdgeData{ID: fmt.Sprintf("e%d", i), Source: edge.FromKey, Target: edge.ToKey})
		if edge.ToKey == assetKey {
			roles[edge.FromKey] = "upstream"
			upstream = append(upstream, edge.FromKey)
		} else if edge.FromKey == assetKey {
			roles[edge.ToKey] = "downstream"
			downstream = append(downstream, edge.ToKey)
		} else {
			if _, ok := roles[edge.FromKey]; !ok {
				roles[edge.FromKey] = "related"
			}
			if _, ok := roles[edge.ToKey]; !ok {
				roles[edge.ToKey] = "related"
			}
		}
	}

	ids := make([]string, 0, len(roles))
	for id := range roles {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]assetGraphNodeData, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, assetGraphNodeData{ID: id, Label: id, Role: roles[id]})
	}
	positionAssetGraphNodes(nodes, assetKey, dedupeStrings(upstream), dedupeStrings(downstream))
	return assetGraphPayload{Nodes: nodes, Edges: graphEdges}
}

func positionAssetGraphNodes(nodes []assetGraphNodeData, assetKey string, upstream []string, downstream []string) {
	positions := map[string]map[string]int{assetKey: {"x": 320, "y": 120}}
	for i := range upstream {
		positions[upstream[i]] = map[string]int{"x": 36, "y": 36 + i*96}
	}
	for i := range downstream {
		positions[downstream[i]] = map[string]int{"x": 604, "y": 36 + i*96}
	}
	relatedIndex := 0
	for i := range nodes {
		if pos, ok := positions[nodes[i].ID]; ok {
			nodes[i].Position = pos
			continue
		}
		nodes[i].Position = map[string]int{"x": 320, "y": 240 + relatedIndex*96}
		relatedIndex++
	}
}

func assetGraphJSON(payload assetGraphPayload) (string, string) {
	nodesJSON, err := json.Marshal(payload.Nodes)
	if err != nil {
		return "[]", "[]"
	}
	edgesJSON, err := json.Marshal(payload.Edges)
	if err != nil {
		return string(nodesJSON), "[]"
	}
	return string(nodesJSON), string(edgesJSON)
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for i := range values {
		if _, ok := seen[values[i]]; ok {
			continue
		}
		seen[values[i]] = struct{}{}
		out = append(out, values[i])
	}
	return out
}

func escapeMermaidLabel(value string) string {
	return strings.ReplaceAll(value, "\"", "\\\"")
}
