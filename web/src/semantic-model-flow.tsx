import { LitElement, html, type PropertyValues } from "lit";
import * as React from "react";
import { createRoot, type Root } from "react-dom/client";
import {
  Background,
  BackgroundVariant,
  BaseEdge,
  Controls,
  EdgeLabelRenderer,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlowProvider,
  ReactFlow,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
  useEdgesState,
  useNodesState,
  useReactFlow,
  getBezierPath,
} from "@xyflow/react";
import dagre from "dagre";
import reactFlowStyles from "@xyflow/react/dist/style.css";

type SemanticFlowField = {
  id: string;
  label: string;
  meta?: string;
  kind?: string;
  sortable?: boolean;
};

type SemanticFlowNode = {
  id: string;
  label: string;
  role?: string;
  baseRelationRef?: string;
  defaultTimeDimension?: string;
  fields?: SemanticFlowField[];
  position?: { x?: number; y?: number };
};

type SemanticFlowEdge = {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  name: string;
  cardinality: string;
  typeLabel: string;
  joinLabel: string;
  sourceField?: string;
  targetField?: string;
  sourceFieldId?: string;
  targetFieldId?: string;
  isDefault?: boolean;
};

type SemanticNodeData = {
  label: string;
  role: string;
  baseRelationRef?: string;
  defaultTimeDimension?: string;
  fields: SemanticFlowField[];
};

type SemanticEdgeData = {
  name: string;
  cardinality: string;
  typeLabel: string;
  joinLabel: string;
  sourceField?: string;
  targetField?: string;
  isDefault?: boolean;
};

type SemanticModelNodeType = Node<SemanticNodeData, "semanticModel">;
type SemanticRelationshipEdgeType = Edge<SemanticEdgeData, "semanticRelationship">;

const semanticFlowStyles = `
:host {
  display: block;
  color: var(--fgColor-default);
}

.semantic-flow-shell {
  display: block;
}

.semantic-flow__viewport {
  min-height: 36rem;
  overflow: hidden;
  border: 1px solid var(--borderColor-default);
  border-radius: 0.875rem;
  background: var(--bgColor-default);
}

.semantic-flow__empty {
  display: grid;
  place-items: center;
  min-height: 18rem;
  padding: 2rem;
  text-align: center;
  color: var(--fgColor-muted);
}

.semantic-flow__canvas {
  width: 100%;
  height: 36rem;
}

.semantic-flow__node {
  width: 17rem;
  border: 1px solid #c8ccd0;
  border-radius: 0.35rem;
  background: #ffffff;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
  overflow: hidden;
}

.semantic-flow__node.is-current {
  border-color: #7aa7ff;
  box-shadow: 0 0 0 1px rgba(58, 115, 255, 0.16), 0 1px 3px rgba(16, 24, 40, 0.12);
}

.semantic-flow__node.react-flow__node-selected,
.semantic-flow__node:has(.react-flow__handle:focus-visible) {
  border-color: #3a73ff;
  box-shadow: 0 0 0 1px rgba(58, 115, 255, 0.28), 0 2px 6px rgba(16, 24, 40, 0.14);
}

.semantic-flow__node-header {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 0.75rem 0.85rem;
  border-bottom: 1px solid #dde1e6;
  background: #f4f5f7;
}

.semantic-flow__table-icon {
  flex: 0 0 auto;
  width: 1rem;
  height: 1rem;
  color: #5f6b7a;
}

.semantic-flow__title {
  margin: 0;
  font-size: 0.9rem;
  font-weight: 600;
  line-height: 1.2;
  word-break: break-word;
}

.semantic-flow__fields {
  display: grid;
}

.semantic-flow__field {
  position: relative;
  display: flex;
  align-items: center;
  min-height: 2.15rem;
  padding: 0 0.85rem;
  border-bottom: 1px solid #edf0f2;
  background: #ffffff;
}

.semantic-flow__field:last-child {
  border-bottom: 0;
}

.semantic-flow__field[data-kind="empty"] {
  color: #6a7481;
}

.semantic-flow__field-label {
  margin: 0;
  padding: 0 0.8rem;
  font-size: 0.8rem;
  font-weight: 500;
  line-height: 1.25;
  color: #1f2937;
  word-break: break-word;
  flex: 1 1 auto;
}

.semantic-flow__field-icon {
  flex: 0 0 auto;
  width: 0.72rem;
  height: 0.72rem;
  color: #8691a1;
}

.semantic-flow__handle {
  width: 0.58rem;
  height: 0.58rem;
  border: 1px solid #ffffff;
  background: #7d8896;
  box-shadow: none;
}

.semantic-flow__handle[data-handle-side="target"] {
  left: 0;
}

.semantic-flow__handle[data-handle-side="source"] {
  right: 0;
}

.semantic-flow__edge-label {
  position: absolute;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 2rem;
  padding: 0.12rem 0.4rem;
  border: 1px solid #d8dde3;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06);
  pointer-events: none;
}

.semantic-flow__edge-type {
  margin: 0;
  font-size: 0.66rem;
  font-weight: 600;
  letter-spacing: 0.01em;
  color: #4b5563;
}

.react-flow {
  background: transparent;
}

.react-flow__pane {
  cursor: grab;
}

.react-flow__pane.dragging {
  cursor: grabbing;
}

.react-flow__controls {
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
}

.react-flow__controls-button {
  background: #ffffff;
  color: #374151;
}

.react-flow__minimap {
  background: rgba(255, 255, 255, 0.96);
  border: 1px solid #d8dde3;
  border-radius: 0.5rem;
  overflow: hidden;
}

.react-flow__attribution {
  display: none;
}

@media (max-width: 900px) {
  .semantic-flow__viewport,
  .semantic-flow__canvas {
    min-height: 32rem;
    height: 32rem;
  }
}
`;

function parseJSON<T>(value: string, fallback: T): T {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function normalizeFieldKind(kind?: string): string {
  const value = (kind ?? "").trim().toLowerCase();
  if (value === "join" || value === "time" || value === "empty") {
    return value;
  }
  return "join";
}

function toReactFlowNodes(nodes: SemanticFlowNode[]): SemanticModelNodeType[] {
  return nodes.map((node) => ({
    id: node.id,
    type: "semanticModel",
    position: {
      x: node.position?.x ?? 0,
      y: node.position?.y ?? 0,
    },
    draggable: true,
    data: {
      label: node.label,
      role: node.role ?? "connected",
      baseRelationRef: node.baseRelationRef,
      defaultTimeDimension: node.defaultTimeDimension,
      fields: (node.fields ?? []).map((field) => ({
        id: field.id,
        label: field.label,
        meta: field.meta,
        kind: normalizeFieldKind(field.kind),
        sortable: field.sortable ?? true,
      })),
    },
  }));
}

const semanticNodeWidth = 272;
const semanticNodeHeaderHeight = 49;
const semanticFieldRowHeight = 34;
const semanticNodePaddingHeight = 8;

function semanticNodeHeight(node: SemanticModelNodeType): number {
  const fieldCount = Math.max(node.data.fields.length, 1);
  return semanticNodeHeaderHeight + fieldCount * semanticFieldRowHeight + semanticNodePaddingHeight;
}

function layoutSemanticFlow(
  nodes: SemanticModelNodeType[],
  edges: SemanticRelationshipEdgeType[],
): SemanticModelNodeType[] {
  if (nodes.length === 0) {
    return nodes;
  }

  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({
    rankdir: "LR",
    align: "UL",
    nodesep: 48,
    ranksep: 120,
    marginx: 32,
    marginy: 32,
  });

  for (const node of nodes) {
    graph.setNode(node.id, {
      width: semanticNodeWidth,
      height: semanticNodeHeight(node),
    });
  }

  for (const edge of edges) {
    graph.setEdge(edge.source, edge.target);
  }

  dagre.layout(graph);

  return nodes.map((node) => {
    const layout = graph.node(node.id);
    if (!layout) {
      return node;
    }

    return {
      ...node,
      position: {
        x: layout.x - semanticNodeWidth / 2,
        y: layout.y - semanticNodeHeight(node) / 2,
      },
    };
  });
}

function toReactFlowEdges(edges: SemanticFlowEdge[]): SemanticRelationshipEdgeType[] {
  return edges.map((edge) => ({
    id: edge.id,
    type: "semanticRelationship",
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.sourceHandle,
    targetHandle: edge.targetHandle,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 16,
      height: 16,
    },
    style: {
      strokeWidth: 2.4,
      stroke: "color-mix(in srgb, var(--borderColor-accent-emphasis) 82%, transparent)",
    },
    data: {
      name: edge.name,
      cardinality: edge.cardinality,
      typeLabel: edge.typeLabel,
      joinLabel: edge.joinLabel,
      sourceField: edge.sourceField,
      targetField: edge.targetField,
      isDefault: edge.isDefault,
    },
  }));
}

function TableIcon() {
  return (
    <svg aria-hidden="true" className="semantic-flow__table-icon" viewBox="0 0 16 16" fill="none">
      <rect x="2" y="2.5" width="12" height="11" rx="1.5" stroke="currentColor" strokeWidth="1.2" />
      <path d="M2.5 6h11M6 2.5v11" stroke="currentColor" strokeWidth="1.2" />
    </svg>
  );
}

function FieldIcon() {
  return (
    <svg aria-hidden="true" className="semantic-flow__field-icon" viewBox="0 0 12 12" fill="none">
      <circle cx="6" cy="6" r="2.5" fill="currentColor" />
    </svg>
  );
}

function SemanticModelNode({ data, selected }: NodeProps<SemanticModelNodeType>) {
  const roleClass = `is-${data.role}`;
  return (
    <div className={`semantic-flow__node ${roleClass} ${selected ? "react-flow__node-selected" : ""}`}>
      <div className="semantic-flow__node-header">
        <TableIcon />
        <p className="semantic-flow__title">{data.label}</p>
      </div>
      <div className="semantic-flow__fields">
        {data.fields.map((field) => (
          <div className="semantic-flow__field" data-kind={field.kind} key={field.id}>
            {field.kind !== "empty" ? (
              <>
                <Handle
                  className="semantic-flow__handle"
                  data-handle-side="target"
                  id={`target:${field.id}`}
                  isConnectable={false}
                  position={Position.Left}
                  type="target"
                  style={{ top: "50%", transform: "translate(-50%, -50%)" }}
                />
                <Handle
                  className="semantic-flow__handle"
                  data-handle-side="source"
                  id={`source:${field.id}`}
                  isConnectable={false}
                  position={Position.Right}
                  type="source"
                  style={{ top: "50%", transform: "translate(50%, -50%)" }}
                />
              </>
            ) : null}
            <FieldIcon />
            <p className="semantic-flow__field-label">{field.label}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function SemanticRelationshipEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  style,
  data,
}: EdgeProps<SemanticRelationshipEdgeType>) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  return (
    <>
      <BaseEdge id={id} markerEnd={markerEnd} path={edgePath} style={style} />
      <EdgeLabelRenderer>
        <div
          className="semantic-flow__edge-label"
          style={{
            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
          }}
        >
          <p className="semantic-flow__edge-type">{data?.cardinality || "Join"}</p>
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

const nodeTypes = {
  semanticModel: SemanticModelNode,
};

const edgeTypes = {
  semanticRelationship: SemanticRelationshipEdge,
};

function SemanticFlowCanvas({ nodes, edges }: { nodes: SemanticModelNodeType[]; edges: SemanticRelationshipEdgeType[] }) {
  const reactFlow = useReactFlow();
  const [flowNodes, setFlowNodes, onNodesChange] = useNodesState(nodes);
  const [flowEdges, setFlowEdges, onEdgesChange] = useEdgesState(edges);

  // Keep drag state local, but refresh if the server payload changes.
  React.useEffect(() => {
    const laidOutNodes = layoutSemanticFlow(nodes, edges);
    setFlowNodes(laidOutNodes);
    setFlowEdges(edges);
    requestAnimationFrame(() => {
      reactFlow.fitView({ padding: 0.18, duration: 280, minZoom: 0.55, maxZoom: 1.65 });
    });
  }, [nodes, edges, reactFlow, setFlowEdges, setFlowNodes]);

  if (flowNodes.length === 0) {
    return <div className="semantic-flow__empty">No semantic models are connected yet.</div>;
  }

  return (
    <div className="semantic-flow__viewport">
      <div className="semantic-flow__canvas">
        <ReactFlow
          fitView
          fitViewOptions={{ padding: 0.2, duration: 280 }}
          nodes={flowNodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodesConnectable={false}
          nodesDraggable
          elementsSelectable
          selectNodesOnDrag={false}
          minZoom={0.55}
          maxZoom={1.65}
          defaultEdgeOptions={{
            type: "semanticRelationship",
            markerEnd: {
              type: MarkerType.ArrowClosed,
              width: 16,
              height: 16,
            },
          }}
        >
          <Background color="#e7eaee" gap={20} size={1} variant={BackgroundVariant.Lines} />
          <MiniMap pannable zoomable />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  );
}

class SemanticModelFlow extends LitElement {
  static properties = {
    nodesJSON: { attribute: "nodes" },
    edgesJSON: { attribute: "edges" },
  };

  declare nodesJSON: string;
  declare edgesJSON: string;

  private reactRoot: Root | null = null;

  constructor() {
    super();
    this.nodesJSON = "[]";
    this.edgesJSON = "[]";
  }

  render() {
    return html`
      <div class="semantic-flow-shell">
        <style>
          ${reactFlowStyles}
          ${semanticFlowStyles}
        </style>
        <div id="semantic-flow-react-root"></div>
      </div>
    `;
  }

  firstUpdated() {
    this.renderReactFlow();
  }

  updated(changedProperties: PropertyValues<this>) {
    if (changedProperties.has("nodesJSON") || changedProperties.has("edgesJSON")) {
      this.renderReactFlow();
    }
  }

  disconnectedCallback() {
    this.reactRoot?.unmount();
    this.reactRoot = null;
    super.disconnectedCallback();
  }

  private renderReactFlow() {
    const mountPoint = this.renderRoot.querySelector("#semantic-flow-react-root");
    if (!(mountPoint instanceof HTMLElement)) {
      return;
    }

    if (this.reactRoot === null) {
      this.reactRoot = createRoot(mountPoint);
    }

    const nodes = toReactFlowNodes(parseJSON<SemanticFlowNode[]>(this.nodesJSON, []));
    const edges = toReactFlowEdges(parseJSON<SemanticFlowEdge[]>(this.edgesJSON, []));

    this.reactRoot.render(
      <ReactFlowProvider>
        <SemanticFlowCanvas edges={edges} nodes={nodes} />
      </ReactFlowProvider>,
    );
  }
}

customElements.define("semantic-model-flow", SemanticModelFlow);
