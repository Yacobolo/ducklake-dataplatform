import { LitElement, css, html } from 'https://esm.sh/lit@3.2.1';
import { getBezierPath, Position } from 'https://esm.sh/@xyflow/system@0.0.75';

class AssetGraphView extends LitElement {
  static properties = {
    nodes: { type: String },
    edges: { type: String },
  };

  static styles = css`
    :host {
      display: block;
    }

    .frame {
      position: relative;
      min-height: 22rem;
      overflow: hidden;
      border: 1px solid var(--borderColor-muted);
      border-radius: calc(var(--radius) * 1.25);
      background:
        radial-gradient(circle at top left, color-mix(in srgb, var(--bgColor-accent-muted) 55%, transparent), transparent 34%),
        linear-gradient(180deg, var(--bgColor-default), var(--bgColor-muted));
    }

    svg {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      pointer-events: none;
    }

    .node-layer {
      position: relative;
      min-height: 22rem;
    }

    .node {
      position: absolute;
      width: 10.5rem;
      padding: 0.75rem 0.85rem;
      border-radius: 16px;
      border: 1px solid var(--borderColor-muted);
      background: color-mix(in srgb, var(--bgColor-default) 88%, transparent);
      box-shadow: var(--shadow-resting-small);
      color: var(--fgColor-default);
      backdrop-filter: blur(10px);
    }

    .node[data-role='current'] {
      border-color: var(--borderColor-accent-emphasis);
      background: color-mix(in srgb, var(--bgColor-accent-muted) 36%, var(--bgColor-default));
      box-shadow: var(--shadow-floating-small);
    }

    .node[data-role='upstream'] {
      border-color: var(--borderColor-success-emphasis);
      background: color-mix(in srgb, var(--bgColor-success-muted) 36%, var(--bgColor-default));
    }

    .node[data-role='downstream'] {
      border-color: var(--borderColor-attention-emphasis);
      background: color-mix(in srgb, var(--bgColor-attention-muted) 36%, var(--bgColor-default));
    }

    .eyebrow {
      margin: 0 0 0.25rem;
      color: var(--fgColor-muted);
      font-size: var(--text-caption-size);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    .label {
      margin: 0;
      font-size: 0.95rem;
      font-weight: 600;
      line-height: 1.25;
      word-break: break-word;
    }

    .edge {
      fill: none;
      stroke: color-mix(in srgb, var(--fgColor-muted) 72%, transparent);
      stroke-width: 2.2;
      stroke-linecap: round;
    }

    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.75rem;
      color: var(--fgColor-muted);
      font-size: var(--text-caption-size);
    }

    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
    }

    .legend span::before {
      content: '';
      width: 0.7rem;
      height: 0.7rem;
      border-radius: 999px;
      background: var(--borderColor-muted);
      display: inline-block;
    }

    .legend .current::before { background: var(--borderColor-accent-emphasis); }
    .legend .upstream::before { background: var(--borderColor-success-emphasis); }
    .legend .downstream::before { background: var(--borderColor-attention-emphasis); }
  `;

  parseJSON(value) {
    try {
      const parsed = JSON.parse(value || '[]');
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  roleLabel(role) {
    switch ((role || '').toLowerCase()) {
      case 'current':
        return 'Current asset';
      case 'upstream':
        return 'Upstream';
      case 'downstream':
        return 'Downstream';
      default:
        return 'Related';
    }
  }

  nodeCenter(node) {
    const position = node.position || { x: 0, y: 0 };
    const width = 168;
    const height = 76;
    return {
      left: position.x,
      top: position.y,
      width,
      height,
      sourceX: position.x + width,
      sourceY: position.y + height / 2,
      targetX: position.x,
      targetY: position.y + height / 2,
    };
  }

  render() {
    const nodes = this.parseJSON(this.nodes);
    const edges = this.parseJSON(this.edges);
    const nodeMap = new Map(nodes.map((node) => [node.id, node]));

    return html`
      <div class="frame">
        <svg viewBox="0 0 820 420" preserveAspectRatio="xMidYMid meet" aria-hidden="true">
          <defs>
            <marker id="asset-graph-arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" fill="currentColor"></path>
            </marker>
          </defs>
          ${edges.map((edge) => {
            const source = nodeMap.get(edge.source);
            const target = nodeMap.get(edge.target);
            if (!source || !target) {
              return null;
            }
            const sourceBox = this.nodeCenter(source);
            const targetBox = this.nodeCenter(target);
            const [path] = getBezierPath({
              sourceX: sourceBox.sourceX,
              sourceY: sourceBox.sourceY,
              sourcePosition: Position.Right,
              targetX: targetBox.targetX,
              targetY: targetBox.targetY,
              targetPosition: Position.Left,
            });
            return html`<path class="edge" d=${path} marker-end="url(#asset-graph-arrow)"></path>`;
          })}
        </svg>
        <div class="node-layer">
          ${nodes.map((node) => html`
            <div
              class="node"
              data-role=${node.role || 'related'}
              style=${`left:${node.position?.x || 0}px; top:${node.position?.y || 0}px;`}
            >
              <p class="eyebrow">${this.roleLabel(node.role)}</p>
              <p class="label">${node.label || node.id}</p>
            </div>
          `)}
        </div>
      </div>
      <div class="legend" aria-hidden="true">
        <span class="current">Current asset</span>
        <span class="upstream">Upstream</span>
        <span class="downstream">Downstream</span>
      </div>
    `;
  }
}

if (!customElements.get('asset-graph-view')) {
  customElements.define('asset-graph-view', AssetGraphView);
}
