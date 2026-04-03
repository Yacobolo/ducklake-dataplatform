import { LitElement, css, html } from "lit";

import type { DashboardWidgetPayload } from "./dashboard-widget-payload";

class DuckMetric extends LitElement {
  static properties = {
    payloadJSON: { attribute: "data-metric-payload" },
  };

  static styles = css`
    :host {
      display: block;
      min-height: 10.5rem;
    }

    .empty,
    .metric {
      display: flex;
      min-height: 10.5rem;
      width: 100%;
      flex: 1;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0.75rem;
      text-align: center;
    }

    .empty {
      color: var(--fgColor-muted);
      padding: 1rem;
    }

    .value {
      margin: 0;
      color: var(--fgColor-default);
      font-size: clamp(2.9rem, 6vw, 4.9rem);
      font-weight: 900;
      letter-spacing: -0.06em;
      line-height: 1;
    }

    .secondary {
      margin: 0;
      color: var(--fgColor-muted);
      font-size: 0.6875rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
  `;

  payloadJSON = "";

  render() {
    const payload = this.parsePayload();
    if (!payload) {
      return html`<div class="empty">Loading metric...</div>`;
    }
    if (!payload.visual || payload.visual.kind !== "metric" || payload.rows.length === 0) {
      return html`<div class="empty">No metric data available.</div>`;
    }

    const field = payload.visual.encodings?.value?.field ?? payload.columns[0] ?? "";
    const fieldIndex = payload.columns.indexOf(field);
    const rawValue = fieldIndex >= 0 && payload.rows[0] ? payload.rows[0][fieldIndex] : payload.rows[0]?.[0];
    return html`
      <div class="metric">
        <p class="value">${formatMetricValue(field || payload.name || "", rawValue)}</p>
        <p class="secondary">${rowCountLabel(payload.row_count ?? payload.rows.length)}</p>
      </div>
    `;
  }

  setPayload(payload: DashboardWidgetPayload | null): void {
    this.payloadJSON = payload ? JSON.stringify(payload) : "";
  }

  private parsePayload(): DashboardWidgetPayload | null {
    try {
      return JSON.parse(this.payloadJSON || "null") as DashboardWidgetPayload | null;
    } catch {
      return null;
    }
  }
}

function rowCountLabel(count: number): string {
  return count === 1 ? "1 row" : `${count} rows`;
}

function formatMetricValue(label: string, rawValue: unknown): string {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return "-";
  }
  if (typeof rawValue !== "number") {
    const numeric = Number(rawValue);
    if (Number.isNaN(numeric)) {
      return String(rawValue);
    }
    rawValue = numeric;
  }
  const value = rawValue as number;
  if (looksCurrency(label)) {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      notation: Math.abs(value) >= 1000 ? "compact" : "standard",
      maximumFractionDigits: Math.abs(value) >= 1000 ? 2 : 2,
    }).format(value);
  }
  if (Math.abs(value - Math.round(value)) < 0.000001) {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Math.round(value));
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(value);
}

function looksCurrency(label: string): boolean {
  const normalized = label.trim().toLowerCase();
  return normalized.includes("revenue") || normalized.includes("amount") || normalized.includes("gross") || normalized.includes("cost") || normalized.includes("price");
}

customElements.define("duck-metric", DuckMetric);
