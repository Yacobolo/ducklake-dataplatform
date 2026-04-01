import { LitElement, css, html } from "lit";
import * as echarts from "echarts/core";
import { AriaComponent, DatasetComponent, GridComponent, LegendComponent, TitleComponent, TooltipComponent } from "echarts/components";
import { BarChart, LineChart, PieChart, ScatterChart } from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";

import type { DashboardWidgetPayload, InteractionBinding, InteractionSpec, VisualSpec } from "./dashboard-widget-payload";

echarts.use([
  AriaComponent,
  BarChart,
  CanvasRenderer,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  LineChart,
  PieChart,
  ScatterChart,
  TitleComponent,
  TooltipComponent,
]);

type ChartPayload = DashboardWidgetPayload;

type ChartFilterSelection = {
  widgetId: string;
  dimension: string;
  value: string;
};

type DashboardChartFilterEventDetail = {
  selections: ChartFilterSelection[];
};

type PointSelectionContext = Partial<Record<InteractionBinding["encoding"], string>>;

class DuckChart extends LitElement {
  static properties = {
    widgetId: { attribute: "data-widget-id" },
    payloadJSON: { attribute: "data-chart-payload" },
  };

  static styles = css`
    :host {
      display: block;
      min-height: 20rem;
    }

    .frame {
      min-height: 20rem;
      background: transparent;
    }

    .frame--interactive {
      cursor: pointer;
    }

    .empty {
      display: grid;
      place-items: center;
      min-height: 20rem;
      color: var(--fgColor-muted);
      padding: 0.75rem;
      text-align: center;
    }
  `;

  payloadJSON = "";
  widgetId = "";
  private chart: echarts.ECharts | null = null;
  private resizeObserver: ResizeObserver | null = null;

  connectedCallback(): void {
    super.connectedCallback();
    this.resizeObserver = new ResizeObserver(() => {
      this.chart?.resize();
    });
  }

  disconnectedCallback(): void {
    this.resizeObserver?.disconnect();
    this.chart?.dispose();
    this.chart = null;
    super.disconnectedCallback();
  }

  firstUpdated(): void {
    const frame = this.renderRoot.querySelector<HTMLElement>(".frame");
    if (frame) {
      this.resizeObserver?.observe(frame);
    }
    this.renderChart();
  }

  updated(): void {
    this.renderChart();
  }

  render() {
    const payload = this.parsePayload();
    if (!payload) {
      return html`<div class="empty">Loading chart...</div>`;
    }
    if (!payload.visual || payload.visual.kind !== "chart" || payload.rows.length === 0) {
      return html`<div class="empty">No chart data available.</div>`;
    }

    const interactiveClass = payload.interaction?.can_initiate ? "frame frame--interactive" : "frame";
    return html`<div class=${interactiveClass} aria-label=${payload.name || "Chart"}></div>`;
  }

  setPayload(payload: ChartPayload | null): void {
    this.payloadJSON = payload ? JSON.stringify(payload) : "";
  }

  private parsePayload(): ChartPayload | null {
    try {
      return JSON.parse(this.payloadJSON || "{}") as ChartPayload;
    } catch {
      return null;
    }
  }

  private renderChart(): void {
    const payload = this.parsePayload();
    const frame = this.renderRoot.querySelector<HTMLElement>(".frame");
    if (!(payload && frame && payload.visual && payload.visual.kind === "chart" && payload.rows.length > 0)) {
      this.chart?.dispose();
      this.chart = null;
      return;
    }

    if (!this.chart) {
      this.chart = echarts.init(frame);
      this.chart.on("click", (params) => {
        this.handleChartClick(params);
      });
    }

    this.chart.setOption(buildOption(payload), true);
  }

  private handleChartClick(params: unknown): void {
    const payload = this.parsePayload();
    if (!payload?.interaction?.can_initiate) {
      return;
    }

    const selections = extractSelectionsFromClick(this.widgetId, payload, params as { name?: unknown; seriesName?: unknown });
    if (selections.length === 0) {
      return;
    }

    this.dispatchEvent(new CustomEvent<DashboardChartFilterEventDetail>("dashboard-chart-filter", {
      bubbles: true,
      composed: true,
      detail: { selections },
    }));
  }
}

function buildOption(payload: ChartPayload): echarts.EChartsCoreOption {
  const visual = payload.visual!;
  const columnIndex = new Map(payload.columns.map((column, index) => [column, index]));
  const fieldValue = (row: unknown[], field?: string) => {
    if (!field) {
      return null;
    }
    const idx = columnIndex.get(field);
    return idx === undefined ? null : row[idx];
  };

  switch (visual.chart_type) {
    case "pie":
    case "doughnut": {
      const seriesData = payload.rows.map((row) => {
        const label = String(fieldValue(row, visual.encodings?.label?.field) ?? "");
        const dimmed = shouldDimPoint(payload, { label });
        return {
          name: label,
          value: Number(fieldValue(row, visual.encodings?.value?.field) ?? 0),
          itemStyle: { opacity: dimmed ? 0.28 : 1 },
        };
      });
      const legend = resolveLegendConfig(visual, seriesData.length > 0 ? 1 : 0);

      return {
        aria: { enabled: true },
        title: { text: "", subtext: "" },
        tooltip: { trigger: "item" },
        legend: legend.option,
        series: [{
          type: "pie",
          radius: visual.chart_type === "doughnut" ? ["45%", "68%"] : "66%",
          center: legend.pieCenter,
          data: seriesData,
        }],
      };
    }
    case "scatter": {
      const legend = resolveLegendConfig(visual, 0);
      return {
        aria: { enabled: true },
        title: { text: "", subtext: "" },
        tooltip: { trigger: "item" },
        legend: legend.option,
        grid: legend.grid,
        xAxis: { type: "value" },
        yAxis: { type: "value" },
        series: [{
          type: "scatter",
          data: payload.rows.map((row) => [
            Number(fieldValue(row, visual.encodings?.x?.field) ?? 0),
            Number(fieldValue(row, visual.encodings?.y?.field) ?? 0),
          ]),
        }],
      };
    }
    default: {
      const xField = visual.encodings?.x?.field;
      const yField = visual.encodings?.y?.field;
      const seriesField = visual.encodings?.series?.field;
      const xLabels = Array.from(new Set(payload.rows.map((row) => String(fieldValue(row, xField) ?? ""))));
      const grouped = new Map<string, Map<string, number>>();

      for (const row of payload.rows) {
        const seriesKey = String(fieldValue(row, seriesField) ?? yField ?? "value");
        const xKey = String(fieldValue(row, xField) ?? "");
        const value = Number(fieldValue(row, yField) ?? 0);
        if (!grouped.has(seriesKey)) {
          grouped.set(seriesKey, new Map());
        }
        grouped.get(seriesKey)!.set(xKey, value);
      }

      const seriesType = visual.chart_type === "line" || visual.chart_type === "area" ? "line" : "bar";
      const legend = resolveLegendConfig(visual, grouped.size);
      return {
        aria: { enabled: true },
        title: { text: "", subtext: "" },
        tooltip: { trigger: "axis" },
        legend: legend.option,
        grid: legend.grid,
        xAxis: { type: "category", data: xLabels },
        yAxis: { type: "value" },
        series: Array.from(grouped.entries()).map(([seriesName, values]) => {
          const seriesDimmed = shouldDimPoint(payload, { series: seriesName });
          return {
            name: seriesName,
            type: seriesType,
            stack: visual.chart_type === "stacked_bar" || visual.stacked ? "total" : undefined,
            areaStyle: visual.chart_type === "area"
              ? { opacity: seriesDimmed ? 0.12 : 0.24 }
              : undefined,
            lineStyle: seriesType === "line" ? { opacity: seriesDimmed ? 0.35 : 1 } : undefined,
            itemStyle: { opacity: seriesDimmed ? 0.35 : 1 },
            data: xLabels.map((label) => ({
              value: values.get(label) ?? 0,
              itemStyle: { opacity: shouldDimPoint(payload, { x: label, series: seriesName }) ? 0.2 : 1 },
            })),
          };
        }),
      };
    }
  }
}

function resolveLegendConfig(visual: VisualSpec, seriesCount: number): {
  option: Record<string, unknown>;
  grid: { left: number; right: number; top: number; bottom: number; containLabel: boolean };
  pieCenter: [string, string];
} {
  const position = visual.legend_position ?? "top";
  const show = shouldShowLegend(visual, seriesCount);

  const grid = { left: 24, right: 24, top: 24, bottom: 24, containLabel: true };
  const pieCenter: [string, string] = ["50%", "56%"];

  if (!show) {
    return {
      option: { show: false },
      grid,
      pieCenter: ["50%", "52%"],
    };
  }

  switch (position) {
    case "bottom":
      return {
        option: { show: true, bottom: 0, left: 0, right: 24, orient: "horizontal" },
        grid: { ...grid, top: 24, bottom: 64 },
        pieCenter: ["50%", "44%"],
      };
    case "left":
      return {
        option: { show: true, top: "middle", left: 0, orient: "vertical" },
        grid: { ...grid, left: 156, right: 24, top: 24, bottom: 24 },
        pieCenter: ["62%", "52%"],
      };
    case "right":
      return {
        option: { show: true, top: "middle", right: 0, orient: "vertical" },
        grid: { ...grid, left: 24, right: 156, top: 24, bottom: 24 },
        pieCenter: ["38%", "52%"],
      };
    case "top":
    default:
      return {
        option: { show: true, top: 0, left: 0, right: 24, orient: "horizontal" },
        grid: { ...grid, top: 64, bottom: 24 },
        pieCenter,
      };
  }
}

function shouldShowLegend(visual: VisualSpec, seriesCount: number): boolean {
  if (visual.legend === true) {
    return true;
  }
  if (visual.legend === false) {
    return false;
  }

  switch (visual.chart_type) {
    case "pie":
    case "doughnut":
      return true;
    case "line":
    case "area":
    case "stacked_bar":
      return seriesCount > 1;
    case "bar":
      return seriesCount > 1;
    default:
      return false;
  }
}

function extractSelectionsFromClick(widgetId: string, payload: ChartPayload, params: { name?: unknown; seriesName?: unknown }): ChartFilterSelection[] {
  const bindings = payload.interaction?.bindings ?? [];
  const selections: ChartFilterSelection[] = [];
  if (!widgetId.trim()) {
    return selections;
  }

  for (const binding of bindings) {
    const rawValue = binding.encoding === "series" ? params.seriesName : params.name;
    if (rawValue === undefined || rawValue === null) {
      continue;
    }
    const value = String(rawValue).trim();
    if (value === "") {
      continue;
    }
    selections.push({
      widgetId,
      dimension: binding.dimension,
      value,
    });
  }

  return selections;
}

function shouldDimPoint(payload: ChartPayload, values: PointSelectionContext): boolean {
  const interaction = payload.interaction;
  if (!interaction?.participates) {
    return false;
  }

  let hasRelevantSelection = false;
  for (const binding of interaction.bindings ?? []) {
    const activeValues = interaction.active_filters?.[binding.dimension] ?? [];
    if (activeValues.length === 0) {
      continue;
    }
    hasRelevantSelection = true;
    const candidate = values[binding.encoding];
    if (!candidate || !activeValues.includes(candidate)) {
      return true;
    }
  }

  return hasRelevantSelection ? false : false;
}

if (!customElements.get("duck-chart")) {
  customElements.define("duck-chart", DuckChart);
}
