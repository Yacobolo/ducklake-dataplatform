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

type WidgetFilterSelection = {
  widgetKey: string;
  dimension: string;
  value: string;
};

type DashboardFilterEventDetail = {
  selections: WidgetFilterSelection[];
};

type PointSelectionContext = Partial<Record<InteractionBinding["encoding"], string>>;

class DuckChart extends LitElement {
  static properties = {
    widgetId: { attribute: "data-widget-id" },
    widgetOriginKey: { attribute: "data-widget-origin-key" },
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
  widgetOriginKey = "";
  private chart: echarts.ECharts | null = null;
  private resizeObserver: ResizeObserver | null = null;
  private frameListener: ((event: PointerEvent) => void) | null = null;
  private frameElement: HTMLElement | null = null;
  private lastSelectionDispatchAt = 0;
  private lastSelectionDispatchKey = "";
  private nativeFallbackTimer: number | null = null;
  private lastChartClickAt = 0;

  connectedCallback(): void {
    super.connectedCallback();
    this.resizeObserver = new ResizeObserver(() => {
      this.chart?.resize();
    });
  }

  disconnectedCallback(): void {
    this.clearNativeFallbackTimer();
    this.detachFrameListener();
    this.resizeObserver?.disconnect();
    this.chart?.dispose();
    this.chart = null;
    super.disconnectedCallback();
  }

  firstUpdated(): void {
    const frame = this.renderRoot.querySelector<HTMLElement>(".frame");
    if (frame) {
      this.resizeObserver?.observe(frame);
      this.attachFrameListener(frame);
    }
    this.renderChart();
  }

  updated(): void {
    const frame = this.renderRoot.querySelector<HTMLElement>(".frame");
    if (frame) {
      this.attachFrameListener(frame);
    } else {
      this.detachFrameListener();
    }
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

  setPayload(payload: DashboardWidgetPayload | null): void {
    this.payloadJSON = payload ? JSON.stringify(payload) : "";
  }

  private parsePayload(): DashboardWidgetPayload | null {
    if (!this.payloadJSON) {
      return null;
    }
    try {
      return JSON.parse(this.payloadJSON) as DashboardWidgetPayload;
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
    this.clearNativeFallbackTimer();
    const payload = this.parsePayload();
    if (!payload?.interaction?.can_initiate) {
      return;
    }

    const selections = extractSelectionsFromClick(this.widgetOriginKey, payload, params as { name?: unknown; seriesName?: unknown });
    if (selections.length === 0) {
      return;
    }

    this.lastChartClickAt = Date.now();
    this.dispatchSelections(selections);
  }

  private handleNativeClick(event: PointerEvent): void {
    const clientX = event.clientX;
    const clientY = event.clientY;
    this.clearNativeFallbackTimer();
    this.nativeFallbackTimer = window.setTimeout(() => {
      this.nativeFallbackTimer = null;
      if (Date.now() - this.lastChartClickAt < 64) {
        return;
      }
      if (Date.now() - this.lastSelectionDispatchAt < 32) {
        return;
      }
      const payload = this.parsePayload();
      const frame = this.frameElement;
      if (!payload?.interaction?.can_initiate || !this.chart || !frame) {
        return;
      }

      const rect = frame.getBoundingClientRect();
      const localX = clientX - rect.left;
      const localY = clientY - rect.top;
      const hover = this.chart.getZr()?.handler?.findHover?.(localX, localY);
      const fallbackParams = extractSelectionsFromNativeClick(this.widgetOriginKey, payload, hover?.target as NativeChartTarget | undefined);
      if (fallbackParams.length === 0) {
        return;
      }

      this.dispatchSelections(fallbackParams);
    }, 24);
  }

  private dispatchSelections(selections: WidgetFilterSelection[]): void {
    const dispatchKey = selections
      .map((selection) => `${selection.widgetKey}|${selection.dimension}:${selection.value}`)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
    const now = Date.now();
    if (dispatchKey && this.lastSelectionDispatchKey === dispatchKey && now - this.lastSelectionDispatchAt < 400) {
      return;
    }
    this.lastSelectionDispatchKey = dispatchKey;
    this.lastSelectionDispatchAt = Date.now();
    this.dispatchEvent(new CustomEvent<DashboardFilterEventDetail>("dashboard-filter-select", {
      bubbles: true,
      composed: true,
      detail: { selections },
    }));
  }

  private attachFrameListener(frame: HTMLElement): void {
    if (this.frameElement === frame) {
      return;
    }
    this.detachFrameListener();
    this.frameElement = frame;
    this.frameListener = (event: PointerEvent) => {
      this.handleNativeClick(event);
    };
    frame.addEventListener("pointerup", this.frameListener, true);
  }

  private detachFrameListener(): void {
    if (this.frameElement && this.frameListener) {
      this.frameElement.removeEventListener("pointerup", this.frameListener, true);
    }
    this.frameElement = null;
    this.frameListener = null;
  }

  private clearNativeFallbackTimer(): void {
    if (this.nativeFallbackTimer !== null) {
      window.clearTimeout(this.nativeFallbackTimer);
      this.nativeFallbackTimer = null;
    }
  }
}

type NativeChartTarget = {
  __dataIndex?: number;
  [key: `__ec_inner_${string}`]: {
    dataIndex?: number;
    seriesIndex?: number;
  } | undefined;
};

function buildOption(payload: DashboardWidgetPayload): echarts.EChartsCoreOption {
  const visual = payload.visual!;

  switch (visual.chart_type) {
    case "pie":
    case "doughnut": {
      const seriesData = payload.rows.map((row) => {
        const label = String(fieldValueFromPayload(payload, row, visual.encodings?.label?.field, 0) ?? "");
        const dimmed = shouldDimPoint(payload, { label });
        return {
          name: label,
          value: Number(fieldValueFromPayload(payload, row, visual.encodings?.value?.field, 1) ?? 0),
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
          data: payload.rows.map((row) => ({
            name: String(fieldValueFromPayload(payload, row, visual.encodings?.label?.field, 0) ?? ""),
            value: [
              Number(fieldValueFromPayload(payload, row, visual.encodings?.x?.field, 0) ?? 0),
              Number(fieldValueFromPayload(payload, row, visual.encodings?.y?.field, 1) ?? 0),
            ],
          })),
        }],
      };
    }
    default: {
      const xField = visual.encodings?.x?.field;
      const yField = visual.encodings?.y?.field;
      const seriesField = visual.encodings?.series?.field;
      const xLabels = Array.from(new Set(payload.rows.map((row) => String(fieldValueFromPayload(payload, row, xField, 0) ?? ""))));
      const grouped = new Map<string, Map<string, number>>();

      for (const row of payload.rows) {
        const seriesKey = String(fieldValueFromPayload(payload, row, seriesField, 1) ?? yField ?? "value");
        const xKey = String(fieldValueFromPayload(payload, row, xField, 0) ?? "");
        const value = Number(fieldValueFromPayload(payload, row, yField, 1) ?? 0);
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

function extractSelectionsFromClick(widgetKey: string, payload: DashboardWidgetPayload, params: { name?: unknown; seriesName?: unknown }): WidgetFilterSelection[] {
  const bindings = payload.interaction?.bindings ?? [];
  const selections: WidgetFilterSelection[] = [];
  if (!widgetKey.trim()) {
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
      widgetKey,
      dimension: binding.dimension,
      value,
    });
  }

  return selections;
}

function extractSelectionsFromNativeClick(widgetKey: string, payload: DashboardWidgetPayload, target?: NativeChartTarget): WidgetFilterSelection[] {
  if (!target) {
    return [];
  }

  const ecInner = Object.entries(target).find(([key]) => key.startsWith("__ec_inner_"))?.[1];
  const dataIndex = typeof ecInner?.dataIndex === "number"
    ? ecInner.dataIndex
    : typeof target.__dataIndex === "number"
      ? target.__dataIndex
      : -1;
  const seriesIndex = typeof ecInner?.seriesIndex === "number" ? ecInner.seriesIndex : 0;
  if (dataIndex < 0) {
    return [];
  }

  const visual = payload.visual;
  if (!visual || visual.kind !== "chart") {
    return [];
  }

  switch (visual.chart_type) {
    case "pie":
    case "doughnut": {
      const labelField = visual.encodings?.label?.field;
      const row = payload.rows[dataIndex];
      const labelValue = row ? fieldValueFromPayload(payload, row, labelField, 0) : null;
      return extractSelectionsFromClick(widgetKey, payload, { name: labelValue });
    }
    case "scatter": {
      const labelField = visual.encodings?.label?.field;
      const row = payload.rows[dataIndex];
      const labelValue = row ? fieldValueFromPayload(payload, row, labelField, 0) : null;
      return extractSelectionsFromClick(widgetKey, payload, { name: labelValue });
    }
    default: {
      const xField = visual.encodings?.x?.field;
      const seriesField = visual.encodings?.series?.field;
      const xLabels = Array.from(new Set(payload.rows.map((row) => String(fieldValueFromPayload(payload, row, xField, 0) ?? ""))));
      const seriesNames = Array.from(new Set(payload.rows.map((row) => String(fieldValueFromPayload(payload, row, seriesField, 1) ?? visual.encodings?.y?.field ?? "value"))));
      return extractSelectionsFromClick(widgetKey, payload, {
        name: xLabels[dataIndex] ?? null,
        seriesName: seriesNames[seriesIndex] ?? null,
      });
    }
  }
}

function fieldValueFromPayload(payload: DashboardWidgetPayload, row: unknown[], field: string | undefined, fallbackIndex: number): unknown {
  if (!field) {
    return row[fallbackIndex] ?? null;
  }
  const index = payload.columns.indexOf(field);
  if (index < 0) {
    return row[fallbackIndex] ?? null;
  }
  return row[index] ?? null;
}

function shouldDimPoint(payload: DashboardWidgetPayload, values: PointSelectionContext): boolean {
  const interaction = payload.interaction;
  if (!interaction?.participates) {
    return false;
  }

  for (const binding of interaction.bindings ?? []) {
    const activeValues = interaction.active_filters?.[binding.dimension] ?? [];
    if (activeValues.length === 0) {
      continue;
    }
    const candidate = values[binding.encoding];
    if (!candidate || !activeValues.includes(candidate)) {
      return true;
    }
  }

  return false;
}

if (!customElements.get("duck-chart")) {
  customElements.define("duck-chart", DuckChart);
}
