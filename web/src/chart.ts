import { LitElement, css, html } from "lit";
import * as echarts from "echarts/core";
import { AriaComponent, DatasetComponent, GridComponent, LegendComponent, TitleComponent, TooltipComponent } from "echarts/components";
import { BarChart, LineChart, PieChart, ScatterChart } from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";

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

type VisualSpec = {
  kind: "table" | "metric" | "chart";
  chart_type?: "bar" | "line" | "area" | "pie" | "doughnut" | "scatter" | "stacked_bar";
  encodings?: {
    x?: { field: string };
    y?: { field: string };
    series?: { field: string };
    label?: { field: string };
    value?: { field: string };
  };
  title?: string;
  subtitle?: string;
  legend?: boolean;
  stacked?: boolean;
};

type ChartPayload = {
  columns: string[];
  rows: unknown[][];
  visual?: VisualSpec | null;
};

class DuckChart extends LitElement {
  static properties = {
    payloadJSON: { attribute: "data-chart-payload" },
  };

  static styles = css`
    :host {
      display: block;
      min-height: 20rem;
    }

    .frame {
      min-height: 20rem;
      border: 1px solid var(--borderColor-default);
      border-radius: var(--borderRadius-medium);
      background: var(--bgColor-default);
    }

    .empty {
      display: grid;
      place-items: center;
      min-height: 20rem;
      color: var(--fgColor-muted);
      padding: var(--space-3);
      text-align: center;
      border: 1px dashed var(--borderColor-muted);
      border-radius: var(--borderRadius-medium);
    }
  `;

  payloadJSON = "";
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
    if (!payload || !payload.visual || payload.visual.kind !== "chart" || payload.rows.length === 0) {
      return html`<div class="empty">No chart data available.</div>`;
    }
    return html`<div class="frame" aria-label=${payload.visual.title || "Chart"}></div>`;
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
    }
    this.chart.setOption(buildOption(payload), true);
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

  const common = {
    aria: { enabled: true },
    title: { text: visual.title || "", subtext: visual.subtitle || "" },
    tooltip: { trigger: "axis" },
    legend: { show: visual.legend !== false },
  } satisfies echarts.EChartsCoreOption;

  switch (visual.chart_type) {
    case "pie":
    case "doughnut": {
      const seriesData = payload.rows.map((row) => ({
        name: String(fieldValue(row, visual.encodings?.label?.field) ?? ""),
        value: Number(fieldValue(row, visual.encodings?.value?.field) ?? 0),
      }));
      return {
        ...common,
        tooltip: { trigger: "item" },
        series: [{
          type: "pie",
          radius: visual.chart_type === "doughnut" ? ["45%", "70%"] : "70%",
          data: seriesData,
        }],
      };
    }
    case "scatter":
      return {
        ...common,
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
      return {
        ...common,
        grid: { left: 24, right: 24, top: 48, bottom: 24, containLabel: true },
        xAxis: { type: "category", data: xLabels },
        yAxis: { type: "value" },
        series: Array.from(grouped.entries()).map(([seriesName, values]) => ({
          name: seriesName,
          type: seriesType,
          stack: visual.chart_type === "stacked_bar" || visual.stacked ? "total" : undefined,
          areaStyle: visual.chart_type === "area" ? {} : undefined,
          data: xLabels.map((label) => values.get(label) ?? 0),
        })),
      };
    }
  }
}

if (!customElements.get("duck-chart")) {
  customElements.define("duck-chart", DuckChart);
}
