export type VisualSpec = {
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
  legend_position?: "top" | "right" | "bottom" | "left";
  stacked?: boolean;
};

export type InteractionBinding = {
  encoding: "x" | "series" | "label" | "column";
  field?: string;
  dimension: string;
};

export type InteractionSpec = {
  participates: boolean;
  can_initiate: boolean;
  disabled_reason?: string;
  bindings?: InteractionBinding[];
  active_filters?: Record<string, string[]>;
  origin_filters?: Record<string, string[]>;
};

export type DashboardWidgetPayload = {
  name?: string;
  columns: string[];
  rows: unknown[][];
  row_count?: number;
  visual?: VisualSpec | null;
  interaction?: InteractionSpec | null;
  page?: {
    offset: number;
    append: boolean;
    has_more: boolean;
  } | null;
  sort?: {
    column: string;
    direction: "asc" | "desc";
  } | null;
};

export type DashboardWidgetStreamEvent = {
  widget_id: string;
  version: string;
  payload: DashboardWidgetPayload;
};
