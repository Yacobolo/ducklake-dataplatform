import type { DashboardWidgetPayload } from "./dashboard-widget-payload";

type WidgetFilterSelection = {
  widgetKey: string;
  dimension: string;
  value: string;
};

export type DashboardFilterEventDetail = {
  selections: WidgetFilterSelection[];
};

export function extractSelectionsFromRow(
  payload: DashboardWidgetPayload,
  widgetOriginKey: string,
  values: Record<string, unknown>,
): WidgetFilterSelection[] {
  if (!payload.interaction?.can_initiate || !widgetOriginKey.trim()) {
    return [];
  }

  const selections: WidgetFilterSelection[] = [];
  for (const binding of payload.interaction.bindings ?? []) {
    const field = binding.field?.trim();
    if (!field) {
      continue;
    }
    const rawValue = values[field];
    if (rawValue === null || rawValue === undefined) {
      continue;
    }
    const value = String(rawValue).trim();
    if (!value) {
      continue;
    }
    selections.push({
      widgetKey: widgetOriginKey,
      dimension: binding.dimension,
      value,
    });
  }
  return selections;
}

export function isTableRowSelected(payload: DashboardWidgetPayload, values: Record<string, unknown>): boolean {
  const bindings = payload.interaction?.bindings ?? [];
  if (bindings.length === 0) {
    return false;
  }

  const originFilters = payload.interaction?.origin_filters ?? {};
  let hasRelevantFilter = false;
  for (const binding of bindings) {
    const activeValues = originFilters[binding.dimension] ?? [];
    if (activeValues.length === 0) {
      continue;
    }
    const field = binding.field?.trim();
    if (!field) {
      continue;
    }
    hasRelevantFilter = true;
    const rawValue = values[field];
    const candidate = rawValue === null || rawValue === undefined ? "" : String(rawValue).trim();
    if (!candidate || !activeValues.includes(candidate)) {
      return false;
    }
  }

  return hasRelevantFilter;
}
