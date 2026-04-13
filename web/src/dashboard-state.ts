type WidgetFilterSelection = {
  widgetKey: string;
  dimension: string;
  value: string;
};

export type DashboardFilterEventDetail = {
  selections: WidgetFilterSelection[];
};

export type DashboardTablePageEventDetail = {
  widgetId: string;
  offset: number;
  limit: number;
  append: boolean;
  sortColumn?: string | null;
  sortDirection?: "asc" | "desc" | null;
};

export type OriginFilter = {
  widgetKey: string;
  dimension: string;
  value: string;
};

export function readOriginFiltersFromURL(rawURL: string, baseOrigin: string): OriginFilter[] {
  const url = new URL(rawURL, baseOrigin);
  return parseOriginFilters(url.searchParams.getAll("fo"));
}

export function parseOriginFilters(rawFilters: string[]): OriginFilter[] {
  const filters: OriginFilter[] = [];
  const seen = new Set<string>();
  for (const rawFilter of rawFilters) {
    const [widgetKey, remainder] = rawFilter.split("|", 2);
    if (!widgetKey || !remainder) {
      continue;
    }
    const separatorIndex = remainder.indexOf(":");
    if (separatorIndex < 1 || separatorIndex === remainder.length - 1) {
      continue;
    }
    const filter: OriginFilter = {
      widgetKey: widgetKey.trim(),
      dimension: remainder.slice(0, separatorIndex).trim(),
      value: remainder.slice(separatorIndex + 1).trim(),
    };
    const key = originFilterKey(filter);
    if (!filter.widgetKey || !filter.dimension || !filter.value || seen.has(key)) {
      continue;
    }
    seen.add(key);
    filters.push(filter);
  }
  return filters;
}

export function serializeOriginFilters(filters: OriginFilter[]): string[] {
  return [...filters]
    .sort((left, right) => originFilterKey(left).localeCompare(originFilterKey(right)))
    .filter((filter) => Boolean(filter.widgetKey.trim() && filter.dimension.trim() && filter.value.trim()))
    .map((filter) => `${filter.widgetKey}|${filter.dimension}:${filter.value}`);
}

export function originFilterKey(filter: OriginFilter): string {
  return `${filter.widgetKey.trim()}|${filter.dimension.trim()}:${filter.value.trim()}`;
}

export function toggleOriginFilter(filters: OriginFilter[], widgetKey: string, dimension: string, value: string): void {
  const filter: OriginFilter = {
    widgetKey: widgetKey.trim(),
    dimension: dimension.trim(),
    value: value.trim(),
  };
  if (!filter.widgetKey || !filter.dimension || !filter.value) {
    return;
  }

  const targetKey = originFilterKey(filter);
  const existingIndex = filters.findIndex((item) => originFilterKey(item) === targetKey);
  if (existingIndex >= 0) {
    filters.splice(existingIndex, 1);
    return;
  }

  filters.push(filter);
}

export function removeOriginFilter(filters: OriginFilter[], dimension: string, value: string): void {
  const normalizedDimension = dimension.trim();
  const normalizedValue = value.trim();
  for (let index = filters.length - 1; index >= 0; index -= 1) {
    const filter = filters[index];
    if (filter.dimension === normalizedDimension && filter.value === normalizedValue) {
      filters.splice(index, 1);
    }
  }
}

export function buildDashboardViewURL(rawLocationHref: string, baseOrigin: string, viewPath: string | undefined, filters: OriginFilter[]): URL {
  const nextURL = new URL(rawLocationHref, baseOrigin);
  if (viewPath) {
    const resolvedViewURL = new URL(viewPath, baseOrigin);
    nextURL.pathname = resolvedViewURL.pathname;
    nextURL.search = resolvedViewURL.search;
  }
  nextURL.searchParams.delete("fo");
  for (const filter of serializeOriginFilters(filters)) {
    nextURL.searchParams.append("fo", filter);
  }
  return nextURL;
}

export function nextDashboardVersion(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function selectionEventKey(selections: WidgetFilterSelection[]): string {
  return [...selections]
    .map((selection) => `${selection.widgetKey.trim()}|${selection.dimension.trim()}:${selection.value.trim()}`)
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
    .join("||");
}
