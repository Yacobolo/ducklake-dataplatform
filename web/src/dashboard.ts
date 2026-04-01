import "./chart";
import "./table";

import type { DashboardWidgetPayload, DashboardWidgetPayloadEnvelope } from "./dashboard-widget-payload";

type ChartFilterSelection = {
  widgetId: string;
  dimension: string;
  value: string;
};

type DashboardChartFilterEventDetail = {
  selections: ChartFilterSelection[];
};

type DashboardTablePageEventDetail = {
  widgetId: string;
  offset: number;
  limit: number;
};

type DuckWidgetElement = HTMLElement & {
  setPayload: (payload: DashboardWidgetPayload | null) => void;
};

type OriginFilter = {
  widgetId: string;
  dimension: string;
  value: string;
};

class DashboardSurfaceController {
  private dataStream: EventSource | null = null;
  private dataStreamURL = "";
  private pendingFilterKey: string | null = null;
  private pendingShell = false;
  private pendingData = false;
  private readonly mutationObserver: MutationObserver;

  constructor() {
    this.mutationObserver = new MutationObserver(() => {
      this.completePendingShellIfReady();
    });

    document.documentElement.setAttribute("data-dashboard-loading", "false");
    this.bindEvents();
    this.observeSurface();
    this.connectDataStream();
  }

  private bindEvents(): void {
    document.addEventListener("dashboard-chart-filter", (event) => {
      void this.handleChartFilter(event as CustomEvent<DashboardChartFilterEventDetail>);
    });
    document.addEventListener("dashboard-table-page-request", (event) => {
      void this.handleTablePageRequest(event as CustomEvent<DashboardTablePageEventDetail>);
    });

    document.addEventListener("click", (event) => {
      void this.handleClick(event);
    });

    window.addEventListener("popstate", () => {
      void this.applyFiltersFromLocation(false);
    });
  }

  private async handleTablePageRequest(event: CustomEvent<DashboardTablePageEventDetail>): Promise<void> {
    const surface = this.getSurface();
    const tablePageURL = surface?.dataset.dashboardTablePageUrl;
    if (!surface || !tablePageURL) {
      return;
    }

    const response = await fetch(tablePageURL, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        csrfToken: this.readCSRFToken(surface),
        originFilters: this.serializeOriginFilters(this.readOriginFiltersFromURL(window.location.href)),
        widgetId: event.detail.widgetId,
        offset: event.detail.offset,
        limit: event.detail.limit,
      }),
    });
    void response;
  }

  private async handleChartFilter(event: CustomEvent<DashboardChartFilterEventDetail>): Promise<void> {
    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    const nextFilters = this.readOriginFiltersFromURL(window.location.href);
    for (const selection of event.detail.selections) {
      this.toggleFilter(nextFilters, selection.widgetId, selection.dimension, selection.value);
    }

    await this.applyFilters(surface, nextFilters, true);
  }

  private async handleClick(event: Event): Promise<void> {
    const target = event.target instanceof HTMLElement ? event.target.closest<HTMLElement>("[data-dashboard-clear-filters], [data-dashboard-remove-filter]") : null;
    if (!target) {
      return;
    }

    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    const nextFilters = this.readOriginFiltersFromURL(window.location.href);
    if (target.hasAttribute("data-dashboard-clear-filters")) {
      nextFilters.length = 0;
      await this.applyFilters(surface, nextFilters, true);
      return;
    }

    const dimension = target.dataset.dashboardFilterDimension;
    const value = target.dataset.dashboardFilterValue;
    if (!dimension || !value) {
      return;
    }

    this.removeFilter(nextFilters, dimension, value);
    await this.applyFilters(surface, nextFilters, true);
  }

  private async applyFiltersFromLocation(pushState: boolean): Promise<void> {
    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    const filters = this.readOriginFiltersFromURL(window.location.href);
    await this.applyFilters(surface, filters, pushState);
  }

  private getSurface(): HTMLElement | null {
    return document.querySelector<HTMLElement>("#dashboard-view-surface[data-dashboard-surface='true']");
  }

  private readOriginFiltersFromURL(rawURL: string): OriginFilter[] {
    const url = new URL(rawURL, window.location.origin);
    return this.parseOriginFilters(url.searchParams.getAll("fo"));
  }

  private parseOriginFilters(rawFilters: string[]): OriginFilter[] {
    const filters: OriginFilter[] = [];
    const seen = new Set<string>();
    for (const rawFilter of rawFilters) {
      const [widgetID, remainder] = rawFilter.split("|", 2);
      if (!widgetID || !remainder) {
        continue;
      }
      const separatorIndex = remainder.indexOf(":");
      if (separatorIndex < 1 || separatorIndex === remainder.length - 1) {
        continue;
      }
      const filter: OriginFilter = {
        widgetId: widgetID.trim(),
        dimension: remainder.slice(0, separatorIndex).trim(),
        value: remainder.slice(separatorIndex + 1).trim(),
      };
      const key = this.originFilterKey(filter);
      if (!filter.widgetId || !filter.dimension || !filter.value || seen.has(key)) {
        continue;
      }
      seen.add(key);
      filters.push(filter);
    }
    return filters;
  }

  private serializeOriginFilters(filters: OriginFilter[]): string[] {
    return [...filters]
      .sort((left, right) => this.originFilterKey(left).localeCompare(this.originFilterKey(right)))
      .filter((filter) => Boolean(filter.widgetId.trim() && filter.dimension.trim() && filter.value.trim()))
      .map((filter) => `${filter.widgetId}|${filter.dimension}:${filter.value}`);
  }

  private buildFilterKey(filters: OriginFilter[]): string {
    return [...filters]
      .map((filter) => this.originFilterKey(filter))
      .sort((left, right) => left.localeCompare(right))
      .join("|");
  }

  private originFilterKey(filter: OriginFilter): string {
    return `${filter.widgetId.trim()}|${filter.dimension.trim()}:${filter.value.trim()}`;
  }

  private toggleFilter(filters: OriginFilter[], widgetId: string, dimension: string, value: string): void {
    const filter: OriginFilter = {
      widgetId: widgetId.trim(),
      dimension: dimension.trim(),
      value: value.trim(),
    };
    if (!filter.widgetId || !filter.dimension || !filter.value) {
      return;
    }

    const targetKey = this.originFilterKey(filter);
    const existingIndex = filters.findIndex((item) => this.originFilterKey(item) === targetKey);
    if (existingIndex >= 0) {
      filters.splice(existingIndex, 1);
      return;
    }

    filters.push(filter);
  }

  private removeFilter(filters: OriginFilter[], dimension: string, value: string): void {
    const normalizedDimension = dimension.trim();
    const normalizedValue = value.trim();
    for (let index = filters.length - 1; index >= 0; index -= 1) {
      const filter = filters[index];
      if (filter.dimension === normalizedDimension && filter.value === normalizedValue) {
        filters.splice(index, 1);
      }
    }
  }

  private buildViewURL(surface: HTMLElement, filters: OriginFilter[]): URL {
    const nextURL = new URL(window.location.href);
    const viewPath = surface.dataset.dashboardViewUrl;
    if (viewPath) {
      nextURL.pathname = viewPath;
    }
    nextURL.searchParams.delete("fo");
    for (const filter of this.serializeOriginFilters(filters)) {
      nextURL.searchParams.append("fo", filter);
    }
    return nextURL;
  }

  private async applyFilters(surface: HTMLElement, filters: OriginFilter[], pushState: boolean): Promise<void> {
    const applyURL = surface.dataset.dashboardApplyUrl;
    if (!applyURL) {
      return;
    }

    const viewURL = this.buildViewURL(surface, filters);
    if (pushState) {
      window.history.pushState({}, "", viewURL);
    }

    this.startLoading(this.buildFilterKey(filters));

    const response = await fetch(applyURL, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        csrfToken: this.readCSRFToken(surface),
        originFilters: this.serializeOriginFilters(filters),
      }),
    });
    if (!response.ok) {
      this.resetLoading();
      return;
    }
  }

  private connectDataStream(): void {
    const surface = this.getSurface();
    const nextURL = surface?.dataset.dashboardDataStreamUrl ?? "";
    if (!nextURL || this.dataStreamURL === nextURL) {
      return;
    }

    this.dataStream?.close();
    this.dataStreamURL = nextURL;
    this.dataStream = new EventSource(nextURL);
    this.dataStream.addEventListener("dashboard-widget-payloads", (event) => {
      this.handleWidgetPayloads(event as MessageEvent<string>);
    });
  }

  private handleWidgetPayloads(event: MessageEvent<string>): void {
    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    let payloads: DashboardWidgetPayloadEnvelope;
    try {
      payloads = JSON.parse(event.data) as DashboardWidgetPayloadEnvelope;
    } catch {
      return;
    }

    this.completePendingDataIfReady(payloads.filter_key ?? "");

    const widgetPayloads = payloads.widgets ?? {};
    const widgets = surface.querySelectorAll("duck-chart[data-widget-id], duck-table[data-widget-id]");
    for (const widget of widgets) {
      const element = widget as DuckWidgetElement;
      const widgetID = element.dataset.widgetId ?? "";
      if (!(widgetID in widgetPayloads)) {
        continue;
      }
      element.setPayload(widgetPayloads[widgetID] ?? null);
    }
  }

  private readCSRFToken(surface: HTMLElement): string {
    const attrToken = surface.dataset.dashboardCsrfToken?.trim();
    if (attrToken) {
      return attrToken;
    }

    const input = surface.querySelector<HTMLInputElement>("input[name='csrf_token']") ?? document.querySelector<HTMLInputElement>("input[name='csrf_token']");
    return input?.value?.trim() ?? "";
  }

  private observeSurface(): void {
    this.mutationObserver.observe(document.body, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-dashboard-filter-key"],
    });
  }

  private startLoading(filterKey: string): void {
    this.pendingFilterKey = filterKey;
    this.pendingShell = true;
    this.pendingData = true;
    document.documentElement.setAttribute("data-dashboard-loading", "true");
    this.completePendingShellIfReady();
  }

  private completePendingShellIfReady(): void {
    if (!this.pendingShell) {
      return;
    }

    const currentKey = this.getSurface()?.dataset.dashboardFilterKey ?? "";
    if (currentKey !== (this.pendingFilterKey ?? "")) {
      return;
    }

    this.pendingShell = false;
    this.connectDataStream();
    this.finishLoadingIfReady();
  }

  private completePendingDataIfReady(filterKey: string): void {
    if (!this.pendingData) {
      return;
    }
    if ((this.pendingFilterKey ?? "") !== filterKey) {
      return;
    }

    this.pendingData = false;
    this.finishLoadingIfReady();
  }

  private finishLoadingIfReady(): void {
    if (this.pendingShell || this.pendingData) {
      return;
    }
    this.resetLoading();
  }

  private resetLoading(): void {
    this.pendingFilterKey = null;
    this.pendingShell = false;
    this.pendingData = false;
    document.documentElement.setAttribute("data-dashboard-loading", "false");
  }
}

new DashboardSurfaceController();
