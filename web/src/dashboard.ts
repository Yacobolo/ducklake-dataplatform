import "./chart";
import "./metric";
import "./table";

import type { DashboardWidgetPayload, DashboardWidgetStreamEvent } from "./dashboard-widget-payload";

type WidgetFilterSelection = {
  widgetKey: string;
  dimension: string;
  value: string;
};

type DashboardFilterEventDetail = {
  selections: WidgetFilterSelection[];
};

type DashboardTablePageEventDetail = {
  widgetId: string;
  offset: number;
  limit: number;
  append: boolean;
  sortColumn?: string | null;
  sortDirection?: "asc" | "desc" | null;
};

type DuckWidgetElement = HTMLElement & {
  setPayload: (payload: DashboardWidgetPayload | null) => void;
};

type OriginFilter = {
  widgetKey: string;
  dimension: string;
  value: string;
};

type DashboardPayloadBus = Record<string, DashboardWidgetStreamEvent>;

class DashboardSurfaceController {
  private activeVersion = "";
  private pendingVersion: string | null = null;
  private pendingShell = false;
  private pendingShellRevision = 0;
  private pendingWidgetIDs = new Set<string>();
  private surfaceRevision = 0;
  private lastFilterEventKey = "";
  private lastFilterEventAt = 0;
  private readonly mutationObserver: MutationObserver;

  constructor() {
    this.mutationObserver = new MutationObserver((mutations) => {
      if (mutations.some((mutation) => mutation.target instanceof Node && mutation.target.parentElement?.closest("#dashboard-view-root"))) {
        this.surfaceRevision += 1;
      }
      this.completePendingShellIfReady();
    });

    document.documentElement.setAttribute("data-dashboard-loading", "false");
    this.bindEvents();
    this.observeSurface();
    this.primeInitialSurface();
  }

  private bindEvents(): void {
    document.addEventListener("dashboard-filter-select", (event) => {
      void this.handleWidgetFilter(event as CustomEvent<DashboardFilterEventDetail>);
    });
    document.addEventListener("dashboard-table-page-request", (event) => {
      void this.handleTablePageRequest(event as CustomEvent<DashboardTablePageEventDetail>);
    });
    window.addEventListener("dashboard-widget-payload", (event) => {
      this.handleWidgetPayload(event as CustomEvent<DashboardWidgetStreamEvent>);
    });

    document.addEventListener("click", (event) => {
      this.handlePageNavigation(event);
    }, true);

    document.addEventListener("click", (event) => {
      void this.handleClick(event);
    });

    window.addEventListener("popstate", () => {
      void this.applyFiltersFromLocation(false);
    });
  }

  private async handleTablePageRequest(event: CustomEvent<DashboardTablePageEventDetail>): Promise<void> {
    const root = this.getRoot();
    const tablePageURL = root?.dataset.dashboardTablePageUrl;
    if (!root || !tablePageURL) {
      return;
    }

    const response = await fetch(tablePageURL, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        csrfToken: this.readCSRFToken(root),
        originFilters: this.serializeOriginFilters(this.readOriginFiltersFromURL(window.location.href)),
        version: this.activeVersion,
        widgetId: event.detail.widgetId,
        offset: event.detail.offset,
        limit: event.detail.limit,
        append: event.detail.append,
        sortColumn: event.detail.sortColumn ?? "",
        sortDirection: event.detail.sortDirection ?? "",
      }),
    });
    void response;
  }

  private async handleWidgetFilter(event: CustomEvent<DashboardFilterEventDetail>): Promise<void> {
    const root = this.getRoot();
    if (!root) {
      return;
    }
    const eventKey = this.selectionEventKey(event.detail.selections);
    const now = Date.now();
    if (eventKey && this.lastFilterEventKey === eventKey && now - this.lastFilterEventAt < 400) {
      return;
    }
    this.lastFilterEventKey = eventKey;
    this.lastFilterEventAt = now;

    const nextFilters = this.readOriginFiltersFromURL(window.location.href);
    for (const selection of event.detail.selections) {
      this.toggleFilter(nextFilters, selection.widgetKey, selection.dimension, selection.value);
    }

    await this.applyFilters(root, nextFilters, true);
  }

  private async handleClick(event: Event): Promise<void> {
    const target = event.target instanceof HTMLElement ? event.target.closest<HTMLElement>("[data-dashboard-clear-filters], [data-dashboard-remove-filter]") : null;
    if (!target) {
      return;
    }

    const root = this.getRoot();
    if (!root) {
      return;
    }

    const nextFilters = this.readOriginFiltersFromURL(window.location.href);
    if (target.hasAttribute("data-dashboard-clear-filters")) {
      nextFilters.length = 0;
      await this.applyFilters(root, nextFilters, true);
      return;
    }

    const dimension = target.dataset.dashboardFilterDimension;
    const value = target.dataset.dashboardFilterValue;
    if (!dimension || !value) {
      return;
    }

    this.removeFilter(nextFilters, dimension, value);
    await this.applyFilters(root, nextFilters, true);
  }

  private handlePageNavigation(event: Event): void {
    const target = event.target instanceof HTMLElement ? event.target.closest<HTMLAnchorElement>("[data-dashboard-page-link]") : null;
    if (!target) {
      return;
    }

    const mouseEvent = event as MouseEvent;
    if (mouseEvent.defaultPrevented || mouseEvent.button !== 0 || mouseEvent.metaKey || mouseEvent.ctrlKey || mouseEvent.shiftKey || mouseEvent.altKey) {
      return;
    }

    const href = target.href;
    if (!href) {
      return;
    }

    event.preventDefault();
    window.location.assign(href);
  }

  private async applyFiltersFromLocation(pushState: boolean): Promise<void> {
    const root = this.getRoot();
    if (!root) {
      return;
    }

    const filters = this.readOriginFiltersFromURL(window.location.href);
    await this.applyFilters(root, filters, pushState);
  }

  private primeInitialSurface(): void {
    const root = this.getRoot();
    if (!root) {
      return;
    }
    this.activeVersion = this.rootVersion(root);
    this.pendingVersion = this.activeVersion || null;
    this.pendingShell = false;
    this.prepareWidgetsForCurrentVersion(root);
    this.drainStoredPayloads(root);
    if (this.pendingWidgetIDs.size > 0) {
      document.documentElement.setAttribute("data-dashboard-loading", "true");
    }
  }

  private getRoot(): HTMLElement | null {
    return document.querySelector<HTMLElement>("#dashboard-view-root[data-dashboard-surface='true']");
  }

  private readOriginFiltersFromURL(rawURL: string): OriginFilter[] {
    const url = new URL(rawURL, window.location.origin);
    return this.parseOriginFilters(url.searchParams.getAll("fo"));
  }

  private parseOriginFilters(rawFilters: string[]): OriginFilter[] {
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
      const key = this.originFilterKey(filter);
      if (!filter.widgetKey || !filter.dimension || !filter.value || seen.has(key)) {
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
      .filter((filter) => Boolean(filter.widgetKey.trim() && filter.dimension.trim() && filter.value.trim()))
      .map((filter) => `${filter.widgetKey}|${filter.dimension}:${filter.value}`);
  }

  private originFilterKey(filter: OriginFilter): string {
    return `${filter.widgetKey.trim()}|${filter.dimension.trim()}:${filter.value.trim()}`;
  }

  private toggleFilter(filters: OriginFilter[], widgetKey: string, dimension: string, value: string): void {
    const filter: OriginFilter = {
      widgetKey: widgetKey.trim(),
      dimension: dimension.trim(),
      value: value.trim(),
    };
    if (!filter.widgetKey || !filter.dimension || !filter.value) {
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

  private buildViewURL(root: HTMLElement, filters: OriginFilter[]): URL {
    const nextURL = new URL(window.location.href);
    const viewPath = root.dataset.dashboardViewUrl;
    if (viewPath) {
      const resolvedViewURL = new URL(viewPath, window.location.origin);
      nextURL.pathname = resolvedViewURL.pathname;
      nextURL.search = resolvedViewURL.search;
    }
    nextURL.searchParams.delete("fo");
    for (const filter of this.serializeOriginFilters(filters)) {
      nextURL.searchParams.append("fo", filter);
    }
    return nextURL;
  }

  private async applyFilters(root: HTMLElement, filters: OriginFilter[], pushState: boolean): Promise<void> {
    const applyURL = root.dataset.dashboardApplyUrl;
    if (!applyURL) {
      return;
    }

    const viewURL = this.buildViewURL(root, filters);
    if (pushState) {
      window.history.pushState({}, "", viewURL);
    }

    const nextVersion = this.nextVersion();
    this.activeVersion = nextVersion;
    this.startLoading(root, nextVersion);

    const response = await fetch(applyURL, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        csrfToken: this.readCSRFToken(root),
        originFilters: this.serializeOriginFilters(filters),
        version: nextVersion,
      }),
    });
    if (!response.ok) {
      this.resetLoading();
      return;
    }
  }

  private handleWidgetPayload(event: CustomEvent<DashboardWidgetStreamEvent>): void {
    const root = this.getRoot();
    if (!root) {
      return;
    }
    const detail = event.detail;
    if (!detail || !detail.widget_id || !detail.version) {
      return;
    }
    if (detail.version !== this.activeVersion) {
      return;
    }
    if (this.pendingShell && this.pendingVersion === detail.version) {
      return;
    }
    if (!this.applyWidgetPayload(root, detail)) {
      return;
    }
    delete this.widgetPayloadBus()[this.payloadBusKey(detail)];
  }

  private readCSRFToken(root: HTMLElement): string {
    const attrToken = root.dataset.dashboardCsrfToken?.trim();
    if (attrToken) {
      return attrToken;
    }

    const input = root.querySelector<HTMLInputElement>("input[name='csrf_token']") ?? document.querySelector<HTMLInputElement>("input[name='csrf_token']");
    return input?.value?.trim() ?? "";
  }

  private observeSurface(): void {
    this.mutationObserver.observe(document.body, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-dashboard-update-version", "data-dashboard-pending-widget-ids"],
    });
  }

  private startLoading(root: HTMLElement, version: string): void {
    this.pendingVersion = version;
    this.pendingShell = true;
    this.pendingShellRevision = this.surfaceRevision + 1;
    this.pendingWidgetIDs = new Set(this.widgetIDs(root));
    root.dataset.dashboardUpdateVersion = version;
    root.dataset.dashboardPendingWidgetIds = Array.from(this.pendingWidgetIDs).join(",");
    this.prepareWidgetsForCurrentVersion(root);
    this.syncWidgetLoadingIndicators(root);
    document.documentElement.setAttribute("data-dashboard-loading", "true");
  }

  private completePendingShellIfReady(): void {
    if (!this.pendingShell) {
      return;
    }

    const root = this.getRoot();
    if (!root) {
      return;
    }
    if (this.surfaceRevision < this.pendingShellRevision) {
      return;
    }

    this.pendingShell = false;
    this.prepareWidgetsForCurrentVersion(root);
    this.syncWidgetLoadingIndicators(root);
    this.drainStoredPayloads(root);
    this.finishLoadingIfReady();
  }

  private finishLoadingIfReady(): void {
    if (this.pendingShell || this.pendingWidgetIDs.size > 0) {
      return;
    }
    this.resetLoading();
  }

  private resetLoading(): void {
    this.pendingVersion = null;
    this.pendingShell = false;
    this.pendingShellRevision = 0;
    this.pendingWidgetIDs.clear();
    const root = this.getRoot();
    if (root) {
      root.dataset.dashboardPendingWidgetIds = "";
      root.dataset.dashboardUpdateVersion = this.activeVersion;
      this.syncWidgetLoadingIndicators(root);
    }
    document.documentElement.setAttribute("data-dashboard-loading", "false");
  }

  private rootVersion(root: HTMLElement): string {
    return root.dataset.dashboardUpdateVersion?.trim() ?? "";
  }

  private prepareWidgetsForCurrentVersion(root: HTMLElement | null): void {
    if (!root) {
      this.pendingWidgetIDs.clear();
      return;
    }
    for (const widgetID of this.pendingWidgetIDs) {
      const widget = root.querySelector<HTMLElement>(`duck-chart[data-widget-id="${widgetID}"], duck-table[data-widget-id="${widgetID}"], duck-metric[data-widget-id="${widgetID}"]`);
      if (!widget) {
        this.pendingWidgetIDs.delete(widgetID);
        continue;
      }
      if (widget.tagName.toLowerCase() === "duck-table") {
        (widget as DuckWidgetElement).setPayload(null);
      }
    }
  }

  private applyWidgetPayload(root: HTMLElement, detail: DashboardWidgetStreamEvent): boolean {
    const widget = root.querySelector<HTMLElement>(`duck-chart[data-widget-id="${detail.widget_id}"], duck-table[data-widget-id="${detail.widget_id}"], duck-metric[data-widget-id="${detail.widget_id}"]`);
    if (!widget) {
      return false;
    }
    (widget as DuckWidgetElement).setPayload(detail.payload ?? null);
    if (this.pendingVersion === detail.version) {
      this.pendingWidgetIDs.delete(detail.widget_id);
      this.syncWidgetLoadingIndicators(root);
      this.finishLoadingIfReady();
    }
    return true;
  }

  private drainStoredPayloads(root: HTMLElement): void {
    const version = this.activeVersion;
    if (!version) {
      return;
    }
    const bus = this.widgetPayloadBus();
    for (const [key, detail] of Object.entries(bus)) {
      if (!detail || detail.version !== version) {
        continue;
      }
      if (!this.applyWidgetPayload(root, detail)) {
        continue;
      }
      delete bus[key];
    }
  }

  private widgetPayloadBus(): DashboardPayloadBus {
    const scopedWindow = window as Window & {
      __dashboardWidgetPayloadBus?: DashboardPayloadBus;
    };
    if (!scopedWindow.__dashboardWidgetPayloadBus) {
      scopedWindow.__dashboardWidgetPayloadBus = {};
    }
    return scopedWindow.__dashboardWidgetPayloadBus;
  }

  private payloadBusKey(detail: DashboardWidgetStreamEvent): string {
    return `${detail.version}:${detail.widget_id}`;
  }

  private widgetIDs(root: HTMLElement): string[] {
    return [...root.querySelectorAll<HTMLElement>("duck-chart[data-widget-id], duck-table[data-widget-id], duck-metric[data-widget-id]")]
      .map((element) => element.dataset.widgetId?.trim() ?? "")
      .filter(Boolean);
  }

  private syncWidgetLoadingIndicators(root: HTMLElement): void {
    const cards = root.querySelectorAll<HTMLElement>("[data-dashboard-widget-card='true'][data-widget-id]");
    for (const card of cards) {
      const widgetID = card.dataset.widgetId?.trim() ?? "";
      if (!widgetID) {
        card.removeAttribute("data-dashboard-widget-loading");
        continue;
      }
      if (this.pendingWidgetIDs.has(widgetID)) {
        card.setAttribute("data-dashboard-widget-loading", "true");
      } else {
        card.removeAttribute("data-dashboard-widget-loading");
      }
    }
  }

  private nextVersion(): string {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  private selectionEventKey(selections: WidgetFilterSelection[]): string {
    return [...selections]
      .map((selection) => `${selection.widgetKey.trim()}|${selection.dimension.trim()}:${selection.value.trim()}`)
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
  }
}

type DashboardWindowState = Window & {
  __dashboardSurfaceController?: DashboardSurfaceController;
};

const dashboardWindow = window as DashboardWindowState;
if (!dashboardWindow.__dashboardSurfaceController) {
  dashboardWindow.__dashboardSurfaceController = new DashboardSurfaceController();
}
