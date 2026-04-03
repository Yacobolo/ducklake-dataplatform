import type { DashboardWidgetStreamEvent } from "./dashboard-widget-payload";
import {
  buildDashboardViewURL,
  nextDashboardVersion,
  readOriginFiltersFromURL,
  removeOriginFilter,
  selectionEventKey,
  serializeOriginFilters,
  toggleOriginFilter,
  type DashboardFilterEventDetail,
  type DashboardTablePageEventDetail,
} from "./dashboard-state";
import { DashboardSurfaceRuntime } from "./dashboard-runtime";

export class DashboardSurfaceController {
  private lastFilterEventKey = "";
  private lastFilterEventAt = 0;
  private readonly runtime = new DashboardSurfaceRuntime(() => this.getRoot());

  constructor() {
    this.bindEvents();
  }

  private bindEvents(): void {
    document.addEventListener("dashboard-filter-select", (event) => {
      void this.handleWidgetFilter(event as CustomEvent<DashboardFilterEventDetail>);
    });
    document.addEventListener("dashboard-table-page-request", (event) => {
      void this.handleTablePageRequest(event as CustomEvent<DashboardTablePageEventDetail>);
    });
    window.addEventListener("dashboard-widget-payload", (event) => {
      this.runtime.handleWidgetPayload((event as CustomEvent<DashboardWidgetStreamEvent>).detail);
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

  private getRoot(): HTMLElement | null {
    return document.querySelector<HTMLElement>("#dashboard-view-root[data-dashboard-surface='true']");
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
        csrfToken: this.runtime.readCSRFToken(),
        originFilters: serializeOriginFilters(readOriginFiltersFromURL(window.location.href, window.location.origin)),
        version: this.runtime.currentVersion,
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
    const eventKey = selectionEventKey(event.detail.selections);
    const now = Date.now();
    if (eventKey && this.lastFilterEventKey === eventKey && now - this.lastFilterEventAt < 400) {
      return;
    }
    this.lastFilterEventKey = eventKey;
    this.lastFilterEventAt = now;

    const nextFilters = readOriginFiltersFromURL(window.location.href, window.location.origin);
    for (const selection of event.detail.selections) {
      toggleOriginFilter(nextFilters, selection.widgetKey, selection.dimension, selection.value);
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

    const nextFilters = readOriginFiltersFromURL(window.location.href, window.location.origin);
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

    removeOriginFilter(nextFilters, dimension, value);
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

    const filters = readOriginFiltersFromURL(window.location.href, window.location.origin);
    await this.applyFilters(root, filters, pushState);
  }

  private async applyFilters(root: HTMLElement, filters: ReturnType<typeof readOriginFiltersFromURL>, pushState: boolean): Promise<void> {
    const applyURL = root.dataset.dashboardApplyUrl;
    if (!applyURL) {
      return;
    }

    const viewURL = buildDashboardViewURL(window.location.href, window.location.origin, root.dataset.dashboardViewUrl, filters);
    if (pushState) {
      window.history.pushState({}, "", viewURL);
    }

    const nextVersion = nextDashboardVersion();
    this.runtime.startLoading(nextVersion);

    const response = await fetch(applyURL, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        csrfToken: this.runtime.readCSRFToken(),
        originFilters: serializeOriginFilters(filters),
        version: nextVersion,
      }),
    });
    if (!response.ok) {
      this.runtime.resetLoading();
    }
  }
}

type DashboardWindowState = Window & {
  __dashboardSurfaceController?: DashboardSurfaceController;
};

export function ensureDashboardSurfaceController(): void {
  const dashboardWindow = window as DashboardWindowState;
  if (!dashboardWindow.__dashboardSurfaceController) {
    dashboardWindow.__dashboardSurfaceController = new DashboardSurfaceController();
  }
}
