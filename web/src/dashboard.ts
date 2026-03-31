import "./chart";

type ChartFilterSelection = {
  dimension: string;
  value: string;
};

type DashboardChartFilterEventDetail = {
  selections: ChartFilterSelection[];
};

class DashboardSurfaceController {
  private requestToken = 0;

  constructor() {
    this.bindEvents();
  }

  private bindEvents(): void {
    document.addEventListener("dashboard-chart-filter", (event) => {
      void this.handleChartFilter(event as CustomEvent<DashboardChartFilterEventDetail>);
    });

    document.addEventListener("click", (event) => {
      void this.handleClick(event);
    });

    window.addEventListener("popstate", () => {
      void this.refreshFromLocation();
    });
  }

  private async handleChartFilter(event: CustomEvent<DashboardChartFilterEventDetail>): Promise<void> {
    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    const nextFilters = this.readFiltersFromURL(window.location.href);
    for (const selection of event.detail.selections) {
      this.toggleFilter(nextFilters, selection.dimension, selection.value);
    }

    await this.navigate(surface, nextFilters, true);
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

    const nextFilters = this.readFiltersFromURL(window.location.href);
    if (target.hasAttribute("data-dashboard-clear-filters")) {
      nextFilters.clear();
      await this.navigate(surface, nextFilters, true);
      return;
    }

    const dimension = target.dataset.dashboardFilterDimension;
    const value = target.dataset.dashboardFilterValue;
    if (!dimension || !value) {
      return;
    }

    this.toggleFilter(nextFilters, dimension, value);
    await this.navigate(surface, nextFilters, true);
  }

  private async refreshFromLocation(): Promise<void> {
    const surface = this.getSurface();
    if (!surface) {
      return;
    }

    const filters = this.readFiltersFromURL(window.location.href);
    await this.navigate(surface, filters, false);
  }

  private getSurface(): HTMLElement | null {
    return document.querySelector<HTMLElement>("#dashboard-view-surface[data-dashboard-surface='true']");
  }

  private readFiltersFromURL(rawURL: string): Map<string, Set<string>> {
    const url = new URL(rawURL, window.location.origin);
    const filters = new Map<string, Set<string>>();

    for (const rawFilter of url.searchParams.getAll("f")) {
      const separatorIndex = rawFilter.indexOf(":");
      if (separatorIndex < 1 || separatorIndex === rawFilter.length - 1) {
        continue;
      }

      const dimension = rawFilter.slice(0, separatorIndex).trim();
      const value = rawFilter.slice(separatorIndex + 1).trim();
      if (!dimension || !value) {
        continue;
      }

      const current = filters.get(dimension) ?? new Set<string>();
      current.add(value);
      filters.set(dimension, current);
    }

    return filters;
  }

  private toggleFilter(filters: Map<string, Set<string>>, dimension: string, value: string): void {
    const current = filters.get(dimension) ?? new Set<string>();
    if (current.has(value)) {
      current.delete(value);
    } else {
      current.add(value);
    }

    if (current.size === 0) {
      filters.delete(dimension);
      return;
    }

    filters.set(dimension, current);
  }

  private buildViewURL(surface: HTMLElement, filters: Map<string, Set<string>>): URL {
    const nextURL = new URL(window.location.href);
    const viewPath = surface.dataset.dashboardViewUrl;
    if (viewPath) {
      nextURL.pathname = viewPath;
    }
    nextURL.searchParams.delete("f");
    for (const [dimension, values] of filters.entries()) {
      for (const value of values.values()) {
        nextURL.searchParams.append("f", `${dimension}:${value}`);
      }
    }
    return nextURL;
  }

  private buildSurfaceURL(surface: HTMLElement, viewURL: URL): URL {
    const surfaceURL = new URL(viewURL.toString());
    const surfacePath = surface.dataset.dashboardSurfaceUrl;
    if (surfacePath) {
      surfaceURL.pathname = surfacePath;
    }
    return surfaceURL;
  }

  private async navigate(surface: HTMLElement, filters: Map<string, Set<string>>, pushState: boolean): Promise<void> {
    const viewURL = this.buildViewURL(surface, filters);
    const surfaceURL = this.buildSurfaceURL(surface, viewURL);
    const requestToken = ++this.requestToken;

    if (pushState) {
      window.history.pushState({}, "", viewURL);
    }

    const response = await fetch(surfaceURL.toString(), {
      headers: {
        "X-Requested-With": "fetch",
      },
    });
    if (!response.ok) {
      return;
    }

    const html = await response.text();
    if (requestToken !== this.requestToken) {
      return;
    }

    const currentSurface = this.getSurface();
    if (!currentSurface) {
      return;
    }
    currentSurface.outerHTML = html;
  }
}

new DashboardSurfaceController();
