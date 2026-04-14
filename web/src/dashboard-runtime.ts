import type { DashboardWidgetPayload, DashboardWidgetStreamEvent } from "./dashboard-widget-payload";

type DuckWidgetElement = HTMLElement & {
  setPayload: (payload: DashboardWidgetPayload | null) => void;
};

type DashboardPayloadBus = Record<string, DashboardWidgetStreamEvent>;

const widgetSelector = "quack-chart[data-widget-id], quack-table[data-widget-id], quack-metric[data-widget-id]";
const widgetInstanceSelector = (widgetID: string) => `quack-chart[data-widget-id="${widgetID}"], quack-table[data-widget-id="${widgetID}"], quack-metric[data-widget-id="${widgetID}"]`;

export class DashboardSurfaceRuntime {
  private activeVersion = "";
  private pendingVersion: string | null = null;
  private pendingShell = false;
  private pendingShellRevision = 0;
  private pendingWidgetIDs = new Set<string>();
  private surfaceRevision = 0;
  private readonly mutationObserver: MutationObserver;

  constructor(private readonly getRoot: () => HTMLElement | null) {
    this.mutationObserver = new MutationObserver((mutations) => {
      if (mutations.some((mutation) => mutation.target instanceof Node && mutation.target.parentElement?.closest("#dashboard-view-root"))) {
        this.surfaceRevision += 1;
      }
      this.completePendingShellIfReady();
    });
    document.documentElement.setAttribute("data-dashboard-loading", "false");
    this.observeSurface();
    this.primeInitialSurface();
  }

  get currentVersion(): string {
    return this.activeVersion;
  }

  public readCSRFToken(): string {
    const root = this.getRoot();
    const attrToken = root?.dataset.dashboardCsrfToken?.trim();
    if (attrToken) {
      return attrToken;
    }

    const input = root?.querySelector<HTMLInputElement>("input[name='csrf_token']") ?? document.querySelector<HTMLInputElement>("input[name='csrf_token']");
    return input?.value?.trim() ?? "";
  }

  public startLoading(version: string): void {
    const root = this.getRoot();
    if (!root) {
      return;
    }

    this.pendingVersion = version;
    this.pendingShell = true;
    this.pendingShellRevision = this.surfaceRevision + 1;
    this.pendingWidgetIDs = new Set(this.widgetIDs(root));
    this.activeVersion = version;
    root.dataset.dashboardUpdateVersion = version;
    root.dataset.dashboardPendingWidgetIds = Array.from(this.pendingWidgetIDs).join(",");
    this.prepareWidgetsForCurrentVersion(root);
    this.syncWidgetLoadingIndicators(root);
    document.documentElement.setAttribute("data-dashboard-loading", "true");
  }

  public resetLoading(): void {
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

  public handleWidgetPayload(detail: DashboardWidgetStreamEvent): void {
    const root = this.getRoot();
    if (!root || !detail || !detail.widget_id || !detail.version) {
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

  private observeSurface(): void {
    this.mutationObserver.observe(document.body, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-dashboard-update-version", "data-dashboard-pending-widget-ids"],
    });
  }

  private completePendingShellIfReady(): void {
    if (!this.pendingShell) {
      return;
    }

    const root = this.getRoot();
    if (!root || this.surfaceRevision < this.pendingShellRevision) {
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

  private rootVersion(root: HTMLElement): string {
    return root.dataset.dashboardUpdateVersion?.trim() ?? "";
  }

  private prepareWidgetsForCurrentVersion(root: HTMLElement | null): void {
    if (!root) {
      this.pendingWidgetIDs.clear();
      return;
    }
    for (const widgetID of this.pendingWidgetIDs) {
      const widget = root.querySelector<HTMLElement>(widgetInstanceSelector(widgetID));
      if (!widget) {
        this.pendingWidgetIDs.delete(widgetID);
        continue;
      }
      if (widget.tagName.toLowerCase() === "quack-table") {
        (widget as DuckWidgetElement).setPayload(null);
      }
    }
  }

  private applyWidgetPayload(root: HTMLElement, detail: DashboardWidgetStreamEvent): boolean {
    const widget = root.querySelector<HTMLElement>(widgetInstanceSelector(detail.widget_id));
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
    return [...root.querySelectorAll<HTMLElement>(widgetSelector)]
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
}
