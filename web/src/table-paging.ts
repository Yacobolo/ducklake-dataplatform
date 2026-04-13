import type { DashboardWidgetPayload } from "./dashboard-widget-payload";

export type SortDirection = "asc" | "desc";

export type DashboardTablePageRequestDetail = {
  widgetId: string;
  offset: number;
  limit: number;
  append: boolean;
  sortColumn?: string | null;
  sortDirection?: SortDirection | null;
};

export type TableSortState = {
  column: string | null;
  direction: SortDirection;
};

export class TablePageRequestDispatcher {
  private dispatchedPageRequests = new Set<string>();

  public clear(): void {
    this.dispatchedPageRequests.clear();
  }

  public dispatch(host: HTMLElement, detail: DashboardTablePageRequestDetail): void {
    const key = this.requestKey(detail);
    if (this.dispatchedPageRequests.has(key)) {
      return;
    }
    this.dispatchedPageRequests.add(key);
    host.dispatchEvent(new CustomEvent<DashboardTablePageRequestDetail>("dashboard-table-page-request", {
      bubbles: true,
      composed: true,
      detail,
    }));
  }

  private requestKey(detail: DashboardTablePageRequestDetail): string {
    return [
      detail.widgetId,
      String(detail.offset),
      String(detail.limit),
      detail.append ? "append" : "replace",
      detail.sortColumn ?? "",
      detail.sortDirection ?? "",
    ].join("|");
  }
}

export function nextSortState(current: TableSortState, column: string): TableSortState {
  if (current.column !== column) {
    return { column, direction: "asc" };
  }
  if (current.direction === "asc") {
    return { column, direction: "desc" };
  }
  return { column: null, direction: "asc" };
}

export function currentSortPayload(sortState: TableSortState): { column: string; direction: SortDirection } | null {
  if (!sortState.column) {
    return null;
  }
  return {
    column: sortState.column,
    direction: sortState.direction,
  };
}

export function payloadMatchesCurrentSort(payload: DashboardWidgetPayload | null, sortState: TableSortState): boolean {
  const activeSort = currentSortPayload(sortState);
  if (!activeSort) {
    return !payload?.sort;
  }
  return payload?.sort?.column === activeSort.column && payload?.sort?.direction === activeSort.direction;
}

export function shouldReloadForActiveSort(payload: DashboardWidgetPayload | null, sortState: TableSortState): boolean {
  if (!payload) {
    return false;
  }
  return !payloadMatchesCurrentSort(payload, sortState);
}
