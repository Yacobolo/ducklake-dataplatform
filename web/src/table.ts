import { LitElement, css, html } from "lit";
import { styleMap } from "lit/directives/style-map.js";

import type { DashboardWidgetPayload } from "./dashboard-widget-payload";
import {
  applyPayloadToSparseTableStore,
  collectChunkOffsetsForWindow,
  computeVisibleTableWindow,
  countLoadedRows,
  createEmptySparseTableStore,
  dashboardTableEstimatedRowHeight,
  dashboardTableOverscan,
  dashboardTablePageSize,
  isDashboardTablePayload,
  resolveTableRowCount,
} from "./table-virtualization";

type SortDirection = "asc" | "desc";

type TableRow = {
  id: string;
  values: Record<string, unknown>;
};

type VisibleTableRow = {
  index: number;
  row: TableRow | null;
};

type ActiveResize = {
  column: string;
  pointerId: number;
  startX: number;
  startWidth: number;
};

class DuckTable extends LitElement {
  static properties = {
    widgetId: { attribute: "data-widget-id" },
    payloadJSON: { attribute: "data-table-payload" },
  };

  static styles = css`
    :host {
      display: block;
      height: 100%;
      max-height: 100%;
      min-height: 0;
      color: var(--fgColor-default);
    }

    .empty {
      display: grid;
      place-items: center;
      height: 100%;
      min-height: 14rem;
      padding: 1rem;
      color: var(--fgColor-muted);
      text-align: center;
    }

    .shell {
      height: 100%;
      max-height: 100%;
      min-height: 0;
    }

    .scroller {
      height: 100%;
      max-height: 100%;
      min-height: 0;
      overflow: auto;
      scrollbar-gutter: stable;
      overscroll-behavior: contain;
      overflow-anchor: none;
    }

    .header-row,
    .body-row {
      display: grid;
      grid-template-columns: var(--dashboard-table-columns);
      width: max-content;
      min-width: 100%;
    }

    .header-row {
      position: sticky;
      top: 0;
      z-index: 1;
      background: color-mix(in srgb, var(--bgColor-muted) 86%, var(--bgColor-default) 14%);
      border-bottom: 1px solid color-mix(in srgb, var(--borderColor-default) 80%, transparent);
    }

    .header-cell-shell {
      position: relative;
      min-width: 0;
    }

    .header-cell {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      min-width: 0;
      width: 100%;
      padding: 0.85rem 1.5rem 0.85rem 1.25rem;
      border: 0;
      background: transparent;
      color: var(--fgColor-muted);
      font-size: 0.68rem;
      font-weight: 800;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      text-align: left;
      cursor: pointer;
      transition: color 150ms ease, background-color 150ms ease;
    }

    .header-cell:hover:not(:disabled) {
      color: var(--fgColor-default);
      background: color-mix(in srgb, var(--bgColor-accent-muted) 20%, transparent);
    }

    .header-cell:disabled {
      cursor: default;
      opacity: 0.72;
    }

    .header-cell--numeric {
      justify-content: flex-end;
      text-align: right;
    }

    .header-label {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .sort-indicator {
      display: inline-flex;
      width: 0.8rem;
      justify-content: center;
      color: var(--fgColor-muted);
      flex: 0 0 auto;
    }

    .sort-indicator[data-active="true"] {
      color: var(--fgColor-accent);
    }

    .resize-handle {
      position: absolute;
      top: 0;
      right: -0.4rem;
      z-index: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      width: 0.8rem;
      height: 100%;
      cursor: col-resize;
      touch-action: none;
    }

    .resize-handle::before {
      content: "";
      width: 0.15rem;
      height: 1.35rem;
      border-radius: 999px;
      background: color-mix(in srgb, var(--borderColor-default) 84%, transparent);
      transition: background-color 150ms ease;
    }

    .header-cell-shell:hover .resize-handle::before,
    .resize-handle[data-active="true"]::before {
      background: color-mix(in srgb, var(--borderColor-accent-emphasis) 78%, transparent);
    }

    .table {
      width: max-content;
      min-width: 100%;
      overflow-anchor: none;
    }

    .body-row {
      height: 2.75rem;
      border-bottom: 1px solid color-mix(in srgb, var(--borderColor-default) 62%, transparent);
      transition: background-color 150ms ease;
    }

    .body-row:hover:not(.body-row--skeleton) {
      background: color-mix(in srgb, var(--bgColor-accent-muted) 24%, transparent);
    }

    .cell {
      display: flex;
      align-items: center;
      height: 100%;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      padding: 0 1.25rem;
      font-size: 0.88rem;
      line-height: 1.35;
      color: var(--fgColor-default);
    }

    .cell--primary {
      font-weight: 600;
    }

    .cell--numeric {
      justify-content: flex-end;
      text-align: right;
      font-variant-numeric: tabular-nums;
      color: color-mix(in srgb, var(--fgColor-default) 82%, var(--fgColor-muted) 18%);
    }

    .skeleton-block {
      display: block;
      width: 100%;
      height: 0.8rem;
      border-radius: 999px;
      background:
        linear-gradient(
          90deg,
          color-mix(in srgb, var(--bgColor-muted) 92%, transparent) 0%,
          color-mix(in srgb, var(--bgColor-default) 65%, var(--bgColor-muted) 35%) 50%,
          color-mix(in srgb, var(--bgColor-muted) 92%, transparent) 100%
        );
      background-size: 200% 100%;
      animation: dashboard-table-skeleton 1.6s ease-in-out infinite;
    }

    .status {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 3rem;
      width: 100%;
      padding: 0.75rem 1rem 1rem;
      color: var(--fgColor-muted);
      font-size: 0.78rem;
      font-weight: 600;
    }

    .unloaded-spacer {
      width: 100%;
      min-width: 100%;
      pointer-events: none;
      overflow-anchor: none;
    }

    @keyframes dashboard-table-skeleton {
      0% {
        background-position: 200% 0;
      }
      100% {
        background-position: -200% 0;
      }
    }
  `;

  payloadJSON = "";
  widgetId = "";
  private sortColumn: string | null = null;
  private sortDirection: SortDirection = "asc";
  private activeResize: ActiveResize | null = null;
  private resizedColumnWidths: Record<string, number> = {};
  private payloadState: DashboardWidgetPayload | null = null;
  private sparseRows = createEmptySparseTableStore().rows;
  private loadedChunkOffsets = createEmptySparseTableStore().loadedChunkOffsets;
  private pendingChunkOffsets = createEmptySparseTableStore().pendingChunkOffsets;
  private scrollOffset = 0;
  private viewportHeight = 0;
  private resizeObserver: ResizeObserver | null = null;

  disconnectedCallback(): void {
    this.stopResize();
    this.resizeObserver?.disconnect();
    super.disconnectedCallback();
  }

  firstUpdated(): void {
    const scroller = this.shadowRoot?.querySelector<HTMLElement>(".scroller");
    if (!scroller || typeof ResizeObserver === "undefined") {
      this.viewportHeight = scroller?.clientHeight ?? this.viewportHeight;
      this.ensureVisibleChunks(this.payloadState ?? this.parsePayload());
      return;
    }

    this.resizeObserver = new ResizeObserver(() => {
      this.viewportHeight = scroller.clientHeight;
      this.ensureVisibleChunks(this.payloadState ?? this.parsePayload());
      this.requestUpdate();
    });
    this.resizeObserver.observe(scroller);
    this.viewportHeight = scroller.clientHeight;
    this.ensureVisibleChunks(this.payloadState ?? this.parsePayload());
  }

  setPayload(payload: DashboardWidgetPayload | null): void {
    if (!payload) {
      this.payloadState = null;
      this.payloadJSON = "";
      const emptyStore = createEmptySparseTableStore();
      this.sparseRows = emptyStore.rows;
      this.loadedChunkOffsets = emptyStore.loadedChunkOffsets;
      this.pendingChunkOffsets = emptyStore.pendingChunkOffsets;
      this.requestUpdate();
      return;
    }

    const current = this.payloadState ?? this.parsePayload();
    const nextRowCount = this.totalRowCount(payload, current);
    const nextStore = applyPayloadToSparseTableStore(
      {
        rows: this.sparseRows,
        loadedChunkOffsets: this.loadedChunkOffsets,
        pendingChunkOffsets: this.pendingChunkOffsets,
      },
      payload,
      nextRowCount,
    );
    this.sparseRows = nextStore.rows;
    this.loadedChunkOffsets = nextStore.loadedChunkOffsets;
    this.pendingChunkOffsets = nextStore.pendingChunkOffsets;
    this.payloadState = {
      ...(current ?? payload),
      ...payload,
      row_count: nextRowCount,
    };
    this.payloadJSON = JSON.stringify(this.payloadState);
    this.ensureVisibleChunks(this.payloadState);
    this.requestUpdate();
  }

  render() {
    const payload = this.payloadState ?? this.parsePayload();
    const totalRows = payload ? this.totalRowCount(payload) : 0;
    if (!payload) {
      return html`<div class="empty">Loading table...</div>`;
    }

    if (!payload.visual || payload.visual.kind !== "table" || payload.columns.length === 0 || totalRows === 0) {
      return html`<div class="empty">No table data available.</div>`;
    }

    const template = this.columnWidths(payload.columns).join(" ");
    const visible = this.visibleRows(payload);

    return html`
      <div class="shell" style=${styleMap({ "--dashboard-table-columns": template })}>
        <div class="scroller" @scroll=${this.handleScroll}>
          <div class="table">
            <div class="header-row">
              ${payload.columns.map((column) => this.renderHeaderCell(column))}
            </div>
            ${visible.topSpacerHeight > 0
              ? html`<div class="unloaded-spacer" style=${styleMap({ height: `${visible.topSpacerHeight}px` })}></div>`
              : null}
            ${visible.rows.map((item) => this.renderVisibleRow(payload.columns, item))}
            ${this.renderStatus(payload)}
            ${visible.bottomSpacerHeight > 0
              ? html`<div class="unloaded-spacer" style=${styleMap({ height: `${visible.bottomSpacerHeight}px` })}></div>`
              : null}
          </div>
        </div>
      </div>
    `;
  }

  private renderStatus(payload: DashboardWidgetPayload) {
    const totalRows = this.totalRowCount(payload);
    const loadedRows = this.loadedRowCount();
    if (this.pendingChunkOffsets.size === 0 && loadedRows >= totalRows) {
      return null;
    }
    return html`
      <div class="status">
        ${this.pendingChunkOffsets.size > 0 ? "Loading rows in view..." : "Scroll to load more rows"}
      </div>
    `;
  }

  private renderHeaderCell(column: string) {
    const sortable = this.canSort();
    const numeric = this.columnLooksNumeric(column);
    const active = this.sortColumn === column;
    const direction = active ? this.sortDirection : null;
    const indicator = direction === "asc" ? "↑" : direction === "desc" ? "↓" : "↕";

    return html`
      <div class="header-cell-shell">
        <button
          class=${`header-cell${numeric ? " header-cell--numeric" : ""}`}
          type="button"
          ?disabled=${!sortable}
          @click=${() => this.toggleSort(column)}
          title=${sortable ? `Sort by ${this.formatColumnLabel(column)}` : "Sorting is available after all rows load"}
        >
          <span class="header-label">${this.formatColumnLabel(column)}</span>
          <span class="sort-indicator" data-active=${active ? "true" : "false"}>${indicator}</span>
        </button>
        <div
          class="resize-handle"
          data-active=${this.activeResize?.column === column ? "true" : "false"}
          title=${`Resize ${this.formatColumnLabel(column)} column`}
          @dblclick=${(event: MouseEvent) => this.resetColumnWidth(column, event)}
          @pointerdown=${(event: PointerEvent) => this.beginResize(column, event)}
        ></div>
      </div>
    `;
  }

  private renderVisibleRow(columns: string[], item: VisibleTableRow) {
    if (!item.row) {
      return this.renderSkeletonRow(columns, item.index);
    }
    return this.renderBodyRow(columns, item.row, item.index);
  }

  private renderSkeletonRow(columns: string[], index: number) {
    return html`
      <div class="body-row body-row--skeleton" data-row-index=${index}>
        ${columns.map((column, columnIndex) => html`
          <div
            class=${[
              "cell",
              columnIndex === 0 ? "cell--primary" : "",
              this.columnLooksNumeric(column) ? "cell--numeric" : "",
            ].filter(Boolean).join(" ")}
          >
            <span class="skeleton-block" style=${styleMap({ width: this.skeletonWidth(columnIndex) })}></span>
          </div>
        `)}
      </div>
    `;
  }

  private renderBodyRow(columns: string[], row: TableRow, index: number) {
    return html`
      <div class="body-row" data-row-index=${index}>
        ${columns.map((column, columnIndex) => {
          const value = row.values[column];
          const numeric = this.valueIsNumeric(value) || this.columnLooksNumeric(column);
          return html`
            <div
              class=${[
                "cell",
                columnIndex === 0 ? "cell--primary" : "",
                numeric ? "cell--numeric" : "",
              ].filter(Boolean).join(" ")}
              title=${this.formatCellValue(column, value)}
            >
              ${this.formatCellValue(column, value)}
            </div>
          `;
        })}
      </div>
    `;
  }

  private parsePayload(): DashboardWidgetPayload | null {
    try {
      const payload = JSON.parse(this.payloadJSON || "null") as DashboardWidgetPayload | null;
      if (!isDashboardTablePayload(payload)) {
        return null;
      }
      return payload;
    } catch {
      return null;
    }
  }

  private totalRowCount(payload: DashboardWidgetPayload, fallback?: DashboardWidgetPayload | null): number {
    return resolveTableRowCount(payload, fallback, this.sparseRows.length);
  }

  private loadedRowCount(): number {
    return countLoadedRows(this.sparseRows);
  }

  private canSort(): boolean {
    return this.sparseRows.length > 0 && this.loadedRowCount() === this.sparseRows.length;
  }

  private sortedRows(payload: DashboardWidgetPayload): TableRow[] {
    const rows = this.sparseRows
      .map((row, rowIndex) => {
        if (!row) {
          return null;
        }
        const values: Record<string, unknown> = {};
        payload.columns.forEach((column, columnIndex) => {
          values[column] = row[columnIndex];
        });
        return { id: `${rowIndex}`, values };
      })
      .filter((row): row is TableRow => row !== null);

    if (!this.sortColumn || !this.canSort()) {
      return rows;
    }

    const column = this.sortColumn;
    const direction = this.sortDirection === "asc" ? 1 : -1;
    return [...rows].sort((left, right) => {
      const comparison = this.compareValues(left.values[column], right.values[column]);
      if (comparison !== 0) {
        return comparison * direction;
      }
      return Number(left.id) - Number(right.id);
    });
  }

  private toggleSort(column: string): void {
    if (!this.canSort()) {
      return;
    }

    if (this.sortColumn !== column) {
      this.sortColumn = column;
      this.sortDirection = "asc";
      this.requestUpdate();
      return;
    }

    if (this.sortDirection === "asc") {
      this.sortDirection = "desc";
      this.requestUpdate();
      return;
    }

    this.sortColumn = null;
    this.sortDirection = "asc";
    this.requestUpdate();
  }

  private compareValues(left: unknown, right: unknown): number {
    const leftNumber = this.toNumber(left);
    const rightNumber = this.toNumber(right);
    if (leftNumber !== null && rightNumber !== null) {
      if (leftNumber < rightNumber) {
        return -1;
      }
      if (leftNumber > rightNumber) {
        return 1;
      }
      return 0;
    }

    const leftText = this.normalizeString(left);
    const rightText = this.normalizeString(right);
    return leftText.localeCompare(rightText, undefined, { numeric: true, sensitivity: "base" });
  }

  private normalizeString(value: unknown): string {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value);
  }

  private toNumber(value: unknown): number | null {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value !== "string") {
      return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const numeric = Number(trimmed);
    return Number.isFinite(numeric) ? numeric : null;
  }

  private valueIsNumeric(value: unknown): boolean {
    return this.toNumber(value) !== null;
  }

  private columnLooksNumeric(column: string): boolean {
    const lowered = column.toLowerCase();
    return ["count", "total", "sum", "number", "amount", "revenue", "fare", "price", "share"].some((token) => lowered.includes(token));
  }

  private columnLooksCurrency(column: string): boolean {
    const lowered = column.toLowerCase();
    return ["revenue", "amount", "gross", "price", "fare", "sales", "cost"].some((token) => lowered.includes(token));
  }

  private formatColumnLabel(column: string): string {
    return column.replaceAll("_", " ");
  }

  private formatCellValue(column: string, value: unknown): string {
    const numeric = this.toNumber(value);
    if (numeric === null) {
      return value === null || value === undefined ? "-" : String(value);
    }

    if (this.columnLooksCurrency(column)) {
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 2,
      }).format(numeric);
    }

    if (Math.abs(numeric - Math.round(numeric)) < 0.000001) {
      return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(numeric);
    }

    return new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(numeric);
  }

  private columnWidths(columns: string[]): string[] {
    return columns.map((column, index) => {
      const resized = this.resizedColumnWidths[column];
      if (resized) {
        return `${resized}px`;
      }
      if (this.columnLooksNumeric(column)) {
        return "11rem";
      }
      if (index === 0) {
        return "16rem";
      }
      return "12rem";
    });
  }

  private skeletonWidth(columnIndex: number): string {
    const widths = ["72%", "56%", "48%", "44%"];
    return widths[columnIndex] ?? "58%";
  }

  private readonly handleScroll = (event: Event): void => {
    const target = event.currentTarget as HTMLElement | null;
    if (!target) {
      return;
    }

    this.scrollOffset = target.scrollTop;
    this.viewportHeight = target.clientHeight;
    this.ensureVisibleChunks(this.payloadState ?? this.parsePayload());
    this.requestUpdate();
  };

  private ensureVisibleChunks(payload: DashboardWidgetPayload | null): void {
    if (!payload) {
      return;
    }

    const totalRows = this.totalRowCount(payload);
    if (totalRows === 0) {
      return;
    }

    const visible = this.visibleRows(payload);
    const offsets = collectChunkOffsetsForWindow(
      {
        startIndex: visible.startIndex,
        endIndex: visible.endIndex,
        topSpacerHeight: visible.topSpacerHeight,
        bottomSpacerHeight: visible.bottomSpacerHeight,
      },
      totalRows,
      this.loadedChunkOffsets,
      this.pendingChunkOffsets,
      dashboardTablePageSize,
    );
    for (const offset of offsets) {
      this.pendingChunkOffsets.add(offset);
      this.dispatchEvent(new CustomEvent("dashboard-table-page-request", {
        bubbles: true,
        composed: true,
        detail: {
          widgetId: this.widgetId,
          offset,
          limit: dashboardTablePageSize,
        },
      }));
    }
  }

  private visibleRows(payload: DashboardWidgetPayload): {
    startIndex: number;
    endIndex: number;
    rows: VisibleTableRow[];
    topSpacerHeight: number;
    bottomSpacerHeight: number;
  } {
    const totalRows = this.totalRowCount(payload);
    const headerHeight = this.shadowRoot?.querySelector<HTMLElement>(".header-row")?.getBoundingClientRect().height ?? 0;
    const window = computeVisibleTableWindow({
      scrollOffset: this.scrollOffset,
      viewportHeight: this.viewportHeight,
      headerHeight,
      totalRows,
      rowHeight: dashboardTableEstimatedRowHeight,
      overscan: dashboardTableOverscan,
    });

    const rows: VisibleTableRow[] = [];
    if (this.canSort() && this.sortColumn) {
      const sortedRows = this.sortedRows(payload);
      const slice = sortedRows.slice(window.startIndex, window.endIndex);
      slice.forEach((row, index) => {
        rows.push({ index: window.startIndex + index, row });
      });
    } else {
      for (let index = window.startIndex; index < window.endIndex; index += 1) {
        const slot = this.sparseRows[index] ?? null;
        if (!slot) {
          rows.push({ index, row: null });
          continue;
        }
        const values: Record<string, unknown> = {};
        payload.columns.forEach((column, columnIndex) => {
          values[column] = slot[columnIndex];
        });
        rows.push({
          index,
          row: { id: `${index}`, values },
        });
      }
    }

    return {
      startIndex: window.startIndex,
      endIndex: window.endIndex,
      rows,
      topSpacerHeight: window.topSpacerHeight,
      bottomSpacerHeight: window.bottomSpacerHeight,
    };
  }

  private beginResize(column: string, event: PointerEvent): void {
    if (event.button !== 0 && event.pointerType !== "touch" && event.pointerType !== "pen") {
      return;
    }

    const shell = (event.currentTarget as HTMLElement | null)?.closest(".header-cell-shell") as HTMLElement | null;
    if (!shell) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    (event.currentTarget as HTMLElement | null)?.setPointerCapture?.(event.pointerId);

    this.activeResize = {
      column,
      pointerId: event.pointerId,
      startX: event.clientX,
      startWidth: shell.getBoundingClientRect().width,
    };

    this.toggleAttribute("data-resizing", true);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    window.addEventListener("pointermove", this.handleResizePointerMove);
    window.addEventListener("pointerup", this.handleResizePointerUp);
    window.addEventListener("pointercancel", this.handleResizePointerUp);
  }

  private resetColumnWidth(column: string, event: MouseEvent): void {
    event.preventDefault();
    event.stopPropagation();
    if (!(column in this.resizedColumnWidths)) {
      return;
    }

    delete this.resizedColumnWidths[column];
    this.resizedColumnWidths = { ...this.resizedColumnWidths };
    this.requestUpdate();
  }

  private readonly handleResizePointerMove = (event: PointerEvent): void => {
    if (!this.activeResize || event.pointerId !== this.activeResize.pointerId) {
      return;
    }

    const minWidth = 128;
    const nextWidth = Math.max(minWidth, this.activeResize.startWidth + (event.clientX - this.activeResize.startX));
    this.resizedColumnWidths = {
      ...this.resizedColumnWidths,
      [this.activeResize.column]: nextWidth,
    };
    this.requestUpdate();
  };

  private readonly handleResizePointerUp = (event: PointerEvent): void => {
    if (!this.activeResize || event.pointerId !== this.activeResize.pointerId) {
      return;
    }

    this.stopResize();
  };

  private stopResize(): void {
    this.activeResize = null;
    this.toggleAttribute("data-resizing", false);
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
    window.removeEventListener("pointermove", this.handleResizePointerMove);
    window.removeEventListener("pointerup", this.handleResizePointerUp);
    window.removeEventListener("pointercancel", this.handleResizePointerUp);
    this.requestUpdate();
  }
}

if (!customElements.get("duck-table")) {
  customElements.define("duck-table", DuckTable);
}
