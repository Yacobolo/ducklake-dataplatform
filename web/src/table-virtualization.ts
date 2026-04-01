import type { DashboardWidgetPayload } from "./dashboard-widget-payload";

export const dashboardTablePageSize = 50;
export const dashboardTableEstimatedRowHeight = 44;
export const dashboardTableOverscan = 8;

export type SparseTableRow = unknown[] | null;

export type SparseTableStore = {
  rows: SparseTableRow[];
  loadedChunkOffsets: Set<number>;
  pendingChunkOffsets: Set<number>;
};

export type VisibleTableWindow = {
  startIndex: number;
  endIndex: number;
  topSpacerHeight: number;
  bottomSpacerHeight: number;
};

export function isDashboardTablePayload(payload: DashboardWidgetPayload | null | undefined): payload is DashboardWidgetPayload {
  return Boolean(payload && Array.isArray(payload.columns) && Array.isArray(payload.rows));
}

export function resolveTableRowCount(
  payload: DashboardWidgetPayload,
  fallback: DashboardWidgetPayload | null | undefined,
  sparseLength: number,
): number {
  const payloadRowCount = Array.isArray(payload.rows) ? payload.rows.length : 0;
  const fallbackRowCount = fallback && Array.isArray(fallback.rows) ? fallback.rows.length : 0;
  return Math.max(payload.row_count ?? 0, fallback?.row_count ?? 0, payloadRowCount, fallbackRowCount, sparseLength);
}

export function createEmptySparseTableStore(): SparseTableStore {
  return {
    rows: [],
    loadedChunkOffsets: new Set<number>(),
    pendingChunkOffsets: new Set<number>(),
  };
}

export function applyPayloadToSparseTableStore(
  store: SparseTableStore,
  payload: DashboardWidgetPayload,
  nextRowCount: number,
): SparseTableStore {
  const offset = payload.page?.offset ?? 0;
  const isReset = !payload.page?.append;
  let rows = isReset ? createSparseRowBuffer(nextRowCount) : resizeSparseRowBuffer(store.rows, nextRowCount);
  const loadedChunkOffsets = isReset ? new Set<number>() : new Set(store.loadedChunkOffsets);
  const pendingChunkOffsets = isReset ? new Set<number>() : new Set(store.pendingChunkOffsets);

  payload.rows.forEach((row, index) => {
    const targetIndex = offset + index;
    if (targetIndex >= 0 && targetIndex < rows.length) {
      rows[targetIndex] = row;
    }
  });

  loadedChunkOffsets.add(offset);
  pendingChunkOffsets.delete(offset);

  return {
    rows,
    loadedChunkOffsets,
    pendingChunkOffsets,
  };
}

export function countLoadedRows(rows: SparseTableRow[]): number {
  let count = 0;
  for (const row of rows) {
    if (row) {
      count += 1;
    }
  }
  return count;
}

export function computeVisibleTableWindow(args: {
  scrollOffset: number;
  viewportHeight: number;
  headerHeight: number;
  totalRows: number;
  rowHeight?: number;
  overscan?: number;
}): VisibleTableWindow {
  const rowHeight = args.rowHeight ?? dashboardTableEstimatedRowHeight;
  const overscan = args.overscan ?? dashboardTableOverscan;
  const usableViewportHeight = Math.max(0, args.viewportHeight - args.headerHeight);
  const visibleCount = Math.max(1, Math.ceil((usableViewportHeight || (rowHeight * 8)) / rowHeight) + (overscan * 2));
  const startIndex = Math.max(0, Math.floor(args.scrollOffset / rowHeight) - overscan);
  const endIndex = Math.min(args.totalRows, startIndex + visibleCount);

  return {
    startIndex,
    endIndex,
    topSpacerHeight: startIndex * rowHeight,
    bottomSpacerHeight: Math.max(0, args.totalRows - endIndex) * rowHeight,
  };
}

export function collectChunkOffsetsForWindow(
  window: VisibleTableWindow,
  totalRows: number,
  loadedChunkOffsets: Set<number>,
  pendingChunkOffsets: Set<number>,
  pageSize = dashboardTablePageSize,
): number[] {
  if (totalRows <= 0) {
    return [];
  }

  const firstChunk = chunkOffsetForIndex(window.startIndex, pageSize);
  const lastChunk = chunkOffsetForIndex(Math.max(0, window.endIndex - 1), pageSize);
  const offsets: number[] = [];

  for (let offset = firstChunk - pageSize; offset <= lastChunk + pageSize; offset += pageSize) {
    if (offset < 0 || offset >= totalRows) {
      continue;
    }
    if (loadedChunkOffsets.has(offset) || pendingChunkOffsets.has(offset)) {
      continue;
    }
    offsets.push(offset);
  }

  return offsets;
}

export function chunkOffsetForIndex(index: number, pageSize = dashboardTablePageSize): number {
  return Math.floor(index / pageSize) * pageSize;
}

function createSparseRowBuffer(length: number): SparseTableRow[] {
  return new Array<SparseTableRow>(length).fill(null);
}

function resizeSparseRowBuffer(previousRows: SparseTableRow[], length: number): SparseTableRow[] {
  if (previousRows.length === length) {
    return [...previousRows];
  }

  const nextRows = createSparseRowBuffer(length);
  const copyLength = Math.min(previousRows.length, length);
  for (let index = 0; index < copyLength; index += 1) {
    nextRows[index] = previousRows[index];
  }
  return nextRows;
}
