export type TableRow = {
  id: string;
  values: Record<string, unknown>;
};

export type VisibleTableRow = {
  index: number;
  row: TableRow | null;
};

export function toNumber(value: unknown): number | null {
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

export function valueIsNumeric(value: unknown): boolean {
  return toNumber(value) !== null;
}

export function columnLooksNumeric(column: string): boolean {
  const lowered = column.toLowerCase();
  return ["count", "total", "sum", "number", "amount", "revenue", "fare", "price", "share"].some((token) => lowered.includes(token));
}

export function columnLooksCurrency(column: string): boolean {
  const lowered = column.toLowerCase();
  return ["revenue", "amount", "gross", "price", "fare", "sales", "cost"].some((token) => lowered.includes(token));
}

export function formatColumnLabel(column: string): string {
  return column.replaceAll("_", " ");
}

export function formatCellValue(column: string, value: unknown): string {
  const numeric = toNumber(value);
  if (numeric === null) {
    return value === null || value === undefined ? "-" : String(value);
  }

  if (columnLooksCurrency(column)) {
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

export function computeColumnWidths(columns: string[], resizedColumnWidths: Record<string, number>): string[] {
  return columns.map((column, index) => {
    const resized = resizedColumnWidths[column];
    if (resized) {
      return `${resized}px`;
    }
    if (columnLooksNumeric(column)) {
      return "11rem";
    }
    if (index === 0) {
      return "16rem";
    }
    return "12rem";
  });
}

export function skeletonWidth(columnIndex: number): string {
  const widths = ["72%", "56%", "48%", "44%"];
  return widths[columnIndex] ?? "58%";
}
