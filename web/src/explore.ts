type URLParamValue = string | string[];
type URLParamsShape = Record<string, URLParamValue>;

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const trimmed = item.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function normalizeURLParams(value: unknown): URLParamsShape {
  const record = typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
  const out: URLParamsShape = {};
  for (const [key, raw] of Object.entries(record)) {
    if (Array.isArray(raw)) {
      out[key] = normalizeStringArray(raw);
      continue;
    }
    if (typeof raw === "string") {
      out[key] = raw.trim();
      continue;
    }
    out[key] = "";
  }
  return out;
}

function toQueryString(value: unknown): string {
  const params = normalizeURLParams(value);
  const search = new URLSearchParams();
  for (const [key, raw] of Object.entries(params)) {
    if (Array.isArray(raw)) {
      for (const item of raw) {
        search.append(key, item);
      }
      continue;
    }
    if (raw) {
      search.set(key, raw);
    }
  }
  return search.toString();
}

function toURL(path: string, value: unknown): string {
  const query = toQueryString(value);
  return query ? `${path}?${query}` : path;
}

function toggleArrayValue(value: unknown, key: string, item: string, checked: boolean): URLParamsShape {
  const next = normalizeURLParams(value);
  const current = Array.isArray(next[key]) ? [...(next[key] as string[])] : [];
  const trimmed = item.trim();
  if (!trimmed) {
    return next;
  }
  next[key] = checked
    ? Array.from(new Set([...current, trimmed]))
    : current.filter((candidate) => candidate !== trimmed);
  return next;
}

function clear(value: unknown, keys?: string[]): URLParamsShape {
  const next = normalizeURLParams(value);
  const targetKeys = Array.isArray(keys) && keys.length > 0 ? keys : Object.keys(next);
  for (const key of targetKeys) {
    next[key] = Array.isArray(next[key]) ? [] : "";
  }
  return next;
}

function fromLocation(fallback: unknown): URLParamsShape {
  const base = normalizeURLParams(fallback);
  const url = new URL(window.location.href);
  const next: URLParamsShape = {};
  for (const [key, raw] of Object.entries(base)) {
    if (Array.isArray(raw)) {
      next[key] = normalizeStringArray(url.searchParams.getAll(key));
      continue;
    }
    next[key] = url.searchParams.get(key)?.trim() ?? raw;
  }
  return next;
}

declare global {
  interface Window {
    DuckUIURLParams?: {
      clear: typeof clear;
      fromLocation: typeof fromLocation;
      normalize: typeof normalizeURLParams;
      toQueryString: typeof toQueryString;
      toURL: typeof toURL;
      toggleArrayValue: typeof toggleArrayValue;
    };
  }
}

window.DuckUIURLParams = {
  clear,
  fromLocation,
  normalize: normalizeURLParams,
  toQueryString,
  toURL,
  toggleArrayValue,
};
