import { RuntimeManifestClient } from "./manifest-client";
import {
  executeLocalQuery,
  LocalQueryCancelledError,
  LocalQueryPreflight,
  LocalQueryUnsupportedError,
  preflightLocalQuery,
  previewLocalQuery,
} from "./query-executor";
import { getDuckDBWasmSupport, resetDuckDBWasmRuntime } from "./duckdb-wasm";
import type { BrowserRuntimeSupport } from "./types";

const COMPUTE_MODE_AUTO = "AUTO";
const COMPUTE_MODE_LOCAL = "BYOC_LOCAL";
const COMPUTE_MODE_SHARED = "SHARED_ENDPOINT";

interface RuntimeElements {
  root: HTMLElement;
  form: HTMLFormElement;
  modeSelect: HTMLSelectElement;
  endpointSelect: HTMLSelectElement;
  textarea: HTMLTextAreaElement;
  runButton: HTMLButtonElement | null;
  asyncButton: HTMLButtonElement | null;
  cancelLocalButton: HTMLButtonElement | null;
  resetLocalButton: HTMLButtonElement | null;
  resultsPanel: HTMLElement | null;
  catalogInput: HTMLInputElement | null;
  schemaInput: HTMLInputElement | null;
  statusTitle: HTMLElement | null;
  statusMessage: HTMLElement | null;
  preflight: HTMLElement | null;
}

interface LocalRunState {
  cancelled: boolean;
}

export function initializeSqlComputeController(root: ParentNode = document): void {
  const runtimeRoot = root.querySelector<HTMLElement>("[data-sql-browser-runtime='true']");
  const form = root.querySelector<HTMLFormElement>("[data-sql-editor-form='true']");
  const modeSelect = root.querySelector<HTMLSelectElement>("#sql-compute-mode");
  const endpointSelect = root.querySelector<HTMLSelectElement>("#sql-compute-endpoint");
  const textarea = root.querySelector<HTMLTextAreaElement>("textarea.sql-editor-textarea");
  if (!(runtimeRoot && form && modeSelect && endpointSelect && textarea)) {
    return;
  }

  const elements: RuntimeElements = {
    root: runtimeRoot,
    form,
    modeSelect,
    endpointSelect,
    textarea,
    runButton: root.querySelector<HTMLButtonElement>("#sql-run-query"),
    asyncButton: root.querySelector<HTMLButtonElement>("#sql-run-query-async"),
    cancelLocalButton: root.querySelector<HTMLButtonElement>("#sql-cancel-local-run"),
    resetLocalButton: root.querySelector<HTMLButtonElement>("#sql-reset-local-runtime"),
    resultsPanel: root.querySelector<HTMLElement>("[data-sql-results-panel='true']"),
    catalogInput: root.querySelector<HTMLInputElement>("input[name='catalog']"),
    schemaInput: root.querySelector<HTMLInputElement>("input[name='schema']"),
    statusTitle: root.querySelector<HTMLElement>("[data-sql-browser-runtime-title='true']"),
    statusMessage: root.querySelector<HTMLElement>("[data-sql-browser-runtime-message='true']"),
    preflight: root.querySelector<HTMLElement>("[data-sql-browser-runtime-preflight='true']"),
  };

  const manifestEndpoint = runtimeRoot.dataset.runtimeManifestEndpoint ?? "";
  const manifestClient = manifestEndpoint ? new RuntimeManifestClient(manifestEndpoint) : null;
  let currentLocalRun: LocalRunState | null = null;
  let allowServerSubmit = false;

  const refresh = async (): Promise<void> => {
    updateEndpointState(elements);
    updateAsyncState(elements, currentLocalRun);
    updateLocalRuntimeButtons(elements, currentLocalRun);

    if (modeSelect.value === COMPUTE_MODE_SHARED) {
      renderSupport(elements, {
        supported: true,
        status: "planned",
        message: "Shared compute selected. Browser-local runtime checks are paused.",
      });
      return;
    }

    await refreshLocalOrAutoPreflight(elements, manifestClient);
  };

  modeSelect.addEventListener("change", () => {
    void refresh();
  });
  endpointSelect.addEventListener("change", () => {
    void refresh();
  });
  textarea.addEventListener("blur", () => {
    void refresh();
  });
  form.addEventListener("submit", (event) => {
    if (allowServerSubmit) {
      allowServerSubmit = false;
      return;
    }
    const submitter = event instanceof SubmitEvent ? event.submitter : null;
    if (!(submitter instanceof HTMLButtonElement)) {
      return;
    }
    if (submitter.id === "sql-run-query-async") {
      return;
    }
    if (modeSelect.value === COMPUTE_MODE_SHARED) {
      return;
    }

    event.preventDefault();
    const localRunControls = {
      getCurrent: () => currentLocalRun,
      setCurrent: (state) => {
        currentLocalRun = state;
      },
      onStateChange: () => {
        updateAsyncState(elements, currentLocalRun);
        updateLocalRuntimeButtons(elements, currentLocalRun);
      },
    };

    if (modeSelect.value === COMPUTE_MODE_LOCAL) {
      void runLocalQuery(elements, manifestClient, localRunControls);
      return;
    }

    void runAutoQuery(elements, manifestClient, submitter, localRunControls, () => {
      allowServerSubmit = true;
    });
  });
  elements.cancelLocalButton?.addEventListener("click", () => {
    void cancelLocalRun(elements, {
      getCurrent: () => currentLocalRun,
      onStateChange: () => {
        updateAsyncState(elements, currentLocalRun);
        updateLocalRuntimeButtons(elements, currentLocalRun);
      },
    });
  });
  elements.resetLocalButton?.addEventListener("click", () => {
    void resetLocalRuntime(elements, {
      getCurrent: () => currentLocalRun,
      onStateChange: () => {
        updateAsyncState(elements, currentLocalRun);
        updateLocalRuntimeButtons(elements, currentLocalRun);
      },
    });
  });

  void refresh();
}

async function refreshLocalOrAutoPreflight(
  elements: RuntimeElements,
  manifestClient: RuntimeManifestClient | null,
): Promise<void> {
  const autoSelected = elements.modeSelect.value === COMPUTE_MODE_AUTO;
  const support = await getDuckDBWasmSupport();
  renderSupport(elements, support);

  if (!support.supported || !manifestClient) {
    renderPreflight(elements, autoSelected ? "AUTO preflight: browser-local execution is unavailable, so the server will resolve shared compute." : "");
    return;
  }

  try {
    const preflight = await preflightLocalQuery(manifestClient, {
      sql: elements.textarea.value,
      catalog: elements.catalogInput?.value.trim(),
      schema: elements.schemaInput?.value.trim(),
    });
    renderPreflight(elements, formatLocalPreflightMessage(preflight, autoSelected));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Local runtime preflight failed.";
    if (autoSelected) {
      renderPreflight(elements, `AUTO preflight: will use managed compute because ${message}`);
      return;
    }
    renderPreflight(elements, `Local runtime preflight: ${message}`);
  }
}

function updateEndpointState(elements: RuntimeElements): void {
  const sharedSelected = elements.modeSelect.value === COMPUTE_MODE_SHARED;
  elements.endpointSelect.disabled = !sharedSelected;
}

function updateAsyncState(elements: RuntimeElements, currentLocalRun: LocalRunState | null): void {
  if (!(elements.asyncButton instanceof HTMLButtonElement)) {
    return;
  }
  const localSelected = elements.modeSelect.value === COMPUTE_MODE_LOCAL;
  const runningLocally = currentLocalRun !== null;
  elements.asyncButton.disabled = localSelected || runningLocally;
  elements.asyncButton.title = localSelected
    ? "Browser-local BYOC is interactive only. Switch to Shared Endpoint or Auto for async runs."
    : runningLocally
      ? "Wait for the current local run to finish or cancel it before starting an async run."
      : "";
}

function updateLocalRuntimeButtons(elements: RuntimeElements, currentLocalRun: LocalRunState | null): void {
  const localSelected = elements.modeSelect.value === COMPUTE_MODE_LOCAL;
  const runningLocally = currentLocalRun !== null;
  if (elements.runButton) {
    elements.runButton.disabled = runningLocally;
    elements.runButton.title = runningLocally ? "A browser-local query is already running." : "";
  }
  if (elements.cancelLocalButton) {
    elements.cancelLocalButton.disabled = !runningLocally;
    elements.cancelLocalButton.title = runningLocally
      ? ""
      : localSelected
        ? "No browser-local query is running."
        : "Switch to Local (BYOC) to run queries in DuckDB WASM.";
  }
  if (elements.resetLocalButton) {
    elements.resetLocalButton.disabled = runningLocally;
    elements.resetLocalButton.title = runningLocally
      ? "Cancel the current local run before resetting the DuckDB WASM runtime."
      : "";
  }
}

function renderSupport(elements: RuntimeElements, support: BrowserRuntimeSupport): void {
  elements.root.dataset.runtimeState = support.status;
  if (elements.statusTitle) {
    elements.statusTitle.textContent = support.supported ? "Browser runtime ready" : "Browser runtime unavailable";
  }
  if (elements.statusMessage) {
    elements.statusMessage.textContent = support.details ? `${support.message} (${support.details})` : support.message;
  }
}

function renderPreflight(elements: RuntimeElements, message: string): void {
  if (elements.preflight) {
    elements.preflight.textContent = message;
  }
}

async function runLocalQuery(
  elements: RuntimeElements,
  manifestClient: RuntimeManifestClient | null,
  localRun: {
    getCurrent(): LocalRunState | null;
    setCurrent(state: LocalRunState | null): void;
    onStateChange(): void;
  },
): Promise<void> {
  if (!manifestClient) {
    renderLocalError(elements, "Local runtime manifest endpoint is not configured.");
    return;
  }
  if (localRun.getCurrent()) {
    renderLocalError(elements, "A browser-local query is already running. Cancel it or wait for it to finish.");
    return;
  }

  const runState: LocalRunState = { cancelled: false };
  localRun.setCurrent(runState);
  localRun.onStateChange();

  renderResultsMessage(elements, "Running locally in DuckDB WASM...");

  try {
    const execution = await executeLocalQuery(manifestClient, {
      sql: elements.textarea.value,
      catalog: elements.catalogInput?.value.trim(),
      schema: elements.schemaInput?.value.trim(),
    });
    if (runState.cancelled) {
      throw new LocalQueryCancelledError("Browser-local execution was cancelled.");
    }
    renderSupport(elements, execution.support);
    const relationNames = execution.manifests.map((manifest) => `${manifest.schema}.${manifest.table}`).join(", ");
    renderPreflight(
      elements,
      `Resolved compute: Local (BYOC) using ${execution.manifests[0]?.browser_runtime?.engine ?? "duckdb-wasm"} for ${relationNames}.`,
    );
    renderResultTable(elements, execution.result.columns, execution.result.rows, execution.result.rowCount);
  } catch (error) {
    if (runState.cancelled || error instanceof LocalQueryCancelledError) {
      renderResultsMessage(elements, "Local DuckDB WASM execution was cancelled. Rerun locally or switch compute mode.");
      renderPreflight(elements, "Local runtime cancelled. You can rerun locally or switch to Shared Endpoint or Auto.");
      return;
    }
    if (error instanceof LocalQueryUnsupportedError) {
      renderLocalError(
        elements,
        `${error.message} Switch compute mode to Shared Endpoint or Auto to run this query on managed compute.`,
      );
      return;
    }
    const message = error instanceof Error ? error.message : "Local query execution failed.";
    renderLocalError(elements, `Local query execution failed: ${message}`);
  } finally {
    localRun.setCurrent(null);
    localRun.onStateChange();
  }
}

async function runAutoQuery(
  elements: RuntimeElements,
  manifestClient: RuntimeManifestClient | null,
  submitter: HTMLButtonElement,
  localRun: {
    getCurrent(): LocalRunState | null;
    setCurrent(state: LocalRunState | null): void;
    onStateChange(): void;
  },
  enableServerSubmit: () => void,
): Promise<void> {
  if (!manifestClient) {
    renderPreflight(elements, "AUTO resolved to managed compute because the browser-local manifest endpoint is unavailable.");
    enableServerSubmit();
    elements.form.requestSubmit(submitter);
    return;
  }

  try {
    const preflight = await preflightLocalQuery(manifestClient, {
      sql: elements.textarea.value,
      catalog: elements.catalogInput?.value.trim(),
      schema: elements.schemaInput?.value.trim(),
    });
    renderPreflight(elements, `AUTO resolved to Local (BYOC). ${formatLocalPreflightMessage(preflight, false)}`);
    await runLocalQuery(elements, manifestClient, localRun);
  } catch (error) {
    const message = error instanceof Error ? error.message : "browser-local execution is unavailable";
    renderPreflight(elements, `AUTO resolved to managed compute because ${message}`);
    enableServerSubmit();
    elements.form.requestSubmit(submitter);
  }
}

async function cancelLocalRun(
  elements: RuntimeElements,
  localRun: {
    getCurrent(): LocalRunState | null;
    onStateChange(): void;
  },
): Promise<void> {
  const runState = localRun.getCurrent();
  if (!runState) {
    return;
  }
  runState.cancelled = true;
  renderResultsMessage(elements, "Cancelling local DuckDB WASM execution...");
  renderPreflight(elements, "Resetting the local browser runtime after cancellation.");
  await resetDuckDBWasmRuntime();
  localRun.onStateChange();
}

async function resetLocalRuntime(
  elements: RuntimeElements,
  localRun: {
    getCurrent(): LocalRunState | null;
    onStateChange(): void;
  },
): Promise<void> {
  if (localRun.getCurrent()) {
    renderLocalError(elements, "Cancel the current browser-local run before resetting the local DuckDB WASM runtime.");
    return;
  }
  renderResultsMessage(elements, "Resetting local DuckDB WASM runtime...");
  try {
    await resetDuckDBWasmRuntime();
    renderResultsMessage(elements, "Local DuckDB WASM runtime reset. Run a query locally or switch compute mode.");
    renderPreflight(elements, "Local runtime reset completed. The next local run will reinitialize DuckDB WASM.");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Local runtime reset failed.";
    renderLocalError(elements, `Local runtime reset failed: ${message}`);
  } finally {
    localRun.onStateChange();
  }
}
function renderLocalError(elements: RuntimeElements, message: string): void {
  renderResultsHTML(
    elements,
    [
      '<div class="rounded-xl border border-[var(--borderColor-danger-muted)] bg-[var(--bgColor-danger-muted)] p-4 shadow-xs">',
      '<h2 class="m-0 text-lg font-semibold text-[var(--fgColor-default)]">Local Query Unavailable</h2>',
      `<pre>${escapeHTML(message)}</pre>`,
      '<p class="m-0 text-sm text-[var(--fgColor-muted)]">Fallback is explicit. Choose Shared Endpoint or Auto and rerun if this browser-local path is unavailable.</p>',
      "</div>",
    ].join(""),
  );
}

function renderResultsMessage(elements: RuntimeElements, message: string): void {
  renderResultsHTML(
    elements,
    [
      '<div class="grid place-items-center rounded-xl border border-[var(--borderColor-default)] border-dashed bg-[var(--bgColor-default)] p-4 shadow-xs">',
      '<div class="flex max-w-sm flex-col items-center justify-center gap-3 py-8 text-center">',
      `<p class="m-0 text-lg font-semibold">${escapeHTML(message)}</p>`,
      "</div>",
      "</div>",
    ].join(""),
  );
}

function formatLocalPreflightMessage(preflight: LocalQueryPreflight, autoSelected: boolean): string {
  const runtime = preflight.manifests[0]?.browser_runtime;
  const relationNames = preflight.preview.relations.map((relation) => `${relation.schema}.${relation.table}`).join(", ");
  const prefix = autoSelected ? "AUTO preflight: eligible for local execution" : "Manifest preflight";
  if (!runtime) {
    return `${prefix}: ${relationNames}`;
  }
  return `${prefix}: ${relationNames} · ${runtime.engine} · LIMIT ${preflight.preview.limit} · guidance ${preflight.guidanceMaxRows} rows`;
}

function renderResultTable(elements: RuntimeElements, columns: string[], rows: unknown[][], rowCount: number): void {
  const header = columns.map((column) => `<th>${escapeHTML(column)}</th>`).join("");
  const body = rows
    .map(
      (row) =>
        `<tr>${row
          .map((value) => `<td>${escapeHTML(formatCell(value))}</td>`)
          .join("")}</tr>`,
    )
    .join("");
  renderResultsHTML(
    elements,
    [
      '<div class="flex h-full min-h-0 flex-col rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs">',
      '<div class="sticky top-0 z-[2] mb-3 flex flex-wrap items-start justify-between gap-3 border-b border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] pb-3">',
      '<div class="flex min-w-0 flex-col gap-1">',
      '<h2 class="m-0 text-lg font-semibold text-[var(--fgColor-default)]">Results (Local DuckDB WASM)</h2>',
      `<p class="m-0 text-sm text-[var(--fgColor-muted)]">${rowCount} row(s)</p>`,
      "</div>",
      "</div>",
      '<div class="min-h-0 flex-1 overflow-auto">',
      `<table class="min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--bgColor-muted)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]"><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`,
      "</div>",
      "</div>",
    ].join(""),
  );
}

function renderResultsHTML(elements: RuntimeElements, html: string): void {
  if (elements.resultsPanel) {
    elements.resultsPanel.innerHTML = html;
  }
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function escapeHTML(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
