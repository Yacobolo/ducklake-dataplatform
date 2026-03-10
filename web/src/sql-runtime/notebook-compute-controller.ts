import { RuntimeManifestClient } from "./manifest-client";
import {
  executeLocalQuery,
  LocalQueryCancelledError,
  LocalQueryUnsupportedError,
  preflightLocalQuery,
} from "./query-executor";
import { getDuckDBWasmSupport, resetDuckDBWasmRuntime } from "./duckdb-wasm";
import type { BrowserRuntimeSupport } from "./types";

const COMPUTE_MODE_AUTO = "AUTO";
const COMPUTE_MODE_LOCAL = "BYOC_LOCAL";
const COMPUTE_MODE_SHARED = "SHARED_ENDPOINT";

interface NotebookRuntimeElements {
  root: HTMLElement;
  modeSelect: HTMLSelectElement;
  endpointSelect: HTMLSelectElement;
  resetButton: HTMLButtonElement | null;
  cancelButton: HTMLButtonElement | null;
  runAllButton: HTMLButtonElement | null;
  runAllAsyncButton: HTMLButtonElement | null;
  statusTitle: HTMLElement | null;
  statusMessage: HTMLElement | null;
  preflight: HTMLElement | null;
  notebookCellsRoot: HTMLElement | null;
}

interface LocalRunState {
  cancelled: boolean;
}

export function initializeNotebookComputeController(root: ParentNode = document): void {
  const runtimeRoot = root.querySelector<HTMLElement>("[data-notebook-browser-runtime='true']");
  const modeSelect = root.querySelector<HTMLSelectElement>("#notebook-compute-mode");
  const endpointSelect = root.querySelector<HTMLSelectElement>("#notebook-compute-endpoint");
  if (!(runtimeRoot && modeSelect && endpointSelect)) {
    return;
  }

  const elements: NotebookRuntimeElements = {
    root: runtimeRoot,
    modeSelect,
    endpointSelect,
    resetButton: root.querySelector<HTMLButtonElement>("#notebook-reset-local-runtime"),
    cancelButton: root.querySelector<HTMLButtonElement>("#notebook-cancel-local-run"),
    runAllButton: root.querySelector<HTMLButtonElement>("#notebook-run-all"),
    runAllAsyncButton: root.querySelector<HTMLButtonElement>("#notebook-run-all-async"),
    statusTitle: root.querySelector<HTMLElement>("[data-notebook-browser-runtime-title='true']"),
    statusMessage: root.querySelector<HTMLElement>("[data-notebook-browser-runtime-message='true']"),
    preflight: root.querySelector<HTMLElement>("[data-notebook-browser-runtime-preflight='true']"),
    notebookCellsRoot: root.querySelector<HTMLElement>("[data-notebook-selected-catalog]"),
  };

  const manifestEndpoint = runtimeRoot.dataset.runtimeManifestEndpoint ?? "";
  const manifestClient = manifestEndpoint ? new RuntimeManifestClient(manifestEndpoint) : null;
  let currentLocalRun: LocalRunState | null = null;
  let allowServerSubmit = false;

  const refresh = async (): Promise<void> => {
    syncComputeFields(root, modeSelect.value, endpointSelect.value);
    updateNotebookButtons(elements, currentLocalRun);

    if (modeSelect.value === COMPUTE_MODE_SHARED) {
      renderSupport(elements, {
        supported: true,
        status: "planned",
        message: "Shared compute selected. Browser-local notebook checks are paused.",
      });
      renderPreflight(elements, "");
      return;
    }

    const support = await getDuckDBWasmSupport();
    renderSupport(elements, support);
    if (!support.supported || !manifestClient) {
      renderPreflight(
        elements,
        modeSelect.value === COMPUTE_MODE_AUTO
          ? "AUTO preflight: browser-local notebook execution is unavailable, so managed compute will be used."
          : "",
      );
      return;
    }

    const activeCell = activeNotebookSQLCell(root);
    if (!activeCell) {
      renderPreflight(elements, "Notebook preflight: focus a SQL cell to preview local execution.");
      return;
    }

    const sqlText = cellSQL(activeCell);
    if (!sqlText) {
      renderPreflight(elements, "Notebook preflight: the active SQL cell is empty.");
      return;
    }

    try {
      const preflight = await preflightLocalQuery(manifestClient, {
        sql: sqlText,
        catalog: selectedCatalog(elements),
        schema: selectedSchema(elements),
      });
      renderPreflight(elements, formatNotebookPreflight(preflight, modeSelect.value === COMPUTE_MODE_AUTO));
    } catch (error) {
      const message = error instanceof Error ? error.message : "browser-local notebook preflight failed";
      if (modeSelect.value === COMPUTE_MODE_AUTO) {
        renderPreflight(elements, `AUTO preflight: managed compute will be used because ${message}`);
      } else {
        renderPreflight(elements, `Notebook preflight: ${message}`);
      }
    }
  };

  modeSelect.addEventListener("change", () => void refresh());
  endpointSelect.addEventListener("change", () => void refresh());
  root.addEventListener("focusin", (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && target.closest("[data-notebook-cell='true']")) {
      void refresh();
    }
  });
  root.addEventListener("input", (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && target.matches("[data-cell-editor='true']")) {
      void refresh();
    }
  });

  root.addEventListener("submit", (event) => {
    if (allowServerSubmit) {
      allowServerSubmit = false;
      return;
    }

    const form = event.target;
    if (!(form instanceof HTMLFormElement) || form.dataset.notebookCellForm !== "true") {
      return;
    }

    const submitter = event instanceof SubmitEvent ? event.submitter : null;
    if (!(submitter instanceof HTMLButtonElement) || submitter.dataset.notebookRunCell !== "true") {
      return;
    }

    if (modeSelect.value === COMPUTE_MODE_SHARED) {
      return;
    }

    event.preventDefault();
    const cell = form.closest<HTMLElement>("[data-notebook-cell='true']");
    if (!cell) {
      return;
    }

    const controls = {
      getCurrent: () => currentLocalRun,
      setCurrent: (state: LocalRunState | null) => {
        currentLocalRun = state;
      },
      onStateChange: () => updateNotebookButtons(elements, currentLocalRun),
    };

    if (modeSelect.value === COMPUTE_MODE_LOCAL) {
      void runNotebookLocalCell(elements, manifestClient, cell, controls);
      return;
    }

    void runNotebookAutoCell(elements, manifestClient, form, controls, () => {
      allowServerSubmit = true;
    });
  });

  elements.cancelButton?.addEventListener("click", () => {
    void cancelNotebookLocalRun(elements, {
      getCurrent: () => currentLocalRun,
      onStateChange: () => updateNotebookButtons(elements, currentLocalRun),
    });
  });
  elements.resetButton?.addEventListener("click", () => {
    void resetNotebookLocalRuntime(elements, {
      getCurrent: () => currentLocalRun,
      onStateChange: () => updateNotebookButtons(elements, currentLocalRun),
    });
  });

  void refresh();
}

function syncComputeFields(root: ParentNode, mode: string, endpointName: string): void {
  const forms = root.querySelectorAll<HTMLFormElement>("[data-notebook-cell-form='true'], [data-notebook-run-all-form='true'], [data-notebook-run-all-async-form='true']");
  forms.forEach((form) => {
    const modeInput = form.querySelector<HTMLInputElement>("input[name='compute_mode']");
    const endpointInput = form.querySelector<HTMLInputElement>("input[name='endpoint_name']");
    if (modeInput) {
      modeInput.value = mode;
    }
    if (endpointInput) {
      endpointInput.value = endpointName;
    }
  });
}

function updateNotebookButtons(elements: NotebookRuntimeElements, currentLocalRun: LocalRunState | null): void {
  const localSelected = elements.modeSelect.value === COMPUTE_MODE_LOCAL;
  const runningLocally = currentLocalRun !== null;

  if (elements.cancelButton) {
    elements.cancelButton.disabled = !runningLocally;
  }
  if (elements.resetButton) {
    elements.resetButton.disabled = runningLocally;
  }
  if (elements.runAllButton) {
    elements.runAllButton.disabled = localSelected || runningLocally;
    elements.runAllButton.title = localSelected
      ? "Notebook Run all uses managed compute. Switch to Shared Endpoint or Auto."
      : "";
  }
  if (elements.runAllAsyncButton) {
    elements.runAllAsyncButton.disabled = localSelected || runningLocally;
    elements.runAllAsyncButton.title = localSelected
      ? "Async notebook runs use managed compute. Switch to Shared Endpoint or Auto."
      : "";
  }
}

function renderSupport(elements: NotebookRuntimeElements, support: BrowserRuntimeSupport): void {
  if (elements.statusTitle) {
    elements.statusTitle.textContent = support.supported ? "Browser runtime ready" : "Browser runtime unavailable";
  }
  if (elements.statusMessage) {
    elements.statusMessage.textContent = support.details ? `${support.message} (${support.details})` : support.message;
  }
}

function renderPreflight(elements: NotebookRuntimeElements, message: string): void {
  if (elements.preflight) {
    elements.preflight.textContent = message;
  }
}

function activeNotebookSQLCell(root: ParentNode): HTMLElement | null {
  const focused = document.activeElement;
  if (focused instanceof HTMLElement) {
    const cell = focused.closest<HTMLElement>("[data-notebook-cell='true'][data-cell-type='sql']");
    if (cell) {
      return cell;
    }
  }
  return root.querySelector<HTMLElement>("[data-notebook-cell='true'][data-cell-type='sql']");
}

function cellSQL(cell: HTMLElement): string {
  const editor = cell.querySelector<HTMLTextAreaElement>("textarea.sql-editor-textarea");
  return editor?.value.trim() ?? "";
}

function selectedCatalog(elements: NotebookRuntimeElements): string | undefined {
  const value = elements.notebookCellsRoot?.dataset.notebookSelectedCatalog?.trim();
  return value || undefined;
}

function selectedSchema(elements: NotebookRuntimeElements): string | undefined {
  const value = elements.notebookCellsRoot?.dataset.notebookSelectedSchema?.trim();
  return value || undefined;
}

async function runNotebookLocalCell(
  elements: NotebookRuntimeElements,
  manifestClient: RuntimeManifestClient | null,
  cell: HTMLElement,
  localRun: {
    getCurrent(): LocalRunState | null;
    setCurrent(state: LocalRunState | null): void;
    onStateChange(): void;
  },
): Promise<void> {
  if (!manifestClient) {
    renderNotebookLocalError(cell, "Local runtime manifest endpoint is not configured.");
    return;
  }
  if (localRun.getCurrent()) {
    renderNotebookLocalError(cell, "A browser-local notebook cell is already running.");
    return;
  }

  const runState: LocalRunState = { cancelled: false };
  localRun.setCurrent(runState);
  localRun.onStateChange();
  renderNotebookMessage(cell, "Running locally in DuckDB WASM...");

  try {
    const execution = await executeLocalQuery(manifestClient, {
      sql: cellSQL(cell),
      catalog: selectedCatalog(elements),
      schema: selectedSchema(elements),
    });
    if (runState.cancelled) {
      throw new LocalQueryCancelledError("Browser-local notebook execution was cancelled.");
    }
    const relationNames = execution.manifests.map((manifest) => `${manifest.schema}.${manifest.table}`).join(", ");
    renderPreflight(elements, `Resolved compute: Local (BYOC) for notebook cell using ${relationNames}.`);
    renderNotebookResult(cell, execution.result.columns, execution.result.rows, execution.result.rowCount);
  } catch (error) {
    if (runState.cancelled || error instanceof LocalQueryCancelledError) {
      renderNotebookMessage(cell, "Local notebook execution was cancelled.");
      renderPreflight(elements, "Local notebook runtime cancelled. You can rerun locally or switch compute mode.");
      return;
    }
    if (error instanceof LocalQueryUnsupportedError) {
      renderNotebookLocalError(cell, `${error.message} Switch to Shared Endpoint or Auto to use managed compute.`);
      return;
    }
    const message = error instanceof Error ? error.message : "Local notebook execution failed.";
    renderNotebookLocalError(cell, `Local notebook execution failed: ${message}`);
  } finally {
    localRun.setCurrent(null);
    localRun.onStateChange();
  }
}

async function runNotebookAutoCell(
  elements: NotebookRuntimeElements,
  manifestClient: RuntimeManifestClient | null,
  form: HTMLFormElement,
  localRun: {
    getCurrent(): LocalRunState | null;
    setCurrent(state: LocalRunState | null): void;
    onStateChange(): void;
  },
  enableServerSubmit: () => void,
): Promise<void> {
  const cell = form.closest<HTMLElement>("[data-notebook-cell='true']");
  if (!cell) {
    return;
  }
  if (!manifestClient) {
    enableServerSubmit();
    form.requestSubmit(form.querySelector<HTMLButtonElement>("[data-notebook-run-cell='true']") ?? undefined);
    return;
  }

  try {
    const preflight = await preflightLocalQuery(manifestClient, {
      sql: cellSQL(cell),
      catalog: selectedCatalog(elements),
      schema: selectedSchema(elements),
    });
    renderPreflight(elements, `AUTO resolved to Local (BYOC). ${formatNotebookPreflight(preflight, false)}`);
    await runNotebookLocalCell(elements, manifestClient, cell, localRun);
  } catch (error) {
    const message = error instanceof Error ? error.message : "browser-local notebook execution is unavailable";
    renderPreflight(elements, `AUTO resolved to managed compute for notebook cell because ${message}`);
    enableServerSubmit();
    form.requestSubmit(form.querySelector<HTMLButtonElement>("[data-notebook-run-cell='true']") ?? undefined);
  }
}

async function cancelNotebookLocalRun(
  elements: NotebookRuntimeElements,
  localRun: { getCurrent(): LocalRunState | null; onStateChange(): void },
): Promise<void> {
  const runState = localRun.getCurrent();
  if (!runState) {
    return;
  }
  runState.cancelled = true;
  renderPreflight(elements, "Resetting the local notebook runtime after cancellation.");
  await resetDuckDBWasmRuntime();
  localRun.onStateChange();
}

async function resetNotebookLocalRuntime(
  elements: NotebookRuntimeElements,
  localRun: { getCurrent(): LocalRunState | null; onStateChange(): void },
): Promise<void> {
  if (localRun.getCurrent()) {
    renderPreflight(elements, "Cancel the current browser-local notebook run before resetting the runtime.");
    return;
  }
  await resetDuckDBWasmRuntime();
  renderPreflight(elements, "Local notebook runtime reset completed. The next local SQL cell run will reinitialize DuckDB WASM.");
  localRun.onStateChange();
}

function renderNotebookResult(cell: HTMLElement, columns: string[], rows: unknown[][], rowCount: number): void {
  const target = cell.querySelector<HTMLElement>("[data-notebook-cell-output='true']");
  if (!target) {
    return;
  }
  const header = columns.map((column) => `<th>${escapeHTML(column)}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${row.map((value) => `<td>${escapeHTML(formatCell(value))}</td>`).join("")}</tr>`)
    .join("");
  target.innerHTML = [
    '<div class="d-flex flex-justify-between flex-wrap flex-items-center gap-2">',
    "<h4>Output</h4>",
    "</div>",
    `<p class="color-fg-muted text-small">${rowCount} row(s) · Local DuckDB WASM</p>`,
    `<table class="data-table"><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`,
  ].join("");
}

function renderNotebookLocalError(cell: HTMLElement, message: string): void {
  const target = cell.querySelector<HTMLElement>("[data-notebook-cell-output='true']");
  if (!target) {
    return;
  }
  target.innerHTML = [
    '<div class="flash flash-error">',
    "<h4>Local Query Unavailable</h4>",
    `<pre>${escapeHTML(message)}</pre>`,
    "</div>",
  ].join("");
}

function renderNotebookMessage(cell: HTMLElement, message: string): void {
  const target = cell.querySelector<HTMLElement>("[data-notebook-cell-output='true']");
  if (!target) {
    return;
  }
  target.innerHTML = `<p class="color-fg-muted text-small">${escapeHTML(message)}</p>`;
}

function formatNotebookPreflight(
  preflight: Awaited<ReturnType<typeof preflightLocalQuery>>,
  autoSelected: boolean,
): string {
  const runtime = preflight.manifests[0]?.browser_runtime;
  const relationNames = preflight.preview.relations.map((relation) => `${relation.schema}.${relation.table}`).join(", ");
  const prefix = autoSelected ? "AUTO preflight: eligible for local notebook execution" : "Notebook preflight";
  if (!runtime) {
    return `${prefix}: ${relationNames}`;
  }
  return `${prefix}: ${relationNames} · ${runtime.engine} · LIMIT ${preflight.preview.limit} · guidance ${preflight.guidanceMaxRows} rows`;
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
