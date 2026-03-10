import * as duckdb from "@duckdb/duckdb-wasm";

import type { BrowserRuntimeSupport } from "./types";

const RUNTIME_DB_PATH = ":memory:";

let runtimePromise: Promise<DuckDBWasmRuntime> | null = null;

export class DuckDBWasmRuntime {
  private constructor(
    private readonly db: duckdb.AsyncDuckDB,
    private readonly connection: duckdb.AsyncDuckDBConnection,
    readonly support: BrowserRuntimeSupport,
  ) {}

  static async create(): Promise<DuckDBWasmRuntime> {
    if (typeof window === "undefined" || typeof Worker === "undefined") {
      throw new Error("This environment does not support browser workers.");
    }

    const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
    if (!bundle.mainModule || !bundle.mainWorker) {
      throw new Error("DuckDB WASM assets are not available for this browser.");
    }

    const features = await duckdb.getPlatformFeatures();
    const logger = new duckdb.ConsoleLogger();
    const worker = await duckdb.createWorker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(logger, worker);

    try {
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      await db.open({ path: RUNTIME_DB_PATH });
      const connection = await db.connect();

      return new DuckDBWasmRuntime(db, connection, {
        supported: true,
        status: "ready",
        message: "DuckDB WASM is initialized in this browser.",
        details: [
          `exceptions=${String(features.wasmExceptions)}`,
          `simd=${String(features.wasmSIMD)}`,
          `threads=${String(features.wasmThreads)}`,
          `coi=${String(features.crossOriginIsolated)}`,
        ].join(", "),
      });
    } catch (error) {
      await worker.terminate();
      throw error;
    }
  }

  async registerFileURL(path: string, sourceURL: string): Promise<void> {
    await this.db.registerFileURL(path, sourceURL, duckdb.DuckDBDataProtocol.HTTP, true);
  }

  async resetSession(): Promise<void> {
    await this.connection.query("RESET schema;");
  }

  async exec(sqlText: string): Promise<void> {
    await this.connection.query(sqlText);
  }

  async query(sqlText: string): Promise<duckdb.ArrowTable> {
    return this.connection.query(sqlText);
  }

  async close(): Promise<void> {
    await this.connection.close();
    await this.db.terminate();
  }
}

export async function getDuckDBWasmRuntime(): Promise<DuckDBWasmRuntime> {
  if (!runtimePromise) {
    runtimePromise = DuckDBWasmRuntime.create().catch((error) => {
      runtimePromise = null;
      throw error;
    });
  }
  return runtimePromise;
}

export async function getDuckDBWasmSupport(): Promise<BrowserRuntimeSupport> {
  try {
    const runtime = await getDuckDBWasmRuntime();
    return runtime.support;
  } catch (error) {
    if (error instanceof Error) {
      const message = error.message || "DuckDB WASM initialization failed.";
      const unsupported = /not support browser workers|not available for this browser/i.test(message);
      return {
        supported: false,
        status: unsupported ? "unsupported" : "error",
        message,
      };
    }
    return {
      supported: false,
      status: "error",
      message: "DuckDB WASM initialization failed.",
    };
  }
}

export async function resetDuckDBWasmRuntime(): Promise<void> {
  if (!runtimePromise) {
    return;
  }
  const runtime = await runtimePromise.catch(() => null);
  runtimePromise = null;
  if (runtime) {
    await runtime.close();
  }
}
