export interface ManifestColumn {
  name: string;
  type: string;
}

export interface ManifestBrowserRuntime {
  supported: boolean;
  contract_version: string;
  engine: string;
  adapter: string;
  required_auth_modes: string[];
  supported_file_url_types: string[];
  recommended_max_rows: number;
  recommended_memory_mb: number;
  requires_cors: boolean;
  status: string;
  status_reason: string;
  required_runtime_version: string;
}

export interface ManifestResponse {
  manifest_version: string;
  table: string;
  schema: string;
  columns: ManifestColumn[];
  files: string[];
  row_filters: string[];
  column_masks: Record<string, string>;
  expires_at: string;
  browser_runtime?: ManifestBrowserRuntime;
}

export interface BrowserRuntimeSupport {
  supported: boolean;
  status: "ready" | "planned" | "unsupported" | "error";
  message: string;
  details?: string;
}
