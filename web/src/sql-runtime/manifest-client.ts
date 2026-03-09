import type { ManifestResponse } from "./types";

export interface RuntimeManifestRequest {
  catalog?: string;
  schema?: string;
  table: string;
}

export class RuntimeManifestClient {
  constructor(private readonly endpoint: string) {}

  async fetchManifest(request: RuntimeManifestRequest): Promise<ManifestResponse> {
    const url = new URL(this.endpoint, window.location.origin);
    if (request.catalog) {
      url.searchParams.set("catalog", request.catalog);
    }
    if (request.schema) {
      url.searchParams.set("schema", request.schema);
    }
    url.searchParams.set("table", request.table);

    const response = await fetch(url.toString(), {
      method: "GET",
      credentials: "same-origin",
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(await parseErrorMessage(response));
    }

    return (await response.json()) as ManifestResponse;
  }
}

async function parseErrorMessage(response: Response): Promise<string> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const payload = (await response.json()) as { error?: string };
    if (payload.error) {
      return payload.error;
    }
  }

  const message = await response.text();
  return message || `Manifest request failed with HTTP ${response.status}`;
}
