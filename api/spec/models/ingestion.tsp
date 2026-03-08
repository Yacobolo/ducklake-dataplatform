namespace Duck;

model UploadUrlRequest {
  filename?: string;
}

model UploadUrlResponse {
  upload_url: string;
  s3_key: string;
  expires_at: utcDateTime;
}

model IngestionOptions {
  allow_missing_columns?: boolean;
  ignore_extra_columns?: boolean;
}

model CommitIngestionRequest {
  s3_keys: string[];
  options?: IngestionOptions;
}

model LoadExternalRequest {
  paths: string[];
  options?: IngestionOptions;
}

model IngestionResult {
  files_registered: int64;
  files_skipped: int64;
  schema: string;
  table: string;
}
