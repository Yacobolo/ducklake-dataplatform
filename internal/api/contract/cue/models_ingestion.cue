package api

// Authored ingestion schemas.

schemas_ingestion: {
  CommitIngestionRequest: #objectSchema & {
    #fields: {
      s3_keys: #stringArrayProperty,
      options: #refProperty & {#ref: "IngestionOptions"}
    },
    #required: [
      "s3_keys"
    ]
  },
  IngestionOptions: #objectSchema & {
    #fields: {
      allow_missing_columns: #boolProperty,
      ignore_extra_columns: #boolProperty
    }
  },
  IngestionResult: #objectSchema & {
    #fields: {
      files_registered: #int64Property,
      files_skipped: #int64Property,
      schema: #stringProperty,
      table: #stringProperty
    },
    #required: [
      "files_registered",
      "files_skipped",
      "schema",
      "table"
    ]
  },
  LoadExternalRequest: #objectSchema & {
    #fields: {
      paths: #stringArrayProperty,
      options: #refProperty & {#ref: "IngestionOptions"}
    },
    #required: [
      "paths"
    ]
  },
  UploadUrlRequest: #objectSchema & {
    #fields: {
      filename: #stringProperty
    }
  },
  UploadUrlResponse: #objectSchema & {
    #fields: {
      upload_url: #stringProperty,
      s3_key: #stringProperty,
      expires_at: #expiresAtProperty
    },
    #required: [
      "upload_url",
      "s3_key",
      "expires_at"
    ]
  }
}
