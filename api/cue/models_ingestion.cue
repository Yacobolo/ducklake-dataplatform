package api

// Authored ingestion schemas.

schemas_ingestion: {
  "CommitIngestionRequest": #objectSchema & {
    #fields: {
      "options": #refProperty & {#ref: "IngestionOptions"},
      "s3_keys": #stringArrayProperty
    },
    #required: [
      "s3_keys"
    ]
  },
  "IngestionOptions": #objectSchema & {
    #fields: {
      "allow_missing_columns": #boolProperty,
      "ignore_extra_columns": #boolProperty
    }
  },
  "IngestionResult": #objectSchema & {
    #fields: {
      "files_registered": #int64Property,
      "files_skipped": #int64Property,
      "schema": #stringProperty,
      "table": #stringProperty
    },
    #required: [
      "files_registered",
      "files_skipped",
      "schema",
      "table"
    ]
  },
  "LoadExternalRequest": #objectSchema & {
    #fields: {
      "options": #refProperty & {#ref: "IngestionOptions"},
      "paths": #stringArrayProperty
    },
    #required: [
      "paths"
    ]
  },
  "UploadUrlRequest": #objectSchema & {
    #fields: {
      "filename": #stringProperty
    }
  },
  "UploadUrlResponse": #objectSchema & {
    #fields: {
      "expires_at": #expiresAtProperty,
      "s3_key": #stringProperty,
      "upload_url": #stringProperty
    },
    #required: [
      "upload_url",
      "s3_key",
      "expires_at"
    ]
  }
}

