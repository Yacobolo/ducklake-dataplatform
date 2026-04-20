package api

// Authored ingestion schemas.

schemas_ingestion: {
  CommitIngestionRequest: #objectSchema & {
    example: {
      s3_keys: [
        "landing/orders/2026/04/13/orders_0001.parquet",
        "landing/orders/2026/04/13/orders_0002.parquet",
      ]
      options: {
        allow_missing_columns: false
        ignore_extra_columns:  true
      }
    }
    #fields: {
      s3_keys: #stringArrayProperty,
      options: #refProperty & {#ref: "IngestionOptions"}
    },
    #required: [
      "s3_keys"
    ]
  },
  IngestionOptions: #objectSchema & {
    example: {
      allow_missing_columns: false
      ignore_extra_columns:  true
    }
    #fields: {
      allow_missing_columns: #boolProperty,
      ignore_extra_columns: #boolProperty
    }
  },
  IngestionResult: #objectSchema & {
    example: {
      files_registered: 2
      files_skipped:    0
      schema:           "landing"
      table:            "orders_raw"
    }
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
    example: {
      paths: [
        "s3://github.com/Yacobolo/quackstack/raw/orders/2026/04/13/*.parquet"
      ]
      options: {
        allow_missing_columns: false
        ignore_extra_columns:  true
      }
    }
    #fields: {
      paths: #stringArrayProperty,
      options: #refProperty & {#ref: "IngestionOptions"}
    },
    #required: [
      "paths"
    ]
  },
  UploadUrlRequest: #objectSchema & {
    example: {
      filename: "orders_2026_04_13.parquet"
    }
    #fields: {
      filename: #stringProperty
    }
  },
  UploadUrlResponse: #objectSchema & {
    example: {
      upload_url: "https://uploads.example.com/presigned/orders_2026_04_13.parquet"
      s3_key:     "landing/orders/2026/04/13/orders_2026_04_13.parquet"
      expires_at: "2026-04-13T10:30:00Z"
    }
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
