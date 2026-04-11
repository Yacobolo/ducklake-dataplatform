package api

// Authored ingestion schemas.

schemas_ingestion: {
  "CommitIngestionRequest": {
    "type": "object",
    "properties": {
      "options": {
        "schema": {
          "ref": "IngestionOptions"
        }
      },
      "s3_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "s3_keys"
    ]
  },
  "IngestionOptions": {
    "type": "object",
    "properties": {
      "allow_missing_columns": {
        "schema": {
          "type": "boolean"
        }
      },
      "ignore_extra_columns": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "IngestionResult": {
    "type": "object",
    "properties": {
      "files_registered": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "files_skipped": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "schema": {
        "schema": {
          "type": "string"
        }
      },
      "table": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "files_registered",
      "files_skipped",
      "schema",
      "table"
    ]
  },
  "LoadExternalRequest": {
    "type": "object",
    "properties": {
      "options": {
        "schema": {
          "ref": "IngestionOptions"
        }
      },
      "paths": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "paths"
    ]
  },
  "UploadUrlRequest": {
    "type": "object",
    "properties": {
      "filename": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UploadUrlResponse": {
    "type": "object",
    "properties": {
      "expires_at": {
        "schema": {
          "type": "string"
        }
      },
      "s3_key": {
        "schema": {
          "type": "string"
        }
      },
      "upload_url": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "upload_url",
      "s3_key",
      "expires_at"
    ]
  }
}

