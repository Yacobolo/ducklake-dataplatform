package api

// Authored security and workspace schemas.

schemas_security_workspace: {
  AddWorkspaceMemberRequest: #objectSchema & {
    example: {
      principal_name: "analytics-reviewers"
      role:           "viewer"
    }
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    },
    #required: [
      "principal_name"
    ]
  },
  CreateGrantRequest: #objectSchema & {
    example: {
      principal_id:   "group_analytics_reviewers"
      principal_type: "group"
      securable_type: "catalog"
      securable_id:   "analytics"
      privilege:      "SELECT"
    }
    #fields: {
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      securable_type: #stringProperty,
      securable_id: #stringProperty,
      privilege: #stringProperty
    },
    #required: [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ]
  },
  CreateGroupMemberRequest: #objectSchema & {
    #fields: {
      member_type: #refProperty & {#ref: "PrincipalType"},
      member_id: #stringProperty
    },
    #required: [
      "member_type",
      "member_id"
    ]
  },
  CreateGroupRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty
    },
    #required: [
      "name"
    ]
  },
  CreateRowFilterRequest: #objectSchema & {
    #fields: {
      table_id: #stringProperty,
      name: #nameProperty,
      filter_sql: #stringProperty,
      description: #descriptionProperty,
    },
    #required: [
      "name",
      "filter_sql"
    ]
  },
  CreateSavedResourceRequest: #objectSchema & {
    #fields: {
      resource_type: #stringProperty,
      resource_key: #stringProperty,
      display_name: #stringProperty,
      resource_path: #stringProperty,
      section: #stringProperty
    },
    #required: [
      "resource_type",
      "resource_key"
    ]
  },
  CreateTagAssignmentRequest: #objectSchema & {
    #fields: {
      securable_type: #refProperty & {#ref: "TagAssignmentSecurableType"},
      securable_id: #stringProperty,
      column_name: #stringProperty,
    },
    #required: [
      "securable_type",
      "securable_id"
    ]
  },
  CreateTagRequest: #objectSchema & {
    #fields: {
      key: #stringProperty,
      value: #stringProperty
    },
    #required: [
      "key"
    ]
  },
  CreateWorkspaceRequest: #objectSchema & {
    example: {
      name:                   "Revenue Analytics"
      kind:                   "shared"
      owner_team_id:          "team_analytics"
      owner_principal:        "alice@example.com"
      default_environment_id: "env_prod"
      default_project_id:     "prj_revenue"
      git_repo_id:            "repo_revops"
      git_root_path:          "analytics/revenue"
    }
    #fields: {
      name: #nameProperty,
      kind: #refProperty & {#ref: "WorkspaceKind"},
      owner_team_id: #stringProperty,
      owner_principal: #stringProperty,
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
    },
    #required: [
      "name"
    ]
  },
  Folder: #objectSchema & {
    example: {
      id:                     "fld_01hzyfinance"
      workspace_id:           "ws_01hzyrevenue"
      name:                   "Executive dashboards"
      owner:                  "team-finance"
      parent_folder_id:       "fld_root"
      path:                   "/Revenue Analytics/Executive dashboards"
      depth:                  1
      system_role:            "workspace"
      git_repo_id:            "repo_revops"
      git_root_path:          "dashboards/executive"
      default_project_id:     "prj_revenue"
      default_environment_id: "env_prod"
      created_at:             "2026-04-01T09:00:00Z"
      updated_at:             "2026-04-13T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      workspace_id: #stringProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      parent_folder_id: #stringProperty,
      path: #stringProperty,
      depth: #int32Property,
      system_role: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      default_project_id: #stringProperty,
      default_environment_id: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    }
  },
  FolderContentItem: #objectSchema & {
    #fields: {
      kind: #stringProperty,
      scope: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      folder_id: #stringProperty,
      project_name: #stringProperty,
      updated_at: #updatedAtProperty,
      git_repo_id: #stringProperty,
      shared: #boolProperty,
      project_bound: #boolProperty,
    }
  },
  FolderPath: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "Folder"}
    },
    #required: [
      "data"
    ]
  },
  FolderShare: #objectSchema & {
    example: {
      principal_name: "finance-editors"
      role:           "editor"
    }
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    }
  },
  FolderShareList: {
    type: "array"
    items: {
      ref: "FolderShare"
    }
    example: [{
      principal_name: "finance-editors"
      role:           "editor"
    }]
  },
  FreshnessPolicy: #objectSchema & {
    #fields: {
      max_lag_seconds: #int64Property,
      cron_schedule: #stringProperty
    }
  },
  FreshnessStatus: #objectSchema & {
    #fields: {
      is_fresh: #boolProperty,
      last_run_at: #dateTimeProperty,
      max_lag_seconds: #int64Property,
      stale_since: #dateTimeProperty
    }
  },
  Group: #objectSchema & {
    example: {
      id:          "group_analytics_reviewers"
      name:        "analytics-reviewers"
      description: "Read-only group for revenue analytics stakeholders."
      created_at:  "2026-03-15T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "name"
    ]
  },
  GroupMember: #objectSchema & {
    #fields: {
      group_id: #stringProperty,
      member_type: #refProperty & {#ref: "PrincipalType"},
      member_id: #stringProperty
    },
    #required: [
      "group_id",
      "member_type",
      "member_id"
    ]
  },
  PrivilegeGrant: #objectSchema & {
    example: {
      id:             "grant_01hzycatalogselect"
      principal_id:   "group_analytics_reviewers"
      principal_type: "group"
      securable_type: "catalog"
      securable_id:   "analytics"
      privilege:      "SELECT"
      granted_by:     "alice@example.com"
      granted_at:     "2026-04-13T09:30:00Z"
    }
    #fields: {
      id: #idProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      securable_type: #stringProperty,
      securable_id: #stringProperty,
      privilege: #stringProperty,
      granted_by: #stringProperty,
      granted_at: #dateTimeProperty
    },
    #required: [
      "id",
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ]
  },
  Project: #objectSchema & {
    #fields: {
      id: #idProperty,
      workspace_id: #stringProperty,
      name: #nameProperty,
      kind: #refProperty & {#ref: "ProjectKind"},
      description: #descriptionProperty,
      owner_team_id: #stringProperty,
      owner_principal: #stringProperty,
      product_id: #stringProperty,
      default_branch: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    },
    #required: [
      "workspace_id",
      "name",
      "kind"
    ]
  },
  ProjectKind: #enumSchema & {
    #values: [
      "personal",
      "shared",
      "library"
    ]
  },
  RecentResource: #objectSchema & {
    #fields: {
      resource_type: #stringProperty,
      resource_key: #stringProperty,
      display_name: #stringProperty,
      resource_path: #stringProperty,
      href: #stringProperty,
      section: #stringProperty,
      accessed_at: #dateTimeProperty,
    },
    #required: [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  RowFilter: #objectSchema & {
    #fields: {
      id: #idProperty,
      table_id: #stringProperty,
      name: #nameProperty,
      filter_sql: #stringProperty,
      description: #descriptionProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "table_id",
      "name",
      "filter_sql"
    ]
  },
  RowFilterBinding: #objectSchema & {
    #fields: {
      id: #idProperty,
      row_filter_id: #stringProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"}
    }
  },
  RowFilterBindingRequest: #objectSchema & {
    #fields: {
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"}
    },
    #required: [
      "principal_id",
      "principal_type"
    ]
  },
  SavedResource: #objectSchema & {
    #fields: {
      resource_type: #stringProperty,
      resource_key: #stringProperty,
      display_name: #stringProperty,
      resource_path: #stringProperty,
      href: #stringProperty,
      section: #stringProperty,
      saved_at: #dateTimeProperty,
      last_accessed_at: #dateTimeProperty
    },
    #required: [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  SearchResult: #objectSchema & {
    #fields: {
      type: #stringProperty,
      name: #nameProperty,
      schema_name: #stringProperty,
      table_name: #stringProperty,
      comment: #commentProperty,
      match_field: #stringProperty
    }
  },
  SetDefaultCatalogRequest: #objectSchema,
  ShareFolderRequest: #objectSchema & {
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    },
    #required: [
      "principal_name"
    ]
  },
  SourceFreshnessStatus: #objectSchema & {
    #fields: {
      is_fresh: #boolProperty,
      source_schema: #stringProperty,
      source_table: #stringProperty,
      timestamp_column: #stringProperty,
      last_loaded_at: #dateTimeProperty,
      max_lag_seconds: #int64Property,
      stale_since: #dateTimeProperty,
    }
  },
  Tag: #objectSchema & {
    #fields: {
      id: #idProperty,
      key: #stringProperty,
      value: #stringProperty,
      created_by: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  TagAssignment: #objectSchema & {
    #fields: {
      id: #idProperty,
      tag_id: #stringProperty,
      securable_type: #refProperty & {#ref: "TagAssignmentSecurableType"},
      securable_id: #stringProperty,
      column_name: #stringProperty,
      assigned_by: #stringProperty,
      assigned_at: #dateTimeProperty
    }
  },
  TagAssignmentSecurableType: #enumSchema & {
    #values: [
      "schema",
      "table",
      "column",
      "macro"
    ]
  },
  UpdateGroupRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty
    }
  },
  UpdateRowFilterRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      filter_sql: #stringProperty,
      description: #descriptionProperty
    }
  },
  UpdateTagRequest: #objectSchema & {
    #fields: {
      key: #stringProperty,
      value: #stringProperty
    }
  },
  UpdateWorkspaceRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      default_project_id: #stringProperty,
      default_environment_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty
    }
  },
  Workspace: #objectSchema & {
    example: {
      id:                     "ws_01hzyrevenue"
      name:                   "Revenue Analytics"
      kind:                   "shared"
      owner_team_id:          "team_analytics"
      owner_principal:        "alice@example.com"
      default_project_id:     "prj_revenue"
      default_environment_id: "env_prod"
      git_repo_id:            "repo_revops"
      git_root_path:          "analytics/revenue"
      created_at:             "2026-04-01T09:00:00Z"
      updated_at:             "2026-04-13T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      kind: #refProperty & {#ref: "WorkspaceKind"},
      owner_team_id: #stringProperty,
      owner_principal: #stringProperty,
      default_project_id: #stringProperty,
      default_environment_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "name",
      "kind"
    ]
  },
  WorkspaceKind: #enumSchema & {
    #values: [
      "personal",
      "shared",
      "library"
    ]
  },
  WorkspaceMember: #objectSchema & {
    example: {
      workspace_id:   "ws_01hzyrevenue"
      principal_name: "analytics-reviewers"
      role:           "viewer"
      created_at:     "2026-04-13T09:30:00Z"
      updated_at:     "2026-04-13T09:30:00Z"
    }
    #fields: {
      workspace_id: #stringProperty,
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"},
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    },
    #required: [
      "workspace_id",
      "principal_name",
      "role"
    ]
  },
  WorkspaceMemberList: {
    type: "array"
    items: {
      ref: "WorkspaceMember"
    }
    example: [{
      workspace_id:   "ws_01hzyrevenue"
      principal_name: "analytics-reviewers"
      role:           "viewer"
      created_at:     "2026-04-13T09:30:00Z"
      updated_at:     "2026-04-13T09:30:00Z"
    }]
  }
}
