package api

// Authored security and workspace schemas.

schemas_security_workspace: {
  AddWorkspaceMemberRequest: #objectSchema & {
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    },
    #required: [
      "principal_name"
    ]
  },
  CreateGrantRequest: #objectSchema & {
    #fields: {
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      privilege: #stringProperty,
      securable_id: #stringProperty,
      securable_type: #stringProperty
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
      member_id: #stringProperty,
      member_type: #refProperty & {#ref: "PrincipalType"}
    },
    #required: [
      "member_type",
      "member_id"
    ]
  },
  CreateGroupRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  CreateRowFilterRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      filter_sql: #stringProperty,
      name: #nameProperty,
      table_id: #stringProperty
    },
    #required: [
      "name",
      "filter_sql"
    ]
  },
  CreateSavedResourceRequest: #objectSchema & {
    #fields: {
      display_name: #stringProperty,
      resource_key: #stringProperty,
      resource_path: #stringProperty,
      resource_type: #stringProperty,
      section: #stringProperty
    },
    #required: [
      "resource_type",
      "resource_key"
    ]
  },
  CreateTagAssignmentRequest: #objectSchema & {
    #fields: {
      column_name: #stringProperty,
      securable_id: #stringProperty,
      securable_type: #refProperty & {#ref: "TagAssignmentSecurableType"}
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
    #fields: {
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      kind: #refProperty & {#ref: "WorkspaceKind"},
      name: #nameProperty,
      owner_principal: #stringProperty,
      owner_team_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Folder: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      depth: #int32Property,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      parent_folder_id: #stringProperty,
      path: #stringProperty,
      system_role: #stringProperty,
      updated_at: #updatedAtProperty,
      workspace_id: #stringProperty
    }
  },
  FolderContentItem: #objectSchema & {
    #fields: {
      folder_id: #stringProperty,
      git_repo_id: #stringProperty,
      id: #idProperty,
      kind: #stringProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      project_bound: #boolProperty,
      project_name: #stringProperty,
      scope: #stringProperty,
      shared: #boolProperty,
      updated_at: #updatedAtProperty
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
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    }
  },
  FreshnessPolicy: #objectSchema & {
    #fields: {
      cron_schedule: #stringProperty,
      max_lag_seconds: #int64Property
    }
  },
  FreshnessStatus: #objectSchema & {
    #fields: {
      is_fresh: #boolProperty,
      last_run_at: #stringProperty,
      max_lag_seconds: #int64Property,
      stale_since: #stringProperty
    }
  },
  Group: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      id: #idProperty,
      name: #nameProperty
    },
    #required: [
      "id",
      "name"
    ]
  },
  GroupMember: #objectSchema & {
    #fields: {
      group_id: #stringProperty,
      member_id: #stringProperty,
      member_type: #refProperty & {#ref: "PrincipalType"}
    },
    #required: [
      "group_id",
      "member_type",
      "member_id"
    ]
  },
  PrivilegeGrant: #objectSchema & {
    #fields: {
      granted_at: #stringProperty,
      granted_by: #stringProperty,
      id: #idProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      privilege: #stringProperty,
      securable_id: #stringProperty,
      securable_type: #stringProperty
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
      created_at: #createdAtProperty,
      default_branch: #stringProperty,
      description: #descriptionProperty,
      id: #idProperty,
      kind: #refProperty & {#ref: "ProjectKind"},
      name: #nameProperty,
      owner_principal: #stringProperty,
      owner_team_id: #stringProperty,
      product_id: #stringProperty,
      updated_at: #updatedAtProperty,
      workspace_id: #stringProperty
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
      accessed_at: #stringProperty,
      display_name: #stringProperty,
      href: #stringProperty,
      resource_key: #stringProperty,
      resource_path: #stringProperty,
      resource_type: #stringProperty,
      section: #stringProperty
    },
    #required: [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  RowFilter: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      filter_sql: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      table_id: #stringProperty
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
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      row_filter_id: #stringProperty
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
      display_name: #stringProperty,
      href: #stringProperty,
      last_accessed_at: #stringProperty,
      resource_key: #stringProperty,
      resource_path: #stringProperty,
      resource_type: #stringProperty,
      saved_at: #stringProperty,
      section: #stringProperty
    },
    #required: [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  SearchResult: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      match_field: #stringProperty,
      name: #nameProperty,
      schema_name: #stringProperty,
      table_name: #stringProperty,
      type: #stringProperty
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
      last_loaded_at: #stringProperty,
      max_lag_seconds: #int64Property,
      source_schema: #stringProperty,
      source_table: #stringProperty,
      stale_since: #stringProperty,
      timestamp_column: #stringProperty
    }
  },
  Tag: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      id: #idProperty,
      key: #stringProperty,
      value: #stringProperty
    }
  },
  TagAssignment: #objectSchema & {
    #fields: {
      assigned_at: #stringProperty,
      assigned_by: #stringProperty,
      column_name: #stringProperty,
      id: #idProperty,
      securable_id: #stringProperty,
      securable_type: #refProperty & {#ref: "TagAssignmentSecurableType"},
      tag_id: #stringProperty
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
      description: #descriptionProperty,
      filter_sql: #stringProperty,
      name: #nameProperty
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
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      name: #nameProperty
    }
  },
  Workspace: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      id: #idProperty,
      kind: #refProperty & {#ref: "WorkspaceKind"},
      name: #nameProperty,
      owner_principal: #stringProperty,
      owner_team_id: #stringProperty,
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
    #fields: {
      created_at: #createdAtProperty,
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"},
      updated_at: #updatedAtProperty,
      workspace_id: #stringProperty
    },
    #required: [
      "workspace_id",
      "principal_name",
      "role"
    ]
  }
}
