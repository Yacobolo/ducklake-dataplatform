using TypeSpec.Http;

namespace Duck;

@route("/grants")
@get
@tag("Governance")
@summary("List grants")
op listGrants(
  @query principal_id?: string,
  @query principal_type?: PrincipalType,
  @query securable_type?: string,
  @query securable_id?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedGrants> | GuardedReadErrorResponses;

@route("/grants")
@post
@tag("Governance")
@summary("Create grant")
op createGrant(@body body: CreateGrantRequest): Created<PrivilegeGrant> | ValidationResourceErrorResponses;

@route("/grants/{grant_id}")
@delete
@tag("Governance")
@summary("Delete grant")
op deleteGrant(grant_id: string): NoContent | MutatingErrorResponses;

@route("/row-filters")
@get
@tag("Governance")
@summary("List row filters")
op listRowFilters(@query table_id?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedRowFilters> | ResourceErrorResponses;

@route("/row-filters")
@post
@tag("Governance")
@summary("Create row filter")
op createRowFilter(@body body: CreateRowFilterRequest): Created<RowFilter> | MutatingErrorResponses;

@route("/row-filters/{row_filter_id}")
@get
@tag("Governance")
@summary("Get row filter")
op getRowFilter(row_filter_id: string): Ok<RowFilter> | ResourceErrorResponses;

@route("/row-filters/{row_filter_id}")
@patch(#{ implicitOptionality: true })
@tag("Governance")
@summary("Update row filter")
op updateRowFilter(row_filter_id: string, @body body: UpdateRowFilterRequest): Ok<RowFilter> | MutatingErrorResponses;

@route("/row-filters/{row_filter_id}")
@delete
@tag("Governance")
@summary("Delete row filter")
op deleteRowFilter(row_filter_id: string): NoContent | MutatingErrorResponses;

@route("/row-filters/{row_filter_id}/bindings")
@get
@tag("Governance")
@summary("List row filter bindings")
op listRowFilterBindings(
  row_filter_id: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedRowFilterBindings> | ResourceErrorResponses;

@route("/row-filters/{row_filter_id}/bindings")
@post
@tag("Governance")
@summary("Bind row filter")
op bindRowFilter(row_filter_id: string, @body body: RowFilterBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/row-filters/{row_filter_id}/bindings/{principal_type}/{principal_id}")
@delete
@tag("Governance")
@summary("Unbind row filter")
op unbindRowFilter(row_filter_id: string, principal_type: PrincipalType, principal_id: string): NoContent | MutatingErrorResponses;

@route("/column-masks")
@get
@tag("Governance")
@summary("List column masks")
op listColumnMasks(@query table_id?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnMasks> | ResourceErrorResponses;

@route("/column-masks")
@post
@tag("Governance")
@summary("Create column mask")
op createColumnMask(@body body: CreateColumnMaskRequest): Created<ColumnMask> | MutatingErrorResponses;

@route("/column-masks/{column_mask_id}")
@get
@tag("Governance")
@summary("Get column mask")
op getColumnMask(column_mask_id: string): Ok<ColumnMask> | ResourceErrorResponses;

@route("/column-masks/{column_mask_id}")
@patch(#{ implicitOptionality: true })
@tag("Governance")
@summary("Update column mask")
op updateColumnMask(column_mask_id: string, @body body: UpdateColumnMaskRequest): Ok<ColumnMask> | MutatingErrorResponses;

@route("/column-masks/{column_mask_id}")
@delete
@tag("Governance")
@summary("Delete column mask")
op deleteColumnMask(column_mask_id: string): NoContent | MutatingErrorResponses;

@route("/column-masks/{column_mask_id}/bindings")
@get
@tag("Governance")
@summary("List column mask bindings")
op listColumnMaskBindings(
  column_mask_id: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnMaskBindings> | ResourceErrorResponses;

@route("/column-masks/{column_mask_id}/bindings")
@post
@tag("Governance")
@summary("Bind column mask")
op bindColumnMask(column_mask_id: string, @body body: ColumnMaskBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/column-masks/{column_mask_id}/bindings/{principal_type}/{principal_id}")
@delete
@tag("Governance")
@summary("Unbind column mask")
op unbindColumnMask(column_mask_id: string, principal_type: PrincipalType, principal_id: string): NoContent | MutatingErrorResponses;

@route("/tags")
@get
@tag("Governance")
@summary("List tags")
op listTags(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

@route("/tags")
@post
@tag("Governance")
@summary("Create tag")
op createTag(@body body: CreateTagRequest): Created<Tag> | MutatingErrorResponses;

@route("/tags/{tag_id}")
@get
@tag("Governance")
@summary("Get tag")
op getTag(tag_id: string): Ok<Tag> | ResourceErrorResponses;

@route("/tags/{tag_id}")
@patch(#{ implicitOptionality: true })
@tag("Governance")
@summary("Update tag")
op updateTag(tag_id: string, @body body: UpdateTagRequest): Ok<Tag> | MutatingErrorResponses;

@route("/tags/{tag_id}")
@delete
@tag("Governance")
@summary("Delete tag")
op deleteTag(tag_id: string): NoContent | MutatingErrorResponses;

@route("/tags/{tag_id}/assignments")
@get
@tag("Governance")
@summary("List tag assignments")
op listTagAssignments(
  tag_id: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedTagAssignments> | ResourceErrorResponses;

@route("/tags/{tag_id}/assignments")
@post
@tag("Governance")
@summary("Create tag assignment")
op createTagAssignment(tag_id: string, @body body: CreateTagAssignmentRequest): Created<TagAssignment> | ValidationResourceErrorResponses;

@route("/tags/{tag_id}/assignments/{assignment_id}")
@delete
@tag("Governance")
@summary("Delete tag assignment")
op deleteTagAssignment(tag_id: string, assignment_id: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
@tag("Governance")
@summary("List classifications")
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;
