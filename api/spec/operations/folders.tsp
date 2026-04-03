using TypeSpec.Http;

namespace Duck;

@route("/folders")
@get
@tag("Folders")
@summary("List folders")
op listFolders(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedFolders> | StandardErrorResponses;

@route("/folders")
@post
@tag("Folders")
@summary("Create folder")
op createFolder(@body body: CreateFolderRequest): Created<Folder> | MutatingErrorResponses;

@route("/folders/{folder_id}")
@get
@tag("Folders")
@summary("Get folder")
op getFolder(folder_id: string): Ok<Folder> | ResourceErrorResponses;

@route("/folders/{folder_id}")
@patch(#{ implicitOptionality: true })
@tag("Folders")
@summary("Update folder")
op updateFolder(folder_id: string, @body body: UpdateFolderRequest): Ok<Folder> | ResourceErrorResponses;

@route("/folders/{folder_id}/moves")
@post
@tag("Folders")
@summary("Move folder")
op moveFolder(folder_id: string, @body body: MoveFolderRequest): Ok<Folder> | ResourceErrorResponses;

@route("/folders/{folder_id}")
@delete
@tag("Folders")
@summary("Delete folder")
op deleteFolder(folder_id: string): NoContent | ResourceErrorResponses;

@route("/folders/{folder_id}/shares")
@get
@tag("Folders")
@summary("List folder shares")
op listFolderShares(folder_id: string): Ok<FolderShare[]> | ResourceErrorResponses;

@route("/folders/{folder_id}/shares")
@post
@tag("Folders")
@summary("Share folder")
op shareFolder(folder_id: string, @body body: ShareFolderRequest): Ok<FolderShare> | ResourceErrorResponses;

@route("/folders/{folder_id}/shares/{principal_name}")
@delete
@tag("Folders")
@summary("Remove folder share")
op unshareFolder(folder_id: string, principal_name: string): NoContent | ResourceErrorResponses;

@route("/folders/contents")
@get
@tag("Folders")
@summary("List root folder contents")
op listRootFolderContents(
  @query kind?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedFolderContents> | StandardErrorResponses;

@route("/folders/{folder_id}/contents")
@get
@tag("Folders")
@summary("List folder contents")
op listFolderContents(
  folder_id: string,
  @query kind?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedFolderContents> | StandardErrorResponses;
