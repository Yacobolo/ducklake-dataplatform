package explore

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ExploreList(http.ResponseWriter, *http.Request)
	ExploreFragment(http.ResponseWriter, *http.Request)
	ExploreUpdatesStream(http.ResponseWriter, *http.Request)
	ExploreUpdatesApply(http.ResponseWriter, *http.Request)
	FoldersList(http.ResponseWriter, *http.Request)
	FoldersNew(http.ResponseWriter, *http.Request)
	FoldersCreate(http.ResponseWriter, *http.Request)
	FoldersEdit(http.ResponseWriter, *http.Request)
	FoldersUpdate(http.ResponseWriter, *http.Request)
	FoldersMove(http.ResponseWriter, *http.Request)
	FoldersDelete(http.ResponseWriter, *http.Request)
	FoldersShare(http.ResponseWriter, *http.Request)
	FoldersUnshare(http.ResponseWriter, *http.Request)
	GitReposList(http.ResponseWriter, *http.Request)
	GitReposNew(http.ResponseWriter, *http.Request)
	GitReposCreate(http.ResponseWriter, *http.Request)
	GitReposDetail(http.ResponseWriter, *http.Request)
	GitReposDelete(http.ResponseWriter, *http.Request)
	GitReposSync(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/explore", h.ExploreList)
	r.Get("/explore/", h.ExploreList)
	r.Get("/explore/fragment", h.ExploreFragment)
	r.Get("/explore/updates/{streamID}", h.ExploreUpdatesStream)
	r.Post("/explore/updates/{streamID}", h.ExploreUpdatesApply)
	r.Get("/explore/folders", h.FoldersList)
	r.Get("/explore/folders/new", h.FoldersNew)
	r.Post("/explore/folders", h.FoldersCreate)
	r.Get("/explore/folders/{folderID}/edit", h.FoldersEdit)
	r.Post("/explore/folders/{folderID}/update", h.FoldersUpdate)
	r.Post("/explore/folders/{folderID}/move", h.FoldersMove)
	r.Post("/explore/folders/{folderID}/delete", h.FoldersDelete)
	r.Post("/explore/folders/{folderID}/share", h.FoldersShare)
	r.Post("/explore/folders/{folderID}/shares/{principalName}/delete", h.FoldersUnshare)
	r.Get("/explore/git-repos", h.GitReposList)
	r.Get("/explore/git-repos/new", h.GitReposNew)
	r.Post("/explore/git-repos", h.GitReposCreate)
	r.Get("/explore/git-repos/{gitRepoID}", h.GitReposDetail)
	r.Post("/explore/git-repos/{gitRepoID}/delete", h.GitReposDelete)
	r.Post("/explore/git-repos/{gitRepoID}/sync", h.GitReposSync)
}
