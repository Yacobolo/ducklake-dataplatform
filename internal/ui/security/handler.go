package security

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	"github.com/go-chi/chi/v5"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) SecurityHome(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/security/principals", http.StatusSeeOther)
}

func (h *Handler) SecurityPrincipalsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Principal.List(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]securityPrincipalRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, securityPrincipalRowData{
			Filter:    item.Name + " " + item.Type,
			ID:        item.ID,
			Name:      item.Name,
			Type:      item.Type,
			IsAdmin:   item.IsAdmin,
			CreatedAt: formatTime(item.CreatedAt),
			DetailURL: "/ui/security/principals/" + item.ID,
		})
	}
	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "security/principals",
		DisplayName:  "Security",
		Section:      "Operate",
	})
	core.RenderHTML(w, http.StatusOK, securityPrincipalsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) SecurityPrincipalsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, securityPrincipalFormPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) SecurityPrincipalsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.deps.Principal.Create(r.Context(), domain.CreatePrincipalRequest{
		Name:    formString(r.Form, "name"),
		Type:    formString(r.Form, "type"),
		IsAdmin: formBool(r.Form, "is_admin"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) SecurityPrincipalsDetail(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	item, err := h.deps.Principal.GetByID(r.Context(), principalID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	grants, _, err := h.deps.Grant.ListForPrincipal(r.Context(), item.ID, item.Type, domain.PageRequest{MaxResults: 50})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	keys, _, err := h.deps.APIKey.List(r.Context(), &item.ID, domain.PageRequest{MaxResults: 50})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	core.RenderHTML(w, http.StatusOK, securityPrincipalDetailPage(securityPrincipalDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Item:              item,
		Grants:            grants,
		APIKeys:           keys,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) SecurityPrincipalsDelete(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	if err := h.deps.Principal.Delete(r.Context(), principalID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals", http.StatusSeeOther)
}

func (h *Handler) SecurityPrincipalsSetAdmin(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	if _, err := h.deps.Principal.Update(r.Context(), principalID, domain.UpdatePrincipalRequest{
		IsAdmin: boolFormPtr(r.Form, "is_admin"),
	}); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals/"+principalID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Group.List(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]securityGroupRowData, 0, len(items))
	for i := range items {
		item := items[i]
		memberCount := "0"
		members, totalMembers, membersErr := h.deps.Group.ListMembers(r.Context(), item.ID, domain.PageRequest{MaxResults: 1})
		if membersErr == nil {
			if totalMembers > 0 {
				memberCount = formatCount(totalMembers)
			} else if len(members) > 0 {
				memberCount = formatCount(int64(len(members)))
			}
		}
		rows = append(rows, securityGroupRowData{
			Filter:    item.Name + " " + item.Description,
			ID:        item.ID,
			Name:      item.Name,
			Members:   memberCount,
			CreatedAt: formatTime(item.CreatedAt),
			DetailURL: "/ui/security/groups/" + item.ID,
		})
	}
	core.RenderHTML(w, http.StatusOK, securityGroupsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) SecurityGroupsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, securityGroupFormPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) SecurityGroupsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.deps.Group.Create(r.Context(), domain.CreateGroupRequest{
		Name:        formString(r.Form, "name"),
		Description: formString(r.Form, "description"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsDetail(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	item, err := h.deps.Group.GetByID(r.Context(), groupID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	members, _, err := h.deps.Group.ListMembers(r.Context(), groupID, domain.PageRequest{MaxResults: 100})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	memberRows := make([]securityGroupMemberRowData, 0, len(members))
	for i := range members {
		member := members[i]
		memberRows = append(memberRows, securityGroupMemberRowData{
			GroupID:    groupID,
			MemberID:   member.MemberID,
			MemberType: member.MemberType,
			CSRFField:  h.deps.CSRFFieldProvider(r),
		})
	}

	grants, _, err := h.deps.Grant.ListForPrincipal(r.Context(), groupID, "group", domain.PageRequest{MaxResults: 50})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	core.RenderHTML(w, http.StatusOK, securityGroupDetailPage(securityGroupDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Item:              item,
		Members:           memberRows,
		Grants:            grants,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) SecurityGroupsDelete(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if err := h.deps.Group.Delete(r.Context(), groupID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups", http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsAddMember(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	err := h.deps.Group.AddMember(r.Context(), domain.AddGroupMemberRequest{
		GroupID:    groupID,
		MemberID:   formString(r.Form, "member_id"),
		MemberType: formString(r.Form, "member_type"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups/"+groupID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsRemoveMember(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	err := h.deps.Group.RemoveMember(r.Context(), domain.RemoveGroupMemberRequest{
		GroupID:    groupID,
		MemberID:   formString(r.Form, "member_id"),
		MemberType: formString(r.Form, "member_type"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups/"+groupID, http.StatusSeeOther)
}

func (h *Handler) SecurityGrantsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	principalID := r.URL.Query().Get("principal_id")
	principalType := r.URL.Query().Get("principal_type")
	securableType := r.URL.Query().Get("securable_type")
	securableID := r.URL.Query().Get("securable_id")

	var (
		items []domain.PrivilegeGrant
		total int64
		err   error
	)
	switch {
	case principalID != "" && principalType != "":
		items, total, err = h.deps.Grant.ListForPrincipal(r.Context(), principalID, principalType, pageReq)
	case securableID != "" && securableType != "":
		items, total, err = h.deps.Grant.ListForSecurable(r.Context(), securableType, securableID, pageReq)
	default:
		items, total, err = h.deps.Grant.ListAll(r.Context(), pageReq)
	}
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]securityGrantRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, securityGrantRowData{
			ID:            item.ID,
			Filter:        item.PrincipalID + " " + item.Privilege + " " + item.SecurableType + " " + item.SecurableID,
			PrincipalID:   item.PrincipalID,
			PrincipalType: item.PrincipalType,
			SecurableType: item.SecurableType,
			SecurableID:   item.SecurableID,
			Privilege:     item.Privilege,
			GrantedAt:     formatTime(item.GrantedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, securityGrantsPage(securityGrantsPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Rows:              rows,
		Page:              pageReq,
		Total:             total,
		PrincipalID:       principalID,
		PrincipalType:     principalType,
		SecurableType:     securableType,
		SecurableID:       securableID,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) SecurityGrantsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateGrantRequest{
		PrincipalID:   formString(r.Form, "principal_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		SecurableType: formString(r.Form, "securable_type"),
		SecurableID:   formString(r.Form, "securable_id"),
		Privilege:     formString(r.Form, "privilege"),
	}
	if _, err := h.deps.Grant.Grant(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}

	target := "/ui/security/grants"
	if req.PrincipalID != "" && req.PrincipalType != "" {
		target = "/ui/security/grants?principal_id=" + url.QueryEscape(req.PrincipalID) + "&principal_type=" + url.QueryEscape(req.PrincipalType)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
func (h *Handler) SecurityGrantsDelete(w http.ResponseWriter, r *http.Request) {
	grantID := chi.URLParam(r, "grantID")
	if err := h.deps.Grant.Revoke(r.Context(), "", grantID); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/grants"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityRowFiltersList(w http.ResponseWriter, r *http.Request) {
	tableID := r.URL.Query().Get("table_id")
	rows := []securityRowFilterRowData{}
	if tableID != "" {
		items, _, err := h.deps.RowFilter.GetForTable(r.Context(), tableID, domain.PageRequest{MaxResults: 100})
		if err != nil {
			renderServiceError(w, err)
			return
		}
		for i := range items {
			item := items[i]
			bindings, err := h.deps.RowFilter.ListBindings(r.Context(), item.ID)
			if err != nil {
				renderServiceError(w, err)
				return
			}
			rows = append(rows, securityRowFilterRowData{
				ID:          item.ID,
				TableID:     item.TableID,
				FilterSQL:   item.FilterSQL,
				Description: item.Description,
				CreatedAt:   formatTime(item.CreatedAt),
				Bindings:    bindings,
			})
		}
	}
	core.RenderHTML(w, http.StatusOK, securityRowFiltersPage(securityRowFilterPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		TableID:           tableID,
		Rows:              rows,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) SecurityRowFiltersCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateRowFilterRequest{
		TableID:     formString(r.Form, "table_id"),
		Description: formString(r.Form, "description"),
		FilterSQL:   formString(r.Form, "filter_sql"),
	}
	if _, err := h.deps.RowFilter.Create(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/row-filters?table_id="+url.QueryEscape(req.TableID), http.StatusSeeOther)
}
func (h *Handler) SecurityRowFiltersDelete(w http.ResponseWriter, r *http.Request) {
	filterID := chi.URLParam(r, "filterID")
	if err := h.deps.RowFilter.Delete(r.Context(), filterID); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/row-filters"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityRowFiltersBind(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.BindRowFilterRequest{
		RowFilterID:   formString(r.Form, "row_filter_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		PrincipalID:   formString(r.Form, "principal_id"),
	}
	if err := h.deps.RowFilter.Bind(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/row-filters"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityRowFiltersUnbind(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.BindRowFilterRequest{
		RowFilterID:   formString(r.Form, "row_filter_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		PrincipalID:   formString(r.Form, "principal_id"),
	}
	if err := h.deps.RowFilter.Unbind(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/row-filters"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityColumnMasksList(w http.ResponseWriter, r *http.Request) {
	tableID := r.URL.Query().Get("table_id")
	rows := []securityColumnMaskRowData{}
	if tableID != "" {
		items, _, err := h.deps.ColumnMask.GetForTable(r.Context(), tableID, domain.PageRequest{MaxResults: 100})
		if err != nil {
			renderServiceError(w, err)
			return
		}
		for i := range items {
			item := items[i]
			bindings, err := h.deps.ColumnMask.ListBindings(r.Context(), item.ID)
			if err != nil {
				renderServiceError(w, err)
				return
			}
			rows = append(rows, securityColumnMaskRowData{
				ID:             item.ID,
				TableID:        item.TableID,
				ColumnName:     item.ColumnName,
				MaskExpression: item.MaskExpression,
				Description:    item.Description,
				CreatedAt:      formatTime(item.CreatedAt),
				Bindings:       bindings,
			})
		}
	}
	core.RenderHTML(w, http.StatusOK, securityColumnMasksPage(securityColumnMaskPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		TableID:           tableID,
		Rows:              rows,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) SecurityColumnMasksCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateColumnMaskRequest{
		TableID:        formString(r.Form, "table_id"),
		ColumnName:     formString(r.Form, "column_name"),
		Description:    formString(r.Form, "description"),
		MaskExpression: formString(r.Form, "mask_expression"),
	}
	if _, err := h.deps.ColumnMask.Create(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/column-masks?table_id="+url.QueryEscape(req.TableID), http.StatusSeeOther)
}
func (h *Handler) SecurityColumnMasksDelete(w http.ResponseWriter, r *http.Request) {
	maskID := chi.URLParam(r, "maskID")
	if err := h.deps.ColumnMask.Delete(r.Context(), maskID); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/column-masks"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityColumnMasksBind(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.BindColumnMaskRequest{
		ColumnMaskID:  formString(r.Form, "column_mask_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		PrincipalID:   formString(r.Form, "principal_id"),
		SeeOriginal:   formBool(r.Form, "see_original"),
	}
	if err := h.deps.ColumnMask.Bind(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/column-masks"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityColumnMasksUnbind(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.BindColumnMaskRequest{
		ColumnMaskID:  formString(r.Form, "column_mask_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		PrincipalID:   formString(r.Form, "principal_id"),
	}
	if err := h.deps.ColumnMask.Unbind(r.Context(), req); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/column-masks"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityAPIKeysList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	selectedPrincipal := r.URL.Query().Get("principal_id")
	var principalID *string
	if selectedPrincipal != "" {
		principalID = &selectedPrincipal
	}
	items, total, err := h.deps.APIKey.List(r.Context(), principalID, pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]securityAPIKeyRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, securityAPIKeyRowData{
			ID:          item.ID,
			Filter:      item.Name + " " + item.KeyPrefix + " " + item.PrincipalID,
			Name:        item.Name,
			PrincipalID: item.PrincipalID,
			KeyPrefix:   item.KeyPrefix,
			ExpiresAt:   formatTimePtr(item.ExpiresAt),
			CreatedAt:   formatTime(item.CreatedAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, securityAPIKeysPage(securityAPIKeysPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Rows:              rows,
		Page:              pageReq,
		Total:             total,
		SelectedPrincipal: selectedPrincipal,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) SecurityAPIKeysCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	expiresAt, err := formOptionalTime(r.Form, "expires_at")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "expires_at must be YYYY-MM-DD, YYYY-MM-DDTHH:MM, or RFC3339."))
		return
	}
	req := domain.CreateAPIKeyRequest{
		PrincipalID: formString(r.Form, "principal_id"),
		Name:        formString(r.Form, "name"),
		ExpiresAt:   expiresAt,
	}
	rawKey, key, err := h.deps.APIKey.Create(r.Context(), req)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, securityAPIKeyCreatedPage(core.PrincipalFromContext(r.Context()), key.PrincipalID, key.Name, rawKey))
}
func (h *Handler) SecurityAPIKeysDelete(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyID")
	if err := h.deps.APIKey.Delete(r.Context(), keyID); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/api-keys"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) SecurityAPIKeysCleanup(w http.ResponseWriter, r *http.Request) {
	if _, err := h.deps.APIKey.CleanupExpired(r.Context()); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/security/api-keys", http.StatusSeeOther)
}

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formBool(values map[string][]string, key string) bool {
	v := strings.ToLower(formString(values, key))
	return v == "true" || v == "1" || v == "on" || v == "yes"
}

func boolFormPtr(values map[string][]string, key string) *bool {
	v := formBool(values, key)
	return &v
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func formOptionalTime(values map[string][]string, key string) (*time.Time, error) {
	value := formString(values, key)
	if value == "" {
		return nil, nil
	}

	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return &parsed, nil
		}
	}
	return nil, fmt.Errorf("invalid time format")
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
}

func pageFromRequest(r *http.Request, defaultPageSize int) domain.PageRequest {
	maxResults := defaultPageSize
	if maxResults <= 0 {
		maxResults = 25
	}
	if raw := r.URL.Query().Get("max_results"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			maxResults = parsed
		}
	}
	if maxResults < 1 {
		maxResults = 1
	}
	if maxResults > 200 {
		maxResults = 200
	}
	return domain.PageRequest{
		MaxResults: maxResults,
		PageToken:  r.URL.Query().Get("page_token"),
	}
}

func formatCount(total int64) string {
	return strconv.FormatInt(total, 10)
}
