package ui

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) SecurityHome(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, securityHomePage(principalFromContext(r.Context()), []securityCardData{
		{Title: "Principals", Description: "Create principals, inspect admin access, and review owned credentials.", Href: "/ui/security/principals", LinkLabel: "Open principals ->"},
		{Title: "Groups", Description: "Manage membership and review group-level grants.", Href: "/ui/security/groups", LinkLabel: "Open groups ->"},
		{Title: "Grants", Description: "Inspect and issue privilege grants across securables.", Href: "/ui/security/grants", LinkLabel: "Open grants ->"},
		{Title: "API Keys", Description: "Issue, revoke, and clean up programmatic access keys.", Href: "/ui/security/api-keys", LinkLabel: "Open API keys ->"},
	}))
}

func (h *Handler) SecurityPrincipalsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Principal.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
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
	renderHTML(w, http.StatusOK, securityPrincipalsListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) SecurityPrincipalsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, securityPrincipalFormPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) SecurityPrincipalsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.Principal.Create(r.Context(), domain.CreatePrincipalRequest{
		Name:    formString(r.Form, "name"),
		Type:    formString(r.Form, "type"),
		IsAdmin: formBool(r.Form, "is_admin"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) SecurityPrincipalsDetail(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	item, err := h.Principal.GetByID(r.Context(), principalID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	grants, _, err := h.Grant.ListForPrincipal(r.Context(), item.ID, item.Type, domain.PageRequest{MaxResults: 50})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	keys, _, err := h.APIKey.List(r.Context(), &item.ID, domain.PageRequest{MaxResults: 50})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, securityPrincipalDetailPage(securityPrincipalDetailPageData{
		Principal:         principalFromContext(r.Context()),
		Item:              item,
		Grants:            grants,
		APIKeys:           keys,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SecurityPrincipalsDelete(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	if err := h.Principal.Delete(r.Context(), principalID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals", http.StatusSeeOther)
}

func (h *Handler) SecurityPrincipalsSetAdmin(w http.ResponseWriter, r *http.Request) {
	principalID := chi.URLParam(r, "principalID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	if err := h.Principal.SetAdmin(r.Context(), principalID, formBool(r.Form, "is_admin")); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/principals/"+principalID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Group.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]securityGroupRowData, 0, len(items))
	for i := range items {
		item := items[i]
		memberCount := "0"
		members, totalMembers, membersErr := h.Group.ListMembers(r.Context(), item.ID, domain.PageRequest{MaxResults: 1})
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
	renderHTML(w, http.StatusOK, securityGroupsListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func formatCount(total int64) string {
	return strconv.FormatInt(total, 10)
}

func (h *Handler) SecurityGroupsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, securityGroupFormPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) SecurityGroupsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.Group.Create(r.Context(), domain.CreateGroupRequest{
		Name:        formString(r.Form, "name"),
		Description: formString(r.Form, "description"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsDetail(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	item, err := h.Group.GetByID(r.Context(), groupID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	members, _, err := h.Group.ListMembers(r.Context(), groupID, domain.PageRequest{MaxResults: 100})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	memberRows := make([]securityGroupMemberRowData, 0, len(members))
	for i := range members {
		member := members[i]
		memberRows = append(memberRows, securityGroupMemberRowData{
			GroupID:    groupID,
			MemberID:   member.MemberID,
			MemberType: member.MemberType,
			CSRFField:  csrfFieldProvider(r),
		})
	}

	grants, _, err := h.Grant.ListForPrincipal(r.Context(), groupID, "group", domain.PageRequest{MaxResults: 50})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, securityGroupDetailPage(securityGroupDetailPageData{
		Principal:         principalFromContext(r.Context()),
		Item:              item,
		Members:           memberRows,
		Grants:            grants,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SecurityGroupsDelete(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if err := h.Group.Delete(r.Context(), groupID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups", http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsAddMember(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	err := h.Group.AddMember(r.Context(), domain.AddGroupMemberRequest{
		GroupID:    groupID,
		MemberID:   formString(r.Form, "member_id"),
		MemberType: formString(r.Form, "member_type"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/groups/"+groupID, http.StatusSeeOther)
}

func (h *Handler) SecurityGroupsRemoveMember(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	err := h.Group.RemoveMember(r.Context(), domain.RemoveGroupMemberRequest{
		GroupID:    groupID,
		MemberID:   formString(r.Form, "member_id"),
		MemberType: formString(r.Form, "member_type"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
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
		items, total, err = h.Grant.ListForPrincipal(r.Context(), principalID, principalType, pageReq)
	case securableID != "" && securableType != "":
		items, total, err = h.Grant.ListForSecurable(r.Context(), securableType, securableID, pageReq)
	default:
		items, total, err = h.Grant.ListAll(r.Context(), pageReq)
	}
	if err != nil {
		h.renderServiceError(w, r, err)
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

	renderHTML(w, http.StatusOK, securityGrantsPage(securityGrantsPageData{
		Principal:         principalFromContext(r.Context()),
		Rows:              rows,
		Page:              pageReq,
		Total:             total,
		PrincipalID:       principalID,
		PrincipalType:     principalType,
		SecurableType:     securableType,
		SecurableID:       securableID,
		CSRFFieldProvider: csrfFieldProvider(r),
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
	if _, err := h.Grant.Grant(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
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
	if err := h.Grant.Revoke(r.Context(), "", grantID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/grants"
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
	items, total, err := h.APIKey.List(r.Context(), principalID, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
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
	renderHTML(w, http.StatusOK, securityAPIKeysPage(securityAPIKeysPageData{
		Principal:         principalFromContext(r.Context()),
		Rows:              rows,
		Page:              pageReq,
		Total:             total,
		SelectedPrincipal: selectedPrincipal,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SecurityAPIKeysCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	expiresAt, err := formOptionalTime(r.Form, "expires_at")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "expires_at must be YYYY-MM-DD, YYYY-MM-DDTHH:MM, or RFC3339."))
		return
	}
	req := domain.CreateAPIKeyRequest{
		PrincipalID: formString(r.Form, "principal_id"),
		Name:        formString(r.Form, "name"),
		ExpiresAt:   expiresAt,
	}
	rawKey, key, err := h.APIKey.Create(r.Context(), req)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, securityAPIKeyCreatedPage(principalFromContext(r.Context()), key.PrincipalID, key.Name, rawKey))
}

func (h *Handler) SecurityAPIKeysDelete(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyID")
	if err := h.APIKey.Delete(r.Context(), keyID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/api-keys"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (h *Handler) SecurityAPIKeysCleanup(w http.ResponseWriter, r *http.Request) {
	if _, err := h.APIKey.CleanupExpired(r.Context()); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/api-keys", http.StatusSeeOther)
}

func (h *Handler) SecurityRowFiltersList(w http.ResponseWriter, r *http.Request) {
	tableID := r.URL.Query().Get("table_id")
	rows := []securityRowFilterRowData{}
	if tableID != "" {
		items, _, err := h.RowFilter.GetForTable(r.Context(), tableID, domain.PageRequest{MaxResults: 100})
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		for i := range items {
			item := items[i]
			bindings, err := h.RowFilter.ListBindings(r.Context(), item.ID)
			if err != nil {
				h.renderServiceError(w, r, err)
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
	renderHTML(w, http.StatusOK, securityRowFiltersPage(securityRowFilterPageData{
		Principal:         principalFromContext(r.Context()),
		TableID:           tableID,
		Rows:              rows,
		CSRFFieldProvider: csrfFieldProvider(r),
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
	if _, err := h.RowFilter.Create(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/row-filters?table_id="+url.QueryEscape(req.TableID), http.StatusSeeOther)
}

func (h *Handler) SecurityRowFiltersDelete(w http.ResponseWriter, r *http.Request) {
	filterID := chi.URLParam(r, "filterID")
	if err := h.RowFilter.Delete(r.Context(), filterID); err != nil {
		h.renderServiceError(w, r, err)
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
	if err := h.RowFilter.Bind(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
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
	if err := h.RowFilter.Unbind(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
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
		items, _, err := h.ColumnMask.GetForTable(r.Context(), tableID, domain.PageRequest{MaxResults: 100})
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		for i := range items {
			item := items[i]
			bindings, err := h.ColumnMask.ListBindings(r.Context(), item.ID)
			if err != nil {
				h.renderServiceError(w, r, err)
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
	renderHTML(w, http.StatusOK, securityColumnMasksPage(securityColumnMaskPageData{
		Principal:         principalFromContext(r.Context()),
		TableID:           tableID,
		Rows:              rows,
		CSRFFieldProvider: csrfFieldProvider(r),
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
	if _, err := h.ColumnMask.Create(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/security/column-masks?table_id="+url.QueryEscape(req.TableID), http.StatusSeeOther)
}

func (h *Handler) SecurityColumnMasksDelete(w http.ResponseWriter, r *http.Request) {
	maskID := chi.URLParam(r, "maskID")
	if err := h.ColumnMask.Delete(r.Context(), maskID); err != nil {
		h.renderServiceError(w, r, err)
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
	if err := h.ColumnMask.Bind(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
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
	if err := h.ColumnMask.Unbind(r.Context(), req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/security/column-masks"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
