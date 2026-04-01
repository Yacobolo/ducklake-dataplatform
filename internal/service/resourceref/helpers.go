package resourceref

import (
	"net/url"
	"path"
	"strings"

	"duck-demo/internal/domain"

	"github.com/google/uuid"
)

func NormalizeHref(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", domain.ErrValidation("href is required")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", domain.ErrValidation("href is invalid")
	}
	if parsed.IsAbs() {
		return "", domain.ErrValidation("href must be an internal UI path")
	}
	cleaned := strings.TrimSpace(parsed.Path)
	if cleaned == "" {
		return "", domain.ErrValidation("href is required")
	}
	cleaned = path.Clean(cleaned)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	if cleaned != "/" {
		cleaned = strings.TrimRight(cleaned, "/")
	}
	if cleaned == "/ui" {
		return cleaned, nil
	}
	if !strings.HasPrefix(cleaned, "/ui") {
		return "", domain.ErrValidation("href must be a UI path")
	}
	if !isTrackableUIPath(cleaned) {
		return "", domain.ErrValidation("href %q is not trackable", cleaned)
	}
	return cleaned, nil
}

func HrefForResource(resourceType string, resourceKey string) (string, error) {
	resourceType = strings.TrimSpace(resourceType)
	resourceKey, err := normalizeResourceKey(resourceType, resourceKey)
	if err != nil {
		return "", err
	}

	switch resourceType {
	case "workspace":
		return NormalizeHref("/ui/" + resourceKey)
	case "product":
		return "/ui/products/" + url.PathEscape(resourceKey), nil
	case "runtime-asset":
		return "/ui/assets/" + url.PathEscape(resourceKey), nil
	case "dashboard":
		return "/ui/dashboards/" + url.PathEscape(resourceKey), nil
	case "notebook":
		return "/ui/notebooks/" + url.PathEscape(resourceKey), nil
	case "pipeline":
		return "/ui/pipelines/" + url.PathEscape(resourceKey), nil
	case "compute-endpoint":
		return "/ui/compute/endpoints/" + url.PathEscape(resourceKey), nil
	case "model":
		projectName, name, err := splitPairKey(resourceKey)
		if err != nil {
			return "", err
		}
		return "/ui/models/" + url.PathEscape(projectName) + "/" + url.PathEscape(name), nil
	case "semantic-model":
		projectName, name, err := splitPairKey(resourceKey)
		if err != nil {
			return "", err
		}
		return "/ui/semantic/models/" + url.PathEscape(projectName) + "/" + url.PathEscape(name), nil
	default:
		return "", domain.ErrValidation("unsupported resource_type %q", resourceType)
	}
}

func Normalize(resource domain.ResourceRef) (domain.ResourceRef, error) {
	resource.ResourceType = strings.TrimSpace(resource.ResourceType)
	if resource.ResourceType == "" {
		return domain.ResourceRef{}, domain.ErrValidation("resource_type is required")
	}

	normalizedKey, err := normalizeResourceKey(resource.ResourceType, resource.ResourceKey)
	if err != nil {
		return domain.ResourceRef{}, err
	}
	resource.ResourceKey = normalizedKey

	href, err := HrefForResource(resource.ResourceType, resource.ResourceKey)
	if err != nil {
		return domain.ResourceRef{}, err
	}
	resource.Href = href

	resource.DisplayName = strings.TrimSpace(resource.DisplayName)
	if resource.DisplayName == "" {
		resource.DisplayName = resource.ResourceKey
	}

	resource.ResourcePath = strings.TrimSpace(resource.ResourcePath)
	resource.Section = strings.TrimSpace(resource.Section)
	if resource.Section == "" {
		resource.Section = inferSection(resource.ResourceType, resource.ResourceKey)
	}

	return resource, nil
}

func HydrateRecent(items []domain.ResourceAccessEvent) ([]domain.ResourceAccessEvent, error) {
	out := make([]domain.ResourceAccessEvent, 0, len(items))
	for i := range items {
		if !IsRecentResource(items[i].ResourceRef) {
			continue
		}
		ref, err := Normalize(items[i].ResourceRef)
		if err != nil {
			continue
		}
		items[i].ResourceRef = ref
		out = append(out, items[i])
	}
	return out, nil
}

func HydrateSaved(items []domain.SavedResource) ([]domain.SavedResource, error) {
	out := make([]domain.SavedResource, 0, len(items))
	for i := range items {
		if !IsRecentResource(items[i].ResourceRef) {
			continue
		}
		ref, err := Normalize(items[i].ResourceRef)
		if err != nil {
			continue
		}
		items[i].ResourceRef = ref
		out = append(out, items[i])
	}
	return out, nil
}

func IsRecentResource(resource domain.ResourceRef) bool {
	if strings.TrimSpace(resource.ResourceType) == "workspace" {
		return false
	}
	_, err := uuid.Parse(strings.TrimSpace(resource.ResourceKey))
	return err == nil
}

func normalizeResourceKey(resourceType string, resourceKey string) (string, error) {
	cleaned := strings.TrimSpace(resourceKey)
	if cleaned == "" {
		return "", domain.ErrValidation("resource_key is required")
	}

	switch resourceType {
	case "workspace":
		cleaned = strings.Trim(path.Clean(cleaned), "/")
		if cleaned == "" || cleaned == "." {
			return "", domain.ErrValidation("resource_key is required")
		}
		if _, err := NormalizeHref("/ui/" + cleaned); err != nil {
			return "", err
		}
		return cleaned, nil
	case "model", "semantic-model":
		projectName, name, err := splitPairKey(cleaned)
		if err != nil {
			return "", err
		}
		return projectName + "/" + name, nil
	case "product", "runtime-asset", "dashboard", "notebook", "pipeline", "compute-endpoint":
		cleaned = strings.Trim(cleaned, "/")
		if cleaned == "" {
			return "", domain.ErrValidation("resource_key is required")
		}
		return cleaned, nil
	default:
		return "", domain.ErrValidation("unsupported resource_type %q", resourceType)
	}
}

func splitPairKey(resourceKey string) (string, string, error) {
	parts := pathSegments(resourceKey)
	if len(parts) != 2 {
		return "", "", domain.ErrValidation("resource_key must have exactly two segments")
	}
	return parts[0], parts[1], nil
}

func inferSection(resourceType string, resourceKey string) string {
	switch resourceType {
	case "product", "runtime-asset", "dashboard":
		return "Discover"
	case "model", "semantic-model", "notebook", "pipeline":
		return "Build"
	case "compute-endpoint":
		return "Operate"
	case "workspace":
		segment := firstSegment(resourceKey)
		switch segment {
		case "explore", "products", "catalogs", "assets", "dashboards":
			return "Discover"
		case "models", "semantic", "pipelines", "notebooks", "macros":
			return "Build"
		case "security", "governance", "storage", "compute":
			return "Operate"
		default:
			return "Workspace"
		}
	default:
		return "Workspace"
	}
}

func isTrackableUIPath(href string) bool {
	switch {
	case href == "/ui":
		return false
	case href == "/ui/login", strings.HasPrefix(href, "/ui/login/"):
		return false
	case href == "/ui/logout":
		return false
	case strings.HasPrefix(href, "/ui/static/"):
		return false
	case href == "/ui/components", strings.HasPrefix(href, "/ui/components/"):
		return false
	case href == "/ui/resources", strings.HasPrefix(href, "/ui/resources/"):
		return false
	}

	segments := pathSegments(href)
	for _, segment := range segments {
		switch segment {
		case "new", "edit", "move", "duplicate", "delete", "share", "sync", "create", "update":
			return false
		}
	}
	return true
}

func firstSegment(value string) string {
	segments := pathSegments(value)
	if len(segments) == 0 {
		return ""
	}
	return segments[0]
}

func pathSegments(value string) []string {
	parts := strings.Split(strings.Trim(strings.TrimSpace(value), "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		return nil
	}
	return parts
}
