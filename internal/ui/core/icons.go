package core

import (
	"strings"

	lucide "github.com/eduardolat/gomponents-lucide"
	. "maragu.dev/gomponents"
)

var iconFactories = map[string]func(children ...Node) Node{
	"boxes":             lucide.Boxes,
	"bookmark":          lucide.Bookmark,
	"braces":            lucide.Braces,
	"chart-column":      lucide.ChartColumn,
	"chevron-down":      lucide.ChevronDown,
	"chevron-right":     lucide.ChevronRight,
	"clock-3":           lucide.Clock3,
	"compass":           lucide.Compass,
	"cpu":               lucide.Cpu,
	"database":          lucide.Database,
	"download":          lucide.Download,
	"ellipsis":          lucide.Ellipsis,
	"ellipsis-vertical": lucide.EllipsisVertical,
	"eye":               lucide.Eye,
	"file-chart-column": lucide.FileChartColumn,
	"file-stack":        lucide.FileStack,
	"file-text":         lucide.FileText,
	"filter":            lucide.ListFilter,
	"filter-x":          lucide.FunnelX,
	"folder":            lucide.Folder,
	"folder-git-2":      lucide.FolderGit2,
	"folder-lock":       lucide.FolderLock,
	"folder-plus":       lucide.FolderPlus,
	"folder-tree":       lucide.FolderTree,
	"git-branch":        lucide.GitBranch,
	"git-fork":          lucide.GitFork,
	"grip-vertical":     lucide.GripVertical,
	"hard-drive":        lucide.HardDrive,
	"house":             lucide.House,
	"inbox":             lucide.Inbox,
	"key-round":         lucide.KeyRound,
	"layout-grid":       lucide.LayoutGrid,
	"list-tree":         lucide.ListTree,
	"menu":              lucide.Menu,
	"moon":              lucide.Moon,
	"notebook-text":     lucide.NotebookText,
	"package-open":      lucide.PackageOpen,
	"panel-left":        lucide.PanelLeft,
	"panel-left-close":  lucide.PanelLeftClose,
	"play":              lucide.Play,
	"plus":              lucide.Plus,
	"refresh-cw":        lucide.RefreshCw,
	"rotate-ccw":        lucide.RotateCcw,
	"scan-search":       lucide.ScanSearch,
	"search":            lucide.Search,
	"server":            lucide.Server,
	"settings":          lucide.Settings,
	"share-2":           lucide.Share2,
	"shield":            lucide.Shield,
	"square":            lucide.Square,
	"sun":               lucide.Sun,
	"table":             lucide.Table,
	"user":              lucide.User,
	"users":             lucide.Users,
	"waypoints":         lucide.Waypoints,
	"workflow":          lucide.Workflow,
	"x":                 lucide.X,
}

func Icon(name string, attrs ...Node) Node {
	name = strings.TrimSpace(name)
	nodes := make([]Node, 0, len(attrs)+2)
	nodes = append(nodes, Attr("aria-hidden", "true"), Attr("focusable", "false"))
	nodes = append(nodes, attrs...)
	if factory, ok := iconFactories[name]; ok {
		return factory(nodes...)
	}
	return lucide.Circle(nodes...)
}
