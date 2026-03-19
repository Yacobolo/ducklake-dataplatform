// Package sitegen builds the static public site from markdown content and templates.
package sitegen

import "html/template"

// Builder assembles the static site from content, config, templates, and assets.
type Builder struct {
	ContentDir   string
	ConfigPath   string
	NavPath      string
	TemplatesDir string
	AssetsDir    string
	OutDir       string
	BaseURL      string
}

type siteConfig struct {
	Title          string `yaml:"title"`
	Description    string `yaml:"description"`
	GitHubEditBase string `yaml:"github_edit_base"`
}

type navConfig struct {
	Primary []navLinkConfig  `yaml:"primary"`
	Docs    []navGroupConfig `yaml:"docs"`
	API     []navGroupConfig `yaml:"api"`
}

type navLinkConfig struct {
	Title string `yaml:"title"`
	Path  string `yaml:"path"`
}

type navGroupConfig struct {
	Title    string           `yaml:"title"`
	Icon     string           `yaml:"icon"`
	Expanded bool             `yaml:"expanded"`
	Items    []navEntryConfig `yaml:"items"`
}

type navEntryConfig struct {
	Source   string           `yaml:"source"`
	Title    string           `yaml:"title"`
	Icon     string           `yaml:"icon"`
	Expanded bool             `yaml:"expanded"`
	AutoDir  string           `yaml:"autogen_dir"`
	Items    []navEntryConfig `yaml:"items"`
}

type docFrontMatter struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Keywords    []string `yaml:"keywords"`
}

type homeFrontMatter struct {
	Layout   string      `yaml:"layout"`
	Hero     homeHero    `yaml:"hero"`
	Pillars  homeSection `yaml:"pillars"`
	Title    string      `yaml:"title"`
	Keywords []string    `yaml:"keywords"`
}

type homeHero struct {
	Name          string             `yaml:"name"`
	Eyebrow       string             `yaml:"eyebrow"`
	Text          string             `yaml:"text"`
	Headline      string             `yaml:"headline"`
	Tagline       string             `yaml:"tagline"`
	SnapshotTitle string             `yaml:"snapshot_title"`
	SnapshotNote  string             `yaml:"snapshot_note"`
	Actions       []homeHeroLink     `yaml:"actions"`
	Proofs        []homeHeroProof    `yaml:"proofs"`
	Snapshot      []homeHeroSnapshot `yaml:"snapshot"`
}

type homeHeroLink struct {
	Theme string `yaml:"theme"`
	Text  string `yaml:"text"`
	Link  string `yaml:"link"`
}

type homeHeroProof struct {
	Icon string `yaml:"icon"`
	Text string `yaml:"text"`
}

type homeHeroSnapshot struct {
	Icon  string `yaml:"icon"`
	Label string `yaml:"label"`
	Value string `yaml:"value"`
}

type homeSection struct {
	Eyebrow string        `yaml:"eyebrow"`
	Title   string        `yaml:"title"`
	Details string        `yaml:"details"`
	Items   []homeFeature `yaml:"items"`
}

type homeFeature struct {
	Label   string `yaml:"label"`
	Title   string `yaml:"title"`
	Details string `yaml:"details"`
	Link    string `yaml:"link"`
	Icon    string `yaml:"icon"`
}

type homeFeatureCardView struct {
	Wide bool
	Item homeFeature
}

type homeVisualView struct {
	AccessTitle    string
	AccessValue    string
	PolicyTitle    string
	PolicyValue    string
	ExecutionTitle string
	ExecutionValue string
}

type homeHeroView struct {
	Eyebrow       string
	Headline      string
	Tagline       string
	SnapshotTitle string
	SnapshotNote  string
	Actions       []homeHeroLink
	Proofs        []homeHeroProof
	Snapshot      []homeHeroSnapshot
	Visual        homeVisualView
}

type homeSectionView struct {
	Eyebrow string
	Title   string
	Details string
	Cards   []homeFeatureCardView
}

type homePageView struct {
	Hero    homeHeroView
	Pillars homeSectionView
}

type pageKind string

const (
	pageKindHome pageKind = "home"
	pageKindDocs pageKind = "docs"
	pageKindAPI  pageKind = "api"
)

type page struct {
	SourcePath   string
	RelPath      string
	URLPath      string
	MirrorPath   string
	Title        string
	Description  string
	Kind         pageKind
	Section      string
	BodyMarkdown string
	MirrorBody   string
	BodyHTML     template.HTML
	Headings     []heading
	Keywords     []string
	IsHome       bool
	Home         homeFrontMatter
}

type heading struct {
	Level  int
	ID     string
	Title  string
	Method string
}

type navItem struct {
	Title  string
	Path   string
	Active bool
}

type navGroup struct {
	Title string
	Icon  string
	Nodes []navNode
	Open  bool
}

type navNode struct {
	Title       string
	Icon        string
	Path        string
	Active      bool
	Open        bool
	ForceOpen   bool
	Children    []navNode
	Method      string
	RoutePath   string
	Description string
}

type searchItem struct {
	Title       string `json:"title"`
	Path        string `json:"path"`
	Description string `json:"description"`
	Section     string `json:"section"`
	SearchText  string `json:"search_text"`
}

type pageTemplateData struct {
	Site          siteConfig
	SiteRoot      string
	Page          page
	MetaTitle     string
	BodyHTML      template.HTML
	TopNav        []navItem
	SidebarGroups []navGroup
	TOC           []heading
	Breadcrumbs   []navItem
	Prev          *navItem
	Next          *navItem
	Home          homeFrontMatter
	HomeView      homePageView
	MirrorPath    string
}

type templateSet struct {
	base *template.Template
}
