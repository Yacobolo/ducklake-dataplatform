package ui

type segmentedTabItem struct {
	Label  string
	Active bool
}

type formFieldConfig struct {
	Label        string
	Name         string
	Type         string
	Value        string
	Placeholder  string
	Required     bool
	HelpText     string
	ErrorMessage string
	Invalid      bool
}

type treeViewItem struct {
	Label    string
	Icon     string
	Href     string
	Open     bool
	Active   bool
	Children []treeViewItem
}

type breadcrumbItem struct {
	Label  string
	Href   string
	Active bool
}

type avatarConfig struct {
	Label string
	Tone  string
	Size  string
}
