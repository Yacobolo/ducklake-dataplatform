package overview

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type overviewCardData struct {
	Title       string
	Description string
	Href        string
	LinkLabel   string
}

func overviewPage(principal domain.ContextPrincipal, cards []overviewCardData) Node {
	nodes := make([]Node, 0, len(cards))
	for i := range cards {
		c := cards[i]
		nodes = append(nodes, Div(Class(core.CardClass()), H2(Text(c.Title)), P(Text(c.Description)), A(Href(c.Href), Text(c.LinkLabel))))
	}
	return core.AppPage("Overview", "home", principal, Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"), Group(nodes)))
}
