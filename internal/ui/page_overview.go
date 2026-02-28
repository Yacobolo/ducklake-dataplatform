package ui

import (
	"duck-demo/internal/domain"

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
		nodes = append(nodes, Div(Class(cardClass()), H2(Text(c.Title)), P(Text(c.Description)), A(Href(c.Href), Text(c.LinkLabel))))
	}
	return appPage("Overview", "home", principal, Div(Class("grid"), Group(nodes)))
}
