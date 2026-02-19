package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

type overviewCardData struct {
	Title       string
	Description string
	Href        string
	LinkLabel   string
}

func overviewPage(principal domain.ContextPrincipal, cards []overviewCardData) gomponents.Node {
	nodes := make([]gomponents.Node, 0, len(cards))
	for i := range cards {
		c := cards[i]
		nodes = append(nodes, html.Div(html.Class(cardClass()), html.H2(gomponents.Text(c.Title)), html.P(gomponents.Text(c.Description)), html.A(html.Href(c.Href), gomponents.Text(c.LinkLabel))))
	}
	return appPage("Overview", "home", principal, html.Div(html.Class("grid"), gomponents.Group(nodes)))
}
