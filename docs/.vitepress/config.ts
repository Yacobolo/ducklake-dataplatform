import { defineConfig } from "vitepress";

function basePath(): string {
  const value = process.env.DOCS_BASE;
  if (!value || value.trim() === "") {
    return "/";
  }

  const withLeading = value.startsWith("/") ? value : `/${value}`;
  return withLeading.endsWith("/") ? withLeading : `${withLeading}/`;
}

export default defineConfig({
  title: "Duck Data Platform",
  description: "Contract-first documentation for the Duck data platform",
  base: basePath(),
  head: [["link", { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }]],
  themeConfig: {
    logo: "/favicon.svg",
    nav: [
      { text: "Guide", link: "/" },
      { text: "Getting Started", link: "/getting-started" },
      { text: "Features", link: "/reference/generated/api/features" },
      { text: "API", link: "/reference/generated/api/index" },
      { text: "Declarative", link: "/reference/generated/declarative/index" }
    ],
    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Overview", link: "/" },
          { text: "Getting Started", link: "/getting-started" },
          { text: "Core Concepts", link: "/core-concepts" },
          { text: "Declarative Schema", link: "/declarative-schema" }
        ]
      },
      {
        text: "Reference",
        items: [
          { text: "Platform Features", link: "/reference/generated/api/features" },
          { text: "API Reference", link: "/reference/generated/api/index" },
          {
            text: "Declarative Reference",
            link: "/reference/generated/declarative/index"
          }
        ]
      }
    ]
  }
});
