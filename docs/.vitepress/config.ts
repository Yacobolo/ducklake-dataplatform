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
  description: "Product documentation for querying, building, and governing data on Duck",
  base: basePath(),
  srcDir: ".",
  cleanUrls: true,
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }],
    ["meta", { property: "og:title", content: "Duck Data Platform Docs" }],
    [
      "meta",
      {
        property: "og:description",
        content: "Product documentation for platform users working with Duck."
      }
    ],
    ["meta", { property: "og:type", content: "website" }]
  ],
  themeConfig: {
    logo: "/favicon.svg",
    search: {
      provider: "local"
    },
    outline: {
      level: [2, 3],
      label: "On this page"
    },
    lastUpdated: {
      text: "Last updated"
    },
    editLink: {
      pattern: "https://github.com/Yacobolo/ducklake-dataplatform/edit/main/docs/:path",
      text: "Edit this page on GitHub"
    },
    socialLinks: [{ icon: "github", link: "https://github.com/Yacobolo/ducklake-dataplatform" }],
    footer: {
      message: "Product docs first, advanced reference second.",
      copyright: "Duck Data Platform"
    },
    nav: [
      { text: "Guide", link: "/start-here/" },
      { text: "Reference", link: "/reference/" }
    ],
    sidebar: {
      "/start-here/": guideSidebar(),
      "/how-to/": guideSidebar(),
      "/core-concepts/": guideSidebar(),
      "/operations/": guideSidebar(),
      "/reference/": [
        {
          text: "Reference",
          items: [
            { text: "Overview", link: "/reference/" },
            { text: "Glossary", link: "/reference/glossary" },
            { text: "Advanced API Reference", link: "/reference/api" },
            { text: "Advanced Declarative Reference", link: "/reference/declarative" },
            { text: "Advanced CLI Reference", link: "/reference/cli" }
          ]
        },
        {
          text: "Generated Reference",
          items: [
            { text: "API Features", link: "/reference/generated/api/features" },
            { text: "Generated API", link: "/reference/generated/api/index" },
            { text: "Generated Declarative", link: "/reference/generated/declarative/index" }
          ]
        }
      ]
    }
  }
});

function guideSidebar() {
  return [
    {
      text: "Getting Started",
      items: [
        { text: "Getting Started", link: "/start-here/" },
        { text: "Quickstart", link: "/start-here/quickstart" },
        { text: "Ways to Access Duck", link: "/start-here/deployment-modes" }
      ]
    },
    {
      text: "Core Concepts",
      items: [
        { text: "Core Concepts", link: "/core-concepts/" },
        { text: "Platform Objects", link: "/core-concepts/access-control" },
        { text: "Declarative Workflows", link: "/core-concepts/declarative" },
        { text: "Query and Compute", link: "/core-concepts/compute-and-query" }
      ]
    },
    {
      text: "Use Duck",
      items: [
        { text: "Overview", link: "/how-to/" },
        { text: "Access the Platform", link: "/how-to/authentication" },
        { text: "Query and Explore Data", link: "/how-to/use-the-cli" },
        { text: "Manage Access", link: "/how-to/access-control" },
        { text: "Load Data and Build Assets", link: "/how-to/catalog-and-ingestion" },
        { text: "Work Declaratively", link: "/how-to/declarative-workflows" }
      ]
    },
    {
      text: "Administration",
      items: [
        { text: "Overview", link: "/operations/" },
        { text: "Platform Settings", link: "/operations/configuration" },
        { text: "Security Checklist", link: "/operations/security-checklist" },
        { text: "Distributed Compute", link: "/operations/distributed-compute" }
      ]
    }
  ];
}
