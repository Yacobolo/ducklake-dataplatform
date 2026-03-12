(function () {
  const root = document.documentElement;
  const key = "duck-site-theme";
  const themeModes = ["system", "light", "dark"];
  const navButton = document.querySelector("[data-site-nav-toggle]");
  const nav = document.querySelector("[data-site-mobile-nav]");
  const themeButton = document.querySelector("[data-site-theme-toggle]");
  const searchInput = document.querySelector("[data-site-search-input]");
  const searchResults = document.querySelector("[data-site-search-results]");
  const systemMedia =
    typeof window.matchMedia === "function"
      ? window.matchMedia("(prefers-color-scheme: dark)")
      : null;

  function resolveTheme(themePreference) {
    if (themePreference === "system") {
      return systemMedia && systemMedia.matches ? "dark" : "light";
    }
    return themePreference;
  }

  function applyTheme(themePreference) {
    const resolved = resolveTheme(themePreference);
    root.setAttribute("data-theme-preference", themePreference);
    root.setAttribute("data-theme", resolved);
    if (themeButton) {
      themeButton.setAttribute("aria-label", "Theme: " + themePreference);
      themeButton.setAttribute("title", "Theme: " + themePreference);
    }
  }

  let theme = "system";
  try {
    theme = localStorage.getItem(key) || theme;
  } catch (_) {}
  applyTheme(theme);

  if (themeButton) {
    themeButton.addEventListener("click", function () {
      const nextIndex = (themeModes.indexOf(theme) + 1) % themeModes.length;
      theme = themeModes[nextIndex];
      try {
        localStorage.setItem(key, theme);
      } catch (_) {}
      applyTheme(theme);
    });
  }

  if (systemMedia) {
    systemMedia.addEventListener("change", function () {
      if (theme === "system") {
        applyTheme(theme);
      }
    });
  }

  if (navButton && nav) {
    navButton.addEventListener("click", function () {
      nav.classList.toggle("is-open");
    });
  }

  async function loadSearchIndex() {
    try {
      const response = await fetch("/search-index.json");
      if (!response.ok) {
        return [];
      }
      return await response.json();
    } catch (_) {
      return [];
    }
  }

  let indexPromise = null;
  function ensureIndex() {
    if (!indexPromise) {
      indexPromise = loadSearchIndex();
    }
    return indexPromise;
  }

  function renderResults(items) {
    if (!searchResults) {
      return;
    }
    if (!items.length) {
      searchResults.innerHTML = "";
      searchResults.classList.add("hidden");
      return;
    }
    searchResults.classList.remove("hidden");
    searchResults.innerHTML = items
      .map(function (item) {
        return '<a class="block rounded-2xl border p-3 no-underline hover:bg-slate-50 dark:hover:bg-slate-900" href="' +
          item.path +
          '"><div class="text-sm font-semibold text-slate-900 dark:text-slate-100">' +
          item.title +
          '</div><div class="mt-1 text-sm text-slate-600 dark:text-slate-300">' +
          item.description +
          "</div></a>";
      })
      .join("");
  }

  if (searchInput) {
    searchInput.addEventListener("input", async function (event) {
      const query = String(event.target.value || "").trim().toLowerCase();
      if (!query) {
        renderResults([]);
        return;
      }
      const index = await ensureIndex();
      const results = index
        .filter(function (item) {
          return item.search_text.indexOf(query) >= 0;
        })
        .slice(0, 8);
      renderResults(results);
    });
  }

  document.querySelectorAll("[data-site-copy]").forEach(function (button) {
    button.addEventListener("click", async function () {
      const selector = button.getAttribute("data-site-copy");
      const target = selector ? document.querySelector(selector) : null;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const text = target.innerText;
      try {
        await navigator.clipboard.writeText(text);
        button.textContent = "Copied";
        setTimeout(function () {
          button.textContent = "Copy";
        }, 1200);
      } catch (_) {}
    });
  });

  const tocLinks = Array.from(document.querySelectorAll("[data-site-toc-link]"));
  if (tocLinks.length) {
    const tocById = new Map();
    tocLinks.forEach(function (link) {
      const id = link.getAttribute("data-site-toc-link");
      if (id) {
        tocById.set(id, link);
      }
    });

    let activeId = "";
    function setActiveTOC(id) {
      if (!id || id === activeId) {
        return;
      }
      activeId = id;
      tocLinks.forEach(function (link) {
        link.classList.toggle("is-active", link.getAttribute("data-site-toc-link") === id);
      });
    }

    const headings = Array.from(
      document.querySelectorAll(".site-prose h2[id], .site-prose h3[id]")
    ).filter(function (heading) {
      return tocById.has(heading.id);
    });

    if (window.location.hash) {
      setActiveTOC(window.location.hash.slice(1));
    } else if (headings.length) {
      setActiveTOC(headings[0].id);
    }

    if (headings.length && "IntersectionObserver" in window) {
      const visible = new Map();
      const observer = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (entry.isIntersecting) {
              visible.set(entry.target.id, entry.target);
            } else {
              visible.delete(entry.target.id);
            }
          });

          if (visible.size) {
            const nextId = headings.find(function (heading) {
              return visible.has(heading.id);
            });
            if (nextId) {
              setActiveTOC(nextId.id);
            }
            return;
          }

          const current = headings
            .filter(function (heading) {
              return heading.getBoundingClientRect().top <= 140;
            })
            .pop();
          if (current) {
            setActiveTOC(current.id);
          }
        },
        {
          rootMargin: "-96px 0px -60% 0px",
          threshold: [0, 1],
        }
      );

      headings.forEach(function (heading) {
        observer.observe(heading);
      });
    }

    window.addEventListener("hashchange", function () {
      if (window.location.hash) {
        setActiveTOC(window.location.hash.slice(1));
      }
    });
  }

  const apiNavLinks = Array.from(
    document.querySelectorAll("[data-api-sidebar] a[href*='#']")
  );
  if (apiNavLinks.length) {
    const apiLinkById = new Map();
    apiNavLinks.forEach(function (link) {
      const href = link.getAttribute("href") || "";
      const index = href.indexOf("#");
      const id = index >= 0 ? href.slice(index + 1) : "";
      if (id) {
        apiLinkById.set(id, link);
      }
    });

    let activeAPINavId = "";
    function setActiveAPINav(id) {
      if (!id || id === activeAPINavId) {
        return;
      }
      activeAPINavId = id;
      apiNavLinks.forEach(function (link) {
        const href = link.getAttribute("href") || "";
        const index = href.indexOf("#");
        const currentId = index >= 0 ? href.slice(index + 1) : "";
        const isOverviewLink = index < 0 && link.pathname === window.location.pathname;
        link.classList.toggle("is-active", currentId === id && !isOverviewLink);
      });
    }

    const apiSections = Array.from(document.querySelectorAll(".api-prose h2[id]")).filter(
      function (heading) {
        return apiLinkById.has(heading.id);
      }
    );

    if (window.location.hash) {
      setActiveAPINav(window.location.hash.slice(1));
    } else if (apiSections.length) {
      setActiveAPINav(apiSections[0].id);
    }

    if (apiSections.length && "IntersectionObserver" in window) {
      const observer = new IntersectionObserver(
        function (entries) {
          const visible = entries
            .filter(function (entry) {
              return entry.isIntersecting;
            })
            .sort(function (a, b) {
              return a.target.getBoundingClientRect().top - b.target.getBoundingClientRect().top;
            });

          if (visible.length) {
            setActiveAPINav(visible[0].target.id);
            return;
          }

          const current = apiSections
            .filter(function (heading) {
              return heading.getBoundingClientRect().top <= 140;
            })
            .pop();
          if (current) {
            setActiveAPINav(current.id);
          }
        },
        {
          rootMargin: "-96px 0px -62% 0px",
          threshold: [0, 1],
        }
      );

      apiSections.forEach(function (heading) {
        observer.observe(heading);
      });
    }

    window.addEventListener("hashchange", function () {
      if (window.location.hash) {
        setActiveAPINav(window.location.hash.slice(1));
      }
    });
  }
})();
