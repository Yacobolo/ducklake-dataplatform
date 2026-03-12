(function () {
  const root = document.documentElement;
  const key = "duck-site-theme";
  const navButton = document.querySelector("[data-site-nav-toggle]");
  const nav = document.querySelector("[data-site-mobile-nav]");
  const themeButton = document.querySelector("[data-site-theme-toggle]");
  const searchInput = document.querySelector("[data-site-search-input]");
  const searchResults = document.querySelector("[data-site-search-results]");

  function applyTheme(theme) {
    root.setAttribute("data-theme", theme);
    if (themeButton) {
      themeButton.textContent = theme === "dark" ? "Light" : "Dark";
    }
  }

  let theme = "light";
  try {
    theme = localStorage.getItem(key) || theme;
  } catch (_) {}
  applyTheme(theme);

  if (themeButton) {
    themeButton.addEventListener("click", function () {
      theme = theme === "dark" ? "light" : "dark";
      try {
        localStorage.setItem(key, theme);
      } catch (_) {}
      applyTheme(theme);
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
})();
