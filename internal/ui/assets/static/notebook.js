(() => {
  const workspace = document.querySelector("[data-reorder-url]");
  const reorderURL = workspace instanceof HTMLElement ? workspace.dataset.reorderUrl || "" : "";

  const cells = () => Array.from(document.querySelectorAll("[data-notebook-cell='true']"));
  const csrf = () => {
    const input = document.querySelector("input[name='csrf_token']");
    return input instanceof HTMLInputElement ? input.value : "";
  };

  const activeCell = () => {
    const focused = document.activeElement;
    if (!focused) {
      return null;
    }
    return focused.closest("[data-notebook-cell='true']");
  };

  const focusCellEditor = (cell) => {
    if (!cell) {
      return;
    }
    const editor = cell.querySelector("[data-cell-editor='true']");
    if (editor instanceof HTMLElement) {
      editor.focus();
      const end = editor.value.length;
      if (typeof editor.setSelectionRange === "function") {
        editor.setSelectionRange(end, end);
      }
    }
  };

  const moveFocus = (delta) => {
    const list = cells();
    const current = activeCell();
    if (!current || list.length === 0) {
      return;
    }
    const idx = list.indexOf(current);
    if (idx === -1) {
      return;
    }
    const next = list[idx + delta];
    if (next) {
      focusCellEditor(next);
    }
  };

  document.addEventListener("keydown", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const isEditor = target.matches("textarea, input, select");
    const current = activeCell();

    if (event.key === "Enter" && event.shiftKey && current) {
      const runButton = current.querySelector("[data-run-cell='true']");
      if (runButton instanceof HTMLButtonElement) {
        event.preventDefault();
        runButton.click();
      }
      return;
    }

    if (!isEditor || !current) {
      return;
    }

    if (event.key === "j" && !event.metaKey && !event.ctrlKey && !event.altKey) {
      event.preventDefault();
      moveFocus(1);
      return;
    }

    if (event.key === "k" && !event.metaKey && !event.ctrlKey && !event.altKey) {
      event.preventDefault();
      moveFocus(-1);
      return;
    }

    if (event.key === "a" && !event.metaKey && !event.ctrlKey && !event.altKey) {
      const addAbove = current.querySelector("[data-add-above='true']");
      if (addAbove instanceof HTMLButtonElement) {
        event.preventDefault();
        addAbove.click();
      }
      return;
    }

    if (event.key === "b" && !event.metaKey && !event.ctrlKey && !event.altKey) {
      const addBelow = current.querySelector("[data-add-below='true']");
      if (addBelow instanceof HTMLButtonElement) {
        event.preventDefault();
        addBelow.click();
      }
    }
  });

  if (!reorderURL) {
    return;
  }

  let draggedCell = null;

  const submitOrder = async () => {
    const ordered = cells();
    const params = new URLSearchParams();
    ordered.forEach((cell) => {
      const id = cell.getAttribute("data-cell-id");
      if (id) {
        params.append("cell_ids", id);
      }
    });
    const token = csrf();
    if (token) {
      params.set("csrf_token", token);
    }

    const response = await fetch(reorderURL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      body: params.toString()
    });
    if (!response.ok) {
      window.location.reload();
    }
  };

  cells().forEach((cell) => {
    if (!(cell instanceof HTMLElement)) {
      return;
    }
    cell.draggable = true;

    cell.addEventListener("dragstart", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || !target.closest("[data-drag-handle='true']")) {
        event.preventDefault();
        return;
      }
      draggedCell = cell;
      cell.classList.add("dragging");
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = "move";
      }
    });

    cell.addEventListener("dragend", () => {
      cell.classList.remove("dragging");
      cells().forEach((n) => n.classList.remove("drag-over"));
      draggedCell = null;
    });

    cell.addEventListener("dragover", (event) => {
      if (!draggedCell || draggedCell === cell) {
        return;
      }
      event.preventDefault();
      cell.classList.add("drag-over");
    });

    cell.addEventListener("dragleave", () => {
      cell.classList.remove("drag-over");
    });

    cell.addEventListener("drop", async (event) => {
      if (!draggedCell || draggedCell === cell) {
        return;
      }
      event.preventDefault();
      cell.classList.remove("drag-over");

      const parent = cell.parentElement;
      if (!parent) {
        return;
      }

      const rect = cell.getBoundingClientRect();
      const after = event.clientY > rect.top + rect.height / 2;
      if (after) {
        parent.insertBefore(draggedCell, cell.nextSibling);
      } else {
        parent.insertBefore(draggedCell, cell);
      }

      await submitOrder();
    });
  });
})();
