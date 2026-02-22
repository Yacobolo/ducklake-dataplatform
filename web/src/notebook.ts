(() => {
  const workspace = document.querySelector("[data-reorder-url]");
  const reorderURL = workspace instanceof HTMLElement ? workspace.dataset.reorderUrl || "" : "";

  const cells = () => Array.from(document.querySelectorAll<HTMLElement>("[data-notebook-cell='true']"));
  const csrf = () => {
    const input = document.querySelector("input[name='csrf_token']");
    return input instanceof HTMLInputElement ? input.value : "";
  };

  const activeCell = () => {
    const focused = document.activeElement;
    if (!focused) {
      return null;
    }
    return focused.closest<HTMLElement>("[data-notebook-cell='true']");
  };

  const focusCellEditor = (cell: HTMLElement | null) => {
    if (!cell) {
      return;
    }
    const editor = cell.querySelector("[data-cell-editor='true']");
    if (editor instanceof HTMLTextAreaElement || editor instanceof HTMLInputElement) {
      editor.focus();
      const end = editor.value.length;
      if (typeof editor.setSelectionRange === "function") {
        editor.setSelectionRange(end, end);
      }
    }
  };

  const moveFocus = (delta: number) => {
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

  const setActiveCell = (cell: HTMLElement | null) => {
    cells().forEach((item) => {
      if (item === cell) {
        item.classList.add("is-active-cell");
      } else {
        item.classList.remove("is-active-cell");
      }
    });
  };

  const formForCell = (cell: HTMLElement | null) => {
    if (!cell) {
      return null;
    }
    const form = cell.querySelector(".notebook-cell-form");
    return form instanceof HTMLFormElement ? form : null;
  };

  const editorForCell = (cell: HTMLElement | null) => {
    if (!cell) {
      return null;
    }
    const editor = cell.querySelector("[data-cell-editor='true']");
    return editor instanceof HTMLTextAreaElement || editor instanceof HTMLInputElement ? editor : null;
  };

  const encodeForm = (form: HTMLFormElement) => {
    const params = new URLSearchParams();
    const data = new FormData(form);
    for (const [key, value] of data.entries()) {
      params.append(key, String(value));
    }
    return params;
  };

  const syncMarkdownPreviewFromResponse = (cell: HTMLElement, htmlText: string) => {
    const parser = new DOMParser();
    const doc = parser.parseFromString(htmlText, "text/html");
    const id = cell.getAttribute("id");
    if (!id) {
      return;
    }
    const freshPreview = doc.querySelector<HTMLElement>(`#${id} [data-markdown-preview='true']`);
    const currentPreview = cell.querySelector<HTMLElement>("[data-markdown-preview='true']");
    if (freshPreview && currentPreview) {
      currentPreview.innerHTML = freshPreview.innerHTML;
    }
  };

  const saveCell = async (cell: HTMLElement | null) => {
    if (!cell) {
      return;
    }
    const form = formForCell(cell);
    if (!form || form.dataset.dirty !== "true") {
      return;
    }

    const response = await fetch(form.action, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
      body: encodeForm(form).toString(),
    });

    if (!response.ok) {
      window.location.reload();
      return;
    }

    form.dataset.dirty = "false";
    if (cell.dataset.cellType === "markdown") {
      const htmlText = await response.text();
      syncMarkdownPreviewFromResponse(cell, htmlText);
    }
  };

  const enterMarkdownEdit = (cell: HTMLElement | null) => {
    if (!cell || cell.dataset.cellType !== "markdown") {
      return;
    }
    cell.classList.add("is-editing-markdown");
    focusCellEditor(cell);
  };

  const exitMarkdownEdit = (cell: HTMLElement | null) => {
    if (!cell || cell.dataset.cellType !== "markdown") {
      return;
    }
    cell.classList.remove("is-editing-markdown");
    const preview = cell.querySelector<HTMLElement>("[data-markdown-preview='true']");
    preview?.focus();
  };

  document.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !target.matches("[data-cell-editor='true']")) {
      return;
    }
    const cell = target.closest<HTMLElement>("[data-notebook-cell='true']");
    const form = formForCell(cell);
    if (form) {
      form.dataset.dirty = "true";
    }
  });

  document.addEventListener("focusout", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !target.matches("[data-cell-editor='true']")) {
      return;
    }
    const cell = target.closest<HTMLElement>("[data-notebook-cell='true']");
    const next = event.relatedTarget;
    if (next instanceof HTMLElement && cell?.contains(next)) {
      return;
    }
    void saveCell(cell);
  });

  document.addEventListener("focusin", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const cell = target.closest<HTMLElement>("[data-notebook-cell='true']");
    if (cell) {
      setActiveCell(cell);
    }
  });

  document.addEventListener("pointerdown", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const selected = target.closest<HTMLElement>("[data-notebook-cell='true']");
    setActiveCell(selected);
    const editingCells = document.querySelectorAll<HTMLElement>("[data-cell-type='markdown'].is-editing-markdown");
    editingCells.forEach((cell) => {
      if (cell.contains(target)) {
        return;
      }
      void saveCell(cell).finally(() => {
        exitMarkdownEdit(cell);
      });
    });
  });

  document.addEventListener("dblclick", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const preview = target.closest<HTMLElement>("[data-markdown-preview='true']");
    if (!preview) {
      return;
    }
    const cell = preview.closest<HTMLElement>("[data-notebook-cell='true']");
    setActiveCell(cell);
    enterMarkdownEdit(cell);
  });

  document.addEventListener("keydown", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const isEditor = target.matches("textarea, input, select");
    const current = activeCell();

    if (event.key === "Escape" && current?.dataset.cellType === "markdown" && current.classList.contains("is-editing-markdown")) {
      event.preventDefault();
      void saveCell(current).finally(() => {
        exitMarkdownEdit(current);
      });
      return;
    }

    if (event.key === "Enter" && target.matches("[data-markdown-preview='true']")) {
      event.preventDefault();
      const cell = target.closest<HTMLElement>("[data-notebook-cell='true']");
      enterMarkdownEdit(cell);
      return;
    }

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

  let draggedCell: HTMLElement | null = null;

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
      body: params.toString(),
    });
    if (!response.ok) {
      window.location.reload();
    }
  };

  cells().forEach((cell) => {
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
