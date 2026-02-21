import { LitElement, css, html } from "lit";
import hljs from "highlight.js/lib/core";
import sql from "highlight.js/lib/languages/sql";
import { format as formatSQL } from "sql-formatter";

hljs.registerLanguage("sql", sql);

class SqlEditorSurface extends LitElement {
  static properties = {
    lineCount: { state: true },
    activeLine: { state: true },
    lineHeight: { state: true },
    activeTop: { state: true },
  };

  static styles = css`
    :host {
      position: relative;
      display: grid;
      grid-template-columns: calc(var(--control-small-size) + var(--space-3)) minmax(0, 1fr);
      min-height: 0;
      height: 100%;
      flex: 1;
      background: linear-gradient(180deg, var(--bgColor-default), var(--bgColor-muted));
      overflow: hidden;
    }

    .gutter {
      margin: 0;
      padding: var(--space-2) var(--space-1) var(--space-2) var(--space-2);
      border-right: var(--borderWidth-default) solid var(--borderColor-muted);
      color: var(--fgColor-muted);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      text-align: right;
      white-space: pre;
      user-select: none;
      overflow: hidden;
      height: 100%;
      box-sizing: border-box;
    }

    .input-wrap {
      position: relative;
      min-height: 0;
      height: 100%;
      overflow: hidden;
    }

    .highlight-layer {
      position: absolute;
      inset: 0;
      margin: 0;
      padding: var(--space-2) var(--space-2) var(--space-2) var(--space-2);
      overflow: hidden;
      pointer-events: none;
      white-space: pre;
      color: var(--fgColor-default);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      z-index: 1;
    }

    .highlight-layer code {
      display: block;
      min-height: 100%;
    }

    .highlight-layer code.hljs {
      color: var(--fgColor-default);
      background: var(--bgColor-transparent);
      padding: 0;
    }

    .highlight-layer .hljs-keyword,
    .highlight-layer .hljs-operator,
    .highlight-layer .hljs-selector-tag {
      color: var(--fgColor-accent);
      font-weight: var(--base-text-weight-semibold);
    }

    .highlight-layer .hljs-string,
    .highlight-layer .hljs-quote {
      color: var(--fgColor-success);
    }

    .highlight-layer .hljs-number,
    .highlight-layer .hljs-literal {
      color: var(--fgColor-attention);
    }

    .highlight-layer .hljs-comment {
      color: var(--fgColor-muted);
      font-style: italic;
    }

    .highlight-layer .hljs-built_in,
    .highlight-layer .hljs-type,
    .highlight-layer .hljs-function,
    .highlight-layer .hljs-title {
      color: var(--fgColor-severe);
    }

    .active-line {
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      background: var(--bgColor-accent-muted);
      opacity: 0;
      pointer-events: none;
      transition: opacity var(--base-duration-100) ease;
      z-index: 0;
    }

    .input-wrap:focus-within .active-line {
      opacity: 0.35;
    }

    ::slotted(textarea.sql-editor-textarea) {
      margin: 0;
      border: 0;
      border-radius: 0;
      min-height: 0;
      width: 100%;
      height: 100%;
      max-height: 100%;
      resize: none;
      padding-left: var(--space-2);
      background: var(--bgColor-transparent);
      color: var(--bgColor-transparent);
      caret-color: var(--fgColor-default);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      box-shadow: none;
      position: relative;
      z-index: 2;
      -webkit-text-fill-color: transparent;
    }

    @media (max-width: var(--breakpoint-medium)) {
      :host {
        display: block;
      }

      .gutter,
      .active-line,
      .highlight-layer {
        display: none;
      }

      ::slotted(textarea.sql-editor-textarea) {
        min-height: calc(var(--size-editor-min-height) + var(--overlay-height-small));
        color: var(--fgColor-default);
      }
    }
  `;

  private lineCount = 16;
  private activeLine = 1;
  private lineHeight = 20;
  private activeTop = 0;
  private textarea: HTMLTextAreaElement | null = null;
  private formatButton: HTMLButtonElement | null = null;
  private highlightedHTML = "";

  private readonly onSlotChange = (event: Event) => {
    const target = event.target;
    if (target instanceof HTMLSlotElement) {
      this.attachTextarea(target);
    }
  };

  private readonly onInput = () => {
    this.syncState();
  };

  private readonly onKeyDown = (event: KeyboardEvent) => {
    const isEnter = event.key === "Enter";
    const isMeta = event.metaKey || event.ctrlKey;
    if (isMeta && isEnter) {
      const runButton = document.getElementById("sql-run-query");
      if (runButton instanceof HTMLButtonElement) {
        event.preventDefault();
        runButton.click();
      }
      return;
    }

    if (isMeta && event.shiftKey && event.key.toLowerCase() === "f") {
      event.preventDefault();
      this.formatEditorSQL();
    }
  };

  private readonly onFormatClick = (event: Event) => {
    event.preventDefault();
    this.formatEditorSQL();
  };

  connectedCallback(): void {
    super.connectedCallback();
    window.addEventListener("resize", this.onInput);
  }

  disconnectedCallback(): void {
    window.removeEventListener("resize", this.onInput);
    this.detachFormatButton();
    this.detachTextarea();
    super.disconnectedCallback();
  }

  firstUpdated(): void {
    const slot = this.renderRoot.querySelector("slot");
    if (slot instanceof HTMLSlotElement) {
      slot.addEventListener("slotchange", this.onSlotChange);
      this.attachTextarea(slot);
    }
    this.attachFormatButton();
  }

  render() {
    const lineNumbers = [];
    for (let i = 1; i <= this.lineCount; i += 1) {
      lineNumbers.push(i);
    }

    return html`
      <pre class="gutter" aria-hidden="true">${lineNumbers.join("\n")}</pre>
      <div class="input-wrap">
        <pre class="highlight-layer" aria-hidden="true"><code id="sql-highlight-code"></code></pre>
        <div
          class="active-line"
          aria-hidden="true"
          style=${`height: ${this.lineHeight}px; transform: translateY(${this.activeTop}px);`}
        ></div>
        <slot></slot>
      </div>
    `;
  }

  private attachTextarea(slot: HTMLSlotElement): void {
    const assigned = slot.assignedElements({ flatten: true });
    const textarea = assigned.find((node) => node instanceof HTMLTextAreaElement);
    if (!(textarea instanceof HTMLTextAreaElement)) {
      this.detachTextarea();
      return;
    }
    if (this.textarea === textarea) {
      this.syncState();
      return;
    }

    this.detachTextarea();
    this.textarea = textarea;
    this.textarea.style.backgroundColor = "transparent";
    this.textarea.style.color = "transparent";
    this.textarea.style.webkitTextFillColor = "transparent";
    this.textarea.style.caretColor = "var(--fgColor-default)";
    textarea.addEventListener("input", this.onInput);
    textarea.addEventListener("scroll", this.onInput);
    textarea.addEventListener("keyup", this.onInput);
    textarea.addEventListener("click", this.onInput);
    textarea.addEventListener("select", this.onInput);
    textarea.addEventListener("keydown", this.onKeyDown);
    this.syncState();
  }

  private detachTextarea(): void {
    if (!(this.textarea instanceof HTMLTextAreaElement)) {
      this.textarea = null;
      return;
    }
    this.textarea.removeEventListener("input", this.onInput);
    this.textarea.removeEventListener("scroll", this.onInput);
    this.textarea.removeEventListener("keyup", this.onInput);
    this.textarea.removeEventListener("click", this.onInput);
    this.textarea.removeEventListener("select", this.onInput);
    this.textarea.removeEventListener("keydown", this.onKeyDown);
    this.textarea = null;
  }

  private attachFormatButton(): void {
    const button = document.getElementById("sql-format-query");
    if (!(button instanceof HTMLButtonElement)) {
      this.detachFormatButton();
      return;
    }

    if (this.formatButton === button) {
      return;
    }

    this.detachFormatButton();
    this.formatButton = button;
    this.formatButton.addEventListener("click", this.onFormatClick);
  }

  private detachFormatButton(): void {
    if (!(this.formatButton instanceof HTMLButtonElement)) {
      this.formatButton = null;
      return;
    }
    this.formatButton.removeEventListener("click", this.onFormatClick);
    this.formatButton = null;
  }

  private formatEditorSQL(): void {
    if (!(this.textarea instanceof HTMLTextAreaElement)) {
      return;
    }

    const source = this.textarea.value || "";
    if (source.trim() === "") {
      return;
    }

    try {
      const formatted = formatSQL(source, {
        language: "duckdb",
        keywordCase: "upper",
        indentStyle: "standard",
        tabWidth: 2,
        expressionWidth: 60,
      });
      if (formatted !== source) {
        this.textarea.value = formatted;
        this.syncState();
      }
    } catch {
      // Keep original SQL when formatter fails.
    }
  }

  private syncState(): void {
    if (!(this.textarea instanceof HTMLTextAreaElement)) {
      return;
    }

    const value = this.textarea.value || "";
    const lines = value.split("\n").length;
    this.lineCount = Math.max(16, lines + 2);
    const gutter = this.renderRoot.querySelector(".gutter");
    if (gutter instanceof HTMLElement) {
      const nums = [];
      for (let i = 1; i <= this.lineCount; i += 1) {
        nums.push(String(i));
      }
      gutter.textContent = nums.join("\n");
    }
    this.updateHighlight(value);

    const computed = window.getComputedStyle(this.textarea);
    const lineHeight = Number.parseFloat(computed.lineHeight);
    this.lineHeight = Number.isFinite(lineHeight) && lineHeight > 0 ? lineHeight : 20;

    const paddingTop = Number.parseFloat(computed.paddingTop);
    const topPadding = Number.isFinite(paddingTop) ? paddingTop : 0;

    const caret = this.textarea.selectionStart || 0;
    this.activeLine = value.slice(0, caret).split("\n").length;
    this.activeTop = topPadding + ((this.activeLine - 1) * this.lineHeight) - this.textarea.scrollTop;

    const activeLine = this.renderRoot.querySelector(".active-line");
    if (activeLine instanceof HTMLElement) {
      activeLine.style.height = `${this.lineHeight}px`;
      activeLine.style.transform = `translateY(${this.activeTop}px)`;
    }

    const highlight = this.renderRoot.querySelector(".highlight-layer");
    if (highlight instanceof HTMLElement) {
      highlight.scrollTop = this.textarea.scrollTop;
      highlight.scrollLeft = this.textarea.scrollLeft;
    }

    const gutterScroll = this.renderRoot.querySelector(".gutter");
    if (gutterScroll instanceof HTMLElement) {
      gutterScroll.scrollTop = this.textarea.scrollTop;
    }
  }

  private updateHighlight(source: string): void {
    const codeEl = this.renderRoot.querySelector("#sql-highlight-code");
    if (!(codeEl instanceof HTMLElement)) {
      return;
    }
    const highlighted = hljs.highlight(source, { language: "sql", ignoreIllegals: true }).value;
    const htmlText = `${highlighted}\n`;
    if (htmlText === this.highlightedHTML) {
      return;
    }
    this.highlightedHTML = htmlText;
    codeEl.className = "hljs language-sql";
    codeEl.innerHTML = htmlText;
  }
}

if (!customElements.get("sql-editor-surface")) {
  customElements.define("sql-editor-surface", SqlEditorSurface);
}
