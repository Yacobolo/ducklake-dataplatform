import { LitElement, css, html } from "lit";

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
      flex: 1;
      background: linear-gradient(180deg, var(--bgColor-default), var(--bgColor-muted));
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
    }

    .input-wrap {
      position: relative;
      min-height: 0;
      overflow: hidden;
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
      z-index: 1;
    }

    .input-wrap:focus-within .active-line {
      opacity: 1;
    }

    ::slotted(textarea.sql-editor-textarea) {
      margin: 0;
      border: 0;
      border-radius: 0;
      min-height: 0;
      width: 100%;
      height: 100%;
      resize: none;
      padding-left: var(--space-2);
      background: var(--bgColor-transparent);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      box-shadow: none;
      position: relative;
      z-index: 2;
    }

    @media (max-width: var(--breakpoint-medium)) {
      :host {
        display: block;
      }

      .gutter,
      .active-line {
        display: none;
      }

      ::slotted(textarea.sql-editor-textarea) {
        min-height: calc(var(--size-editor-min-height) + var(--overlay-height-small));
      }
    }
  `;

  private lineCount = 16;
  private activeLine = 1;
  private lineHeight = 20;
  private activeTop = 0;
  private textarea: HTMLTextAreaElement | null = null;

  private readonly onSlotChange = (event: Event) => {
    const target = event.target;
    if (target instanceof HTMLSlotElement) {
      this.attachTextarea(target);
    }
  };

  private readonly onInput = () => {
    this.syncState();
  };

  connectedCallback(): void {
    super.connectedCallback();
    window.addEventListener("resize", this.onInput);
  }

  disconnectedCallback(): void {
    window.removeEventListener("resize", this.onInput);
    this.detachTextarea();
    super.disconnectedCallback();
  }

  firstUpdated(): void {
    const slot = this.renderRoot.querySelector("slot");
    if (slot instanceof HTMLSlotElement) {
      slot.addEventListener("slotchange", this.onSlotChange);
      this.attachTextarea(slot);
    }
  }

  render() {
    const lineNumbers = [];
    for (let i = 1; i <= this.lineCount; i += 1) {
      lineNumbers.push(i);
    }

    return html`
      <pre class="gutter" aria-hidden="true">${lineNumbers.join("\n")}</pre>
      <div class="input-wrap">
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
    textarea.addEventListener("input", this.onInput);
    textarea.addEventListener("scroll", this.onInput);
    textarea.addEventListener("keyup", this.onInput);
    textarea.addEventListener("click", this.onInput);
    textarea.addEventListener("select", this.onInput);
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
    this.textarea = null;
  }

  private syncState(): void {
    if (!(this.textarea instanceof HTMLTextAreaElement)) {
      return;
    }

    const value = this.textarea.value || "";
    const lines = value.split("\n").length;
    this.lineCount = Math.max(16, lines + 2);

    const computed = window.getComputedStyle(this.textarea);
    const lineHeight = Number.parseFloat(computed.lineHeight);
    this.lineHeight = Number.isFinite(lineHeight) && lineHeight > 0 ? lineHeight : 20;

    const paddingTop = Number.parseFloat(computed.paddingTop);
    const topPadding = Number.isFinite(paddingTop) ? paddingTop : 0;

    const caret = this.textarea.selectionStart || 0;
    this.activeLine = value.slice(0, caret).split("\n").length;
    this.activeTop = topPadding + ((this.activeLine - 1) * this.lineHeight) - this.textarea.scrollTop;
  }
}

if (!customElements.get("sql-editor-surface")) {
  customElements.define("sql-editor-surface", SqlEditorSurface);
}
