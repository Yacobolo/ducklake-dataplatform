import { LitElement, css, html } from "lit";

class DuckHomeHero extends LitElement {
  static properties = {
    displayName: { attribute: "display-name", type: String },
  };

  static styles = css`
    :host {
      display: block;
      color: var(--fgColor-default);
      background: transparent;
      --hero-text: var(--fgColor-default);
      --hero-muted: var(--fgColor-muted);
      --hero-accent: var(--fgColor-accent);
      --hero-surface: var(--bgColor-default);
      --hero-surface-muted: var(--bgColor-muted);
      --hero-border: var(--borderColor-default);
      --hero-border-strong: var(--borderColor-accent-emphasis);
      --hero-attention: var(--bgColor-attention-emphasis);
      --hero-danger-soft: var(--bgColor-danger-muted);
      --hero-meta-bg: color-mix(in srgb, var(--hero-surface) 76%, transparent);
      --hero-meta-dot: color-mix(in srgb, var(--hero-accent) 58%, var(--hero-surface-muted) 42%);
      --hero-wave-1: color-mix(in srgb, var(--hero-accent) 10%, var(--hero-surface) 90%);
      --hero-wave-2: color-mix(in srgb, var(--hero-accent) 18%, var(--hero-surface-muted) 82%);
      --hero-wave-3: color-mix(in srgb, var(--hero-accent) 28%, var(--hero-surface-muted) 72%);
      --hero-quack-shadow: color-mix(in srgb, var(--hero-text) 12%, transparent);
      --hero-light: color-mix(in srgb, var(--hero-surface) 72%, var(--hero-text) 28%);
      --hero-light-strong: color-mix(in srgb, var(--hero-surface) 88%, var(--hero-text) 12%);
      --hero-light-soft: color-mix(in srgb, var(--hero-surface) 56%, transparent);
      --hero-quack-feather-top: color-mix(in srgb, var(--display-yellow-bgColor-muted) 74%, var(--hero-light-strong) 26%);
      --hero-quack-feather-mid: color-mix(in srgb, var(--display-yellow-bgColor-muted) 44%, var(--display-yellow-fgColor) 56%);
      --hero-quack-feather-low: color-mix(in srgb, var(--display-yellow-fgColor) 82%, var(--display-yellow-bgColor-muted) 18%);
      --hero-quack-highlight: color-mix(in srgb, var(--hero-light) 75%, transparent);
      --hero-quack-shade: color-mix(in srgb, var(--hero-border) 44%, transparent);
      --hero-beak-top: color-mix(in srgb, var(--hero-attention) 78%, var(--hero-light-strong) 22%);
      --hero-beak-bottom: color-mix(in srgb, var(--hero-attention) 88%, var(--hero-text) 12%);
      --hero-eye-white: color-mix(in srgb, white 92%, var(--hero-surface) 8%);
      --hero-eye-border: color-mix(in srgb, var(--hero-border) 72%, transparent);
      --hero-pupil: color-mix(in srgb, black 88%, var(--hero-surface) 12%);
      --hero-cheek: color-mix(in srgb, var(--hero-danger-soft) 48%, transparent);
      --hero-waterline: color-mix(in srgb, var(--hero-light-soft) 42%, var(--hero-surface) 58%);
    }

    * {
      box-sizing: border-box;
    }

    .hero {
      position: relative;
      min-height: 20rem;
      padding: clamp(0.5rem, 1vw, 0.9rem) 0 0;
      overflow: hidden;
    }

    .copy {
      position: relative;
      z-index: 3;
      display: grid;
      gap: 0.55rem;
      max-width: 36rem;
      padding-top: 0.35rem;
      padding-left: clamp(0rem, 1vw, 0.5rem);
    }

    .eyebrow {
      margin: 0;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      color: var(--hero-accent);
    }

    h2 {
      margin: 0;
      font-size: clamp(2rem, 4.2vw, 3.9rem);
      line-height: 0.94;
      letter-spacing: -0.065em;
      font-weight: 700;
      text-wrap: balance;
    }

    p {
      margin: 0;
    }

    .scene {
      position: absolute;
      inset: 0 -3% 0 -3%;
      overflow: hidden;
      mask-image: linear-gradient(180deg, transparent 0%, black 12%, black 100%);
    }

    .quack {
      position: absolute;
      right: 10%;
      bottom: 3.1rem;
      width: min(18rem, 92%);
      aspect-ratio: 1.1;
      transform:
        translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 2px), 0)
        rotate(calc(var(--quack-tilt, 0deg) + 0deg));
      transition: transform 220ms cubic-bezier(0.22, 1, 0.36, 1);
      animation: quack-bob 4.6s ease-in-out infinite;
      transform-origin: center 72%;
      will-change: transform;
    }

    .quack-shadow {
      position: absolute;
      left: 29%;
      bottom: 1rem;
      width: 42%;
      height: 1rem;
      border-radius: 50%;
      background: var(--hero-quack-shadow);
      filter: blur(10px);
      animation: shadow-sway 4.6s ease-in-out infinite;
    }

    .quack-body {
      position: absolute;
      left: 20%;
      bottom: 1.2rem;
      width: 58%;
      height: 38%;
      border-radius: 54% 46% 48% 52% / 58% 56% 44% 42%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      box-shadow:
        inset 0 -0.8rem 0 var(--hero-quack-shade),
        inset 0 0.2rem 0 var(--hero-quack-highlight);
    }

    .quack-tail {
      position: absolute;
      left: -2%;
      top: 34%;
      width: 22%;
      height: 22%;
      border-radius: 30% 70% 30% 70%;
      background: var(--hero-quack-feather-top);
      transform: rotate(-24deg);
    }

    .quack-wing {
      position: absolute;
      left: 31%;
      top: 18%;
      width: 34%;
      height: 42%;
      border-radius: 50% 50% 48% 52% / 56% 54% 46% 44%;
      background: linear-gradient(180deg, var(--hero-quack-feather-mid) 0%, var(--hero-quack-feather-low) 100%);
      transform: rotate(-12deg);
      box-shadow: inset 0 0.12rem 0 color-mix(in srgb, var(--hero-light-soft) 36%, transparent);
    }

    .quack-neck {
      position: absolute;
      right: 22%;
      bottom: 44%;
      width: 20%;
      height: 28%;
      border-radius: 48% 52% 46% 54% / 20% 20% 80% 80%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      transform: rotate(-10deg);
      transform-origin: bottom center;
    }

    .quack-head {
      position: absolute;
      right: 7%;
      bottom: 59%;
      width: 30%;
      height: 26%;
      border-radius: 52% 48% 50% 50% / 48% 48% 52% 52%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      box-shadow: inset 0 0.14rem 0 color-mix(in srgb, var(--hero-light) 58%, transparent);
    }

    .quack-head::after {
      content: "";
      position: absolute;
      left: 70%;
      bottom: 16%;
      width: 44%;
      height: 28%;
      border-radius: 48% 52% 54% 46% / 50% 50% 50% 50%;
      background: linear-gradient(180deg, var(--hero-beak-top) 0%, var(--hero-beak-bottom) 100%);
      transform: rotate(4deg);
      box-shadow: inset 0 -0.1rem 0 color-mix(in srgb, var(--hero-text) 18%, transparent);
    }

    .quack-eye {
      position: absolute;
      top: 34%;
      width: 0.95rem;
      height: 0.95rem;
      border-radius: 50%;
      background: var(--hero-eye-white);
      box-shadow: inset 0 0 0 1px var(--hero-eye-border);
      overflow: hidden;
    }

    .quack-eye.left {
      left: 34%;
    }

    .quack-eye.right {
      left: 54%;
    }

    .quack-pupil {
      position: absolute;
      left: 50%;
      top: 50%;
      width: 0.38rem;
      height: 0.38rem;
      border-radius: 50%;
      background: var(--hero-pupil);
      transform: translate(calc(-50% + var(--pupil-x, 0px)), calc(-50% + var(--pupil-y, 0px)));
      transition: transform 70ms linear;
    }

    .quack-cheek {
      position: absolute;
      left: 22%;
      bottom: 18%;
      width: 16%;
      height: 12%;
      border-radius: 50%;
      background: var(--hero-cheek);
      filter: blur(4px);
    }

    .quack-waterline {
      position: absolute;
      left: 22%;
      bottom: 3.3rem;
      width: 54%;
      height: 0.28rem;
      border-radius: 999px;
      background: var(--hero-waterline);
      opacity: 0.8;
      filter: blur(1px);
    }

    .waves {
      position: absolute;
      left: -2%;
      right: -2%;
      bottom: 0;
      width: 104%;
      height: 10.75rem;
      pointer-events: none;
    }

    .wave {
      animation: wave-drift 13s cubic-bezier(0.55, 0.5, 0.45, 0.5) infinite alternate;
      transform-origin: center;
    }

    .wave-2 {
      fill: var(--hero-wave-2);
      animation-duration: 17s;
      animation-delay: -3s;
    }

    .wave-3 {
      fill: var(--hero-wave-3);
      animation-duration: 22s;
      animation-delay: -5s;
    }

    .wave-1 {
      fill: var(--hero-wave-1);
    }

    @keyframes quack-bob {
      0%,
      100% {
        transform:
          translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 2px), 0)
          rotate(calc(var(--quack-tilt, 0deg) - 0.6deg));
      }
      50% {
        transform:
          translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 12px), 0)
          rotate(calc(var(--quack-tilt, 0deg) + 0.7deg));
      }
    }

    @keyframes shadow-sway {
      0%,
      100% {
        transform: scaleX(0.96);
        opacity: 0.22;
      }
      50% {
        transform: scaleX(1.06);
        opacity: 0.13;
      }
    }

    @keyframes wave-drift {
      0% {
        transform: translate3d(-1.8%, 0.3rem, 0);
      }
      100% {
        transform: translate3d(1.8%, -0.3rem, 0);
      }
    }

    @media (max-width: 56rem) {
      .hero {
        min-height: 17.5rem;
      }

      .copy {
        max-width: 28rem;
        padding-top: 0.25rem;
      }

      .quack {
        right: 50%;
        width: min(15rem, 76%);
        transform: translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 2px), 0) rotate(var(--quack-tilt, 0deg));
      }

      @keyframes quack-bob {
        0%,
        100% {
          transform:
            translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 2px), 0)
            rotate(calc(var(--quack-tilt, 0deg) - 0.6deg));
        }
        50% {
          transform:
            translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 12px), 0)
            rotate(calc(var(--quack-tilt, 0deg) + 0.7deg));
        }
      }
    }

    @media (max-width: 42rem) {
      .hero {
        min-height: 16rem;
      }

      .scene {
        inset: 0 -8% 0 -8%;
      }
    }

    @media (prefers-reduced-motion: reduce) {
      .quack,
      .quack-shadow,
      .wave {
        animation: none;
        transition: none;
      }
    }
  `;

  displayName = "";

  connectedCallback() {
    super.connectedCallback();
    window.addEventListener("pointermove", this.#handleWindowPointerMove);
    window.addEventListener("pointerleave", this.#handleWindowPointerLeave);
    window.addEventListener("blur", this.#handleWindowPointerLeave);
  }

  disconnectedCallback() {
    window.removeEventListener("pointermove", this.#handleWindowPointerMove);
    window.removeEventListener("pointerleave", this.#handleWindowPointerLeave);
    window.removeEventListener("blur", this.#handleWindowPointerLeave);
    super.disconnectedCallback();
  }

  render() {
    const displayName = this.displayName?.trim() || "there";

    return html`
      <section class="hero" aria-label="Welcome home">
        <div class="copy">
          <p class="eyebrow">Welcome back</p>
          <h2>${displayName}</h2>
        </div>

        <div class="scene" aria-hidden="true">
          <div class="quack">
            <div class="quack-shadow"></div>
            <div class="quack-waterline"></div>
            <div class="quack-body">
              <div class="quack-tail"></div>
              <div class="quack-wing"></div>
              <div class="quack-neck"></div>
              <div class="quack-head">
                <div class="quack-cheek"></div>
                <div class="quack-eye left"><span class="quack-pupil"></span></div>
                <div class="quack-eye right"><span class="quack-pupil"></span></div>
              </div>
            </div>
          </div>

          <svg class="waves" viewBox="0 0 1200 220" preserveAspectRatio="none">
            <path
              class="wave wave-1"
              d="M0 118C96 86 187 78 284 93C381 108 474 145 565 146C665 147 753 104 850 97C966 88 1065 121 1200 162V220H0Z"
            ></path>
            <path
              class="wave wave-2"
              d="M0 142C115 168 214 171 306 150C410 126 489 84 583 88C676 92 741 133 841 145C960 160 1066 133 1200 103V220H0Z"
            ></path>
            <path
              class="wave wave-3"
              d="M0 176C111 150 213 135 317 142C431 149 519 196 631 198C742 200 819 156 923 146C1028 136 1115 149 1200 182V220H0Z"
            ></path>
          </svg>
        </div>
      </section>
    `;
  }

  #handleWindowPointerMove = (event: PointerEvent) => {
    const hero = this.renderRoot.querySelector<HTMLElement>(".hero");
    if (!hero) {
      return;
    }

    const rect = hero.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      return;
    }

    const offsetX = (event.clientX - rect.left) / rect.width - 0.5;
    const offsetY = (event.clientY - rect.top) / rect.height - 0.5;

    const pupilX = Math.max(-3, Math.min(3, offsetX * 7));
    const pupilY = Math.max(-2, Math.min(2, offsetY * 5));
    const duckShiftX = offsetX * 12;
    const duckShiftY = offsetY * 7;
    const duckTilt = offsetX * 4;

    hero.style.setProperty("--pupil-x", `${pupilX}px`);
    hero.style.setProperty("--pupil-y", `${pupilY}px`);
    hero.style.setProperty("--quack-shift-x", `${duckShiftX}px`);
    hero.style.setProperty("--quack-shift-y", `${duckShiftY}px`);
    hero.style.setProperty("--quack-tilt", `${duckTilt}deg`);
  };

  #handleWindowPointerLeave = () => {
    const hero = this.renderRoot.querySelector<HTMLElement>(".hero");
    if (!hero) {
      return;
    }

    hero.style.setProperty("--pupil-x", "0px");
    hero.style.setProperty("--pupil-y", "0px");
    hero.style.setProperty("--quack-shift-x", "0px");
    hero.style.setProperty("--quack-shift-y", "0px");
    hero.style.setProperty("--quack-tilt", "0deg");
  };
}

if (!customElements.get("quack-home-hero")) {
  customElements.define("quack-home-hero", DuckHomeHero);
}
