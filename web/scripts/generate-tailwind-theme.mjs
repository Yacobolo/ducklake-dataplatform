import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const webDir = join(__dirname, "..");

const outputFile = join(webDir, "styles", "tokens", "tailwind-theme.generated.css");

const importPaths = [
  "../../tokens/base/size/size.css",
  "../../tokens/base/motion/motion.css",
  "../../tokens/base/typography/typography.css",
  "../../tokens/functional/size/border.css",
  "../../tokens/functional/size/breakpoints.css",
  "../../tokens/functional/size/radius.css",
  "../../tokens/functional/size/size.css",
  "../../tokens/functional/typography/typography.css",
  "../../tokens/functional/themes/light.css",
  "../../tokens/functional/themes/dark.css",
];

const tokenFiles = {
  baseSize: join(webDir, "tokens", "base", "size", "size.css"),
  baseMotion: join(webDir, "tokens", "base", "motion", "motion.css"),
  baseTypography: join(webDir, "tokens", "base", "typography", "typography.css"),
  functionalBorder: join(webDir, "tokens", "functional", "size", "border.css"),
  functionalBreakpoints: join(webDir, "tokens", "functional", "size", "breakpoints.css"),
  functionalRadius: join(webDir, "tokens", "functional", "size", "radius.css"),
  functionalSize: join(webDir, "tokens", "functional", "size", "size.css"),
  functionalTypography: join(webDir, "tokens", "functional", "typography", "typography.css"),
  lightTheme: join(webDir, "tokens", "functional", "themes", "light.css"),
  darkTheme: join(webDir, "tokens", "functional", "themes", "dark.css"),
};

function extractVars(filePath) {
  const text = readFileSync(filePath, "utf8");
  return [...text.matchAll(/--([A-Za-z0-9-]+)\s*:\s*([^;]+);/g)].map(([, name, value]) => ({
    name,
    value: value.trim(),
  }));
}

function normalizeTokenName(name) {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
    .toLowerCase();
}

function cssValueFor(name) {
  return `var(--${name})`;
}

function uniqueByName(tokens) {
  return [...new Map(tokens.map((token) => [token.name, token])).values()];
}

function formatNamespace(namespace, tokens) {
  if (!tokens.length) {
    return "";
  }

  return tokens
    .map((token) => `  --${namespace}-${normalizeTokenName(token.name)}: ${cssValueFor(token.name)};`)
    .join("\n");
}

function isThemeShadow(token) {
  return token.name.includes("shadow") || /\b(inset|0px|px)\b/.test(token.value) && token.value.includes(",");
}

function isThemeColor(token) {
  if (token.name.includes("shadow")) {
    return false;
  }
  if (token.name.startsWith("border-") || token.name === "focus-outline") {
    return false;
  }
  return true;
}

const themeVars = uniqueByName([
  ...extractVars(tokenFiles.lightTheme),
  ...extractVars(tokenFiles.darkTheme),
]);

const colorVars = themeVars.filter(isThemeColor);
const shadowVars = themeVars.filter(isThemeShadow);
const spacingVars = uniqueByName([
  ...extractVars(tokenFiles.baseSize),
  ...extractVars(tokenFiles.functionalSize),
]);
const radiusVars = extractVars(tokenFiles.functionalRadius);
const breakpointVars = extractVars(tokenFiles.functionalBreakpoints);
const fontVars = uniqueByName(
  extractVars(tokenFiles.functionalTypography).filter((token) => token.name.startsWith("fontStack-")),
);
const textSizeVars = uniqueByName([
  ...extractVars(tokenFiles.baseTypography).filter((token) => token.name.includes("-size-")),
  ...extractVars(tokenFiles.functionalTypography).filter((token) => token.name.endsWith("-size")),
]);
const leadingVars = uniqueByName([
  ...extractVars(tokenFiles.baseTypography).filter((token) => token.name.includes("lineHeight")),
  ...extractVars(tokenFiles.functionalTypography).filter((token) => token.name.includes("lineHeight")),
]);
const fontWeightVars = uniqueByName([
  ...extractVars(tokenFiles.baseTypography).filter((token) => token.name.includes("weight")),
  ...extractVars(tokenFiles.functionalTypography).filter((token) => token.name.includes("weight")),
]);
const easeVars = extractVars(tokenFiles.baseMotion).filter((token) => token.name.startsWith("base-easing-"));
const durationVars = extractVars(tokenFiles.baseMotion).filter((token) => token.name.startsWith("base-duration-"));
const borderShadowVars = extractVars(tokenFiles.functionalBorder).filter((token) => token.name.startsWith("boxShadow-"));

const sections = [
  formatNamespace("color", colorVars),
  formatNamespace("shadow", uniqueByName([...shadowVars, ...borderShadowVars])),
  formatNamespace("spacing", spacingVars),
  formatNamespace("radius", radiusVars),
  formatNamespace("breakpoint", breakpointVars),
  formatNamespace("font", fontVars),
  formatNamespace("text", textSizeVars),
  formatNamespace("leading", leadingVars),
  formatNamespace("font-weight", fontWeightVars),
  formatNamespace("ease", easeVars),
  formatNamespace("duration", durationVars),
].filter(Boolean);

const contents = `/* eslint-disable */
/* This file is generated by ./scripts/generate-tailwind-theme.mjs. */
${importPaths.map((path) => `@import "${path}";`).join("\n")}

@theme static {
${sections.join("\n")}
}
`;

writeFileSync(outputFile, contents);
console.log(`Generated ${outputFile}`);
