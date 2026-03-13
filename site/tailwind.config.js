module.exports = {
  content: [
    "./content/**/*.{md,html}",
    "./templates/**/*.{html,tmpl}",
    "./assets/js/**/*.js"
  ],
  theme: {
    extend: {
      colors: {
        brand: "var(--site-brand)",
        surface: "var(--site-surface)",
        border: "var(--site-border)",
        ink: "var(--site-ink)",
        muted: "var(--site-muted)"
      },
      boxShadow: {
        card: "0 20px 60px rgba(15, 23, 42, 0.08)"
      }
    }
  },
  plugins: []
};
