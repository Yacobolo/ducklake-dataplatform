module.exports = {
  content: [
    "./content/**/*.{md,html}",
    "./templates/**/*.{html,tmpl}",
    "./assets/js/**/*.js"
  ],
  theme: {
    extend: {}
  },
  plugins: [require("@tailwindcss/typography")]
};
