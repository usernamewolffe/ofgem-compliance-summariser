/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./api/**/*.py",
    "./summariser/templates/**/*.{html,htm,jinja,jinja2}"
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          50:"#eef8f2",100:"#d8efe3",200:"#c0e6d4",300:"#9fdbbf",
          400:"#6fcca1",500:"#34b68a",600:"#0ea56f",700:"#0b7f56",
          800:"#0a6445",900:"#064a34"
        }
      },
      borderRadius: { '2xl': '1rem' }
    }
  },
  plugins: []
}
