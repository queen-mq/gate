/** @type {import('tailwindcss').Config} */

// Every colour resolves through a CSS variable declared in style.css, so light
// and dark are one class on <html> and no component knows which it is in.
//
// Two conventions carried over from the relay console, both learned the hard
// way:
//  * no opacity modifiers on these tokens (`bg-good/30`) — Tailwind cannot
//    compute alpha on a var() colour and silently emits nothing;
//  * structural sizes (sidebar, control heights) are px literals in the
//    markup, so nothing about the layout depends on the root font size.
const token = (name) => `var(--${name})`

export default {
  darkMode: 'class',
  content: ['./index.html', './src/**/*.{vue,js}'],
  theme: {
    extend: {
      colors: {
        bg: token('bg'),
        surface: token('surface'),
        'surface-2': token('surface-2'),
        fg: token('text'),
        'fg-2': token('text-2'),
        'fg-3': token('text-3'),
        line: token('border'),
        'line-2': token('border-2'),
        link: token('link'),
        good: token('good'),
        'good-dim': token('good-dim'),
        warn: token('warn'),
        'warn-dim': token('warn-dim'),
        bad: token('bad'),
        'bad-dim': token('bad-dim'),
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      opacity: {
        88: '0.88',
      },
      transitionTimingFunction: {
        spring: 'cubic-bezier(0.16, 1, 0.3, 1)',
      },
      keyframes: {
        in: { from: { opacity: '0', transform: 'translateY(4px)' }, to: { opacity: '1', transform: 'none' } },
        shimmer: { '0%': { backgroundPosition: '100% 50%' }, '100%': { backgroundPosition: '0 50%' } },
        pulse2: { '0%, 100%': { opacity: '1' }, '50%': { opacity: '0.35' } },
      },
      animation: {
        in: 'in 0.2s cubic-bezier(0.16, 1, 0.3, 1) both',
        shimmer: 'shimmer 1.4s ease infinite',
        pulse2: 'pulse2 2.4s ease-in-out infinite',
      },
    },
  },
  plugins: [],
}
