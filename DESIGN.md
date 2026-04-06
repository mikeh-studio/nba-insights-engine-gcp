# Design System — NBA Fantasy Research Workbench

## Product Context
- **What this is:** A public NBA research workbench for stats-focused fantasy fans. It helps users discover who is rising or falling, inspect why a player matters, compare two players quickly, and track a short list of players without digging through raw box scores.
- **Who it's for:** NBA fans who care about fantasy-relevant trends, recent form, and short-window decision-making more than season-long reference tables.
- **Space/industry:** Sports analytics, fantasy basketball, public stats products.
- **Project type:** Web app / dashboard / research workbench.

## Aesthetic Direction
- **Direction:** Broadcast performance desk
- **Decoration level:** Minimal but forceful
- **Mood:** The product should feel sharp, fast, and athletic. It should read like a premium studio wall or scoreboard desk, not a warm editorial magazine and not a generic SaaS analytics dashboard.
- **Reference sites:** `https://www.basketball-reference.com/`, `https://www.statmuse.com/nba`, `https://www.fantasypros.com/nba/`

## Typography
- **Display/Hero:** `Bebas Neue` — for major anchors, hero moments, and high-signal section headers. It brings sports energy and decisiveness, but must be used sparingly.
- **Body:** `Inter Tight` — compact, modern, and readable under dense data conditions.
- **UI/Labels:** `Inter Tight` — same as body for controls, labels, and small supporting copy.
- **Data/Tables:** `IBM Plex Mono` — for ranks, stat rows, timestamps, compare windows, and any dense numeric context. Use tabular numerals where supported.
- **Code:** `IBM Plex Mono`
- **Loading:** Google Fonts for `Bebas Neue`, `Inter Tight`, and `IBM Plex Mono` during prototyping. If the product hardens for production, self-host the chosen weights.
- **Scale:**
  - `display-xl`: `7rem / 112px`
  - `display-lg`: `5.5rem / 88px`
  - `h1`: `3.25rem / 52px`
  - `h2`: `2.375rem / 38px`
  - `h3`: `1.5rem / 24px`
  - `body-lg`: `1.0625rem / 17px`
  - `body`: `0.9375rem / 15px`
  - `body-sm`: `0.875rem / 14px`
  - `label`: `0.75rem / 12px`
  - `mono-sm`: `0.75rem / 12px`

## Color
- **Approach:** Restrained high-contrast dark system with one performance accent.
- **Primary:** `#F04E23` — kinetic red-orange for key actions, highlights, and high-signal moments.
- **Secondary:** `#FF6A3D` — hotter accent variant for emphasis, hover energy, and key visual anchors.
- **Neutrals:**
  - `#0F1113` background
  - `#171A1E` primary surface
  - `#1E2329` elevated surface
  - `#252C33` support surface
  - `#2B3138` line / dividers
  - `#A7AFB7` muted copy
  - `#E7E0D4` highlight neutral
  - `#F3F1EC` primary ink
- **Semantic:** success `#5DD39E`, warning `#F04E23`, error `#FF6B6B`, info `#E7E0D4`
- **Dark mode:** This system is dark-mode first. If a light mode is ever added, it should be a deliberate companion system, not a naive inversion. Reduce accent saturation by about 10-15% on large fields and reserve the hottest accent for focused actions only.

## Spacing
- **Base unit:** `8px`
- **Density:** Compact-to-medium. Tight enough for sports analysis, but not cramped.
- **Scale:** `2xs(2) xs(4) sm(8) md(16) lg(24) xl(32) 2xl(48) 3xl(64)`

## Layout
- **Approach:** Structured workspace
- **Grid:**
  - mobile: 4 columns
  - tablet: 8 columns
  - desktop: 12 columns
- **Max content width:** `1340px`
- **Border radius:** `sm: 10px`, `md: 14px`, `lg: 20px`, `xl: 28px`, `pill: 9999px`
- **Composition rules:**
  - The home screen is discovery-led, not rankings-led.
  - The first screen should have one dominant signal board, not a mosaic of equal cards.
  - Compare is a next move from player detail, not a co-equal landing page.
  - Tracking is a secondary rail, never the first thing new users see.
  - Favor a primary analysis column with secondary support rails over equal-width panel grids.

## Motion
- **Approach:** Fast and restrained
- **Easing:** enter(`ease-out`), exit(`ease-in`), move(`ease-in-out`)
- **Duration:** micro(`50-100ms`), short(`150-220ms`), medium(`240-320ms`), long(`360-500ms`)
- **Rules:**
  - Motion should clarify state changes and hierarchy, not decorate empty space.
  - No floaty motion, looping ambient effects, or soft ornamental reveals.
  - One stronger entrance treatment is allowed for the top signal board; everything else should stay crisp and functional.

## Interaction Principles
- Trust states must be obvious. Use prominent inline warnings for stale data instead of tiny passive badges.
- Empty states must explain why a panel is empty and point to a useful next action nearby.
- Partial compare states must preserve the two-column compare layout and visibly mark the limited side.
- The product is utility-first: keep the orientation header compact, then move straight into high-signal content.

## Anti-Patterns
- Do not drift into purple/indigo gradients or generic sports-app red-on-black clichés.
- Do not build the app as stacked equal-weight cards.
- Do not center everything.
- Do not use decorative icon circles, bubbly radii everywhere, or feature-grid SaaS sections.
- Do not soften the product back into a warm editorial magazine tone for core workbench surfaces.

## Implementation Notes
- Define all colors as CSS custom properties before building components.
- Use `IBM Plex Mono` for freshness, ranks, timestamps, and dense stat comparisons.
- Reserve `Bebas Neue` for major anchors only. Body copy and labels stay in `Inter Tight`.
- Keep chart containers, compare panels, and trust states visually integrated with the same surface system.
- On mobile, preserve the same hierarchy: signal board first, then main analysis, then support rails.

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-26 | Initial design system created | Created by `/design-consultation` after design-review findings showed the repo lacked a formal design system. |
| 2026-03-26 | Chose broadcast performance desk over warm editorial direction | Better fit for the utility-first research workbench and the requested “Nike feel.” |
| 2026-03-26 | Chose dark high-contrast system with one performance accent | Keeps the product athletic and specific without becoming a generic sports dashboard. |
