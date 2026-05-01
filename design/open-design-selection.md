# Open Design Selection

## Selected System

**Open Design design system:** `sanity`

The local `open-design` wrapper currently responds as the development lifecycle CLI and does not expose the documented `render`, `list-skills`, or `list-design-systems` output in this shell session. I therefore used the installed Open Design source directly:

- `/Users/kreuzchen/Developer/open-design/design-systems/sanity/DESIGN.md`
- `/Users/kreuzchen/Developer/open-design/skills/web-prototype/SKILL.md`

## Why Sanity Fits

Sanity's visual language is a strong fit for Open Data Issue Lab because it is built around structured content, a dark command-center surface, mono technical labels, and vivid signal colors. It can read as a lab without turning the site into a novelty interface.

## Adapted Tokens

- Background: `#0b0b0b`
- Surface: `#171717`
- Elevated surface: `#212121`
- Border: `#353535`
- Text: `#f7f7f2`
- Muted text: `#b9b9b9`
- Technical text: `#8e8e8e`
- Primary accent: `#f36458`
- Interactive accent: `#4da3ff`
- Success / fresh state: `#37cd84`
- Warning / stale state: `#f1c21b`

## UX Translation

- Homepage reads like a short civic data story.
- Method and source sections read like a lab notebook.
- Energy charts are built with native SVG and CSS to avoid external chart dependencies in the MVP.
- Pipeline status is summarized on-site but detailed logs stay in JSON.
