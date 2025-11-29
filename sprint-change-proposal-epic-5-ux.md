# Sprint Change Proposal: "Soulful" UX Pivot for Epic 5

**Date:** 2025-11-19
**Status:** Draft
**Trigger:** User feedback on Story 5.1 ("Bland vibe," "Bad dark theme," request for "Soul").

## 1. Identified Issue
The initial implementation of Story 5.1 (Streamlit Structure & Theme) produced a "bland," "vibe-coded" result that lacked professional polish and aesthetic depth. The "Warm Beige" theme failed in dark mode and did not convey the "Emotional Landscape" richness required. The user specifically requested a pivot away from sterile "modern analytics" toward a **"Soulful but Professional"** aesthetic.

## 2. Recommended Path Forward
**Pivot to "Rich Spirit World" Dark Mode.**
Instead of a generic light/dark adaptable theme, we will implement a curated, immersive **Dark Mode** experience inspired by the night scenes of *Spirited Away*. This ensures consistent "soulful" quality and allows for high-contrast, glowing data visualizations.

## 3. Artifact Adjustments

### Story 5.1: Streamlit Multi-Page App Structure & Theme (REFACTOR)
*Update Acceptance Criteria to reflect new design direction:*

*   **Theme Strategy:**
    *   **Force Dark Mode:** Set `[theme] base="dark"` in `.streamlit/config.toml`.
    *   **Palette:**
        *   Background: Deep Indigo/Midnight (`#0f172a` / `#1e293b`) - *Not just flat black.*
        *   Primary Accent: Spirit Blue/Cyan (`#38bdf8`) - *Glowing effect.*
        *   Secondary Accent: Gold/Lantern (`#f59e0b`) - *For key metrics.*
        *   Text: Off-white/Stardust (`#f1f5f9`) - *High readability.*
    *   **Typography:** Implement Google Fonts integration.
        *   Headers: *Cinzel* or *Playfair Display* (Serif, cinematic).
        *   Body/Data: *Inter* or *Lato* (Clean sans-serif for readability).

*   **Component Styling (CSS):**
    *   Implement **Glassmorphism** (semi-transparent backgrounds with blur) for cards to create depth.
    *   Remove heavy borders; use subtle shadows and glow effects.
    *   Refine spacing to be "editorial" rather than "compact dashboard."

### Epic 5 (General)
*   **UI Expert Agent Involvement:** Future UI/visualization stories (5.2 - 5.6) must be implemented by (or in consultation with) the UI Expert agent to maintain this new aesthetic standard.
*   **Visualizations:** Update chart specs to use dark-mode friendly colors (neon/pastels that pop against dark backgrounds) rather than default Plotly colors.

## 4. Action Plan
1.  **Approve this Proposal.**
2.  **Revert/Refactor Story 5.1:** Reset the theme implementation.
3.  **Hand off to UI Expert Agent:** Assign the refactoring of `theme.py` and `.streamlit/config.toml` to the UI Expert to build the "Spirit World" design system.

## 5. Success Criteria
*   The app feels "Immersive" and "Premium" immediately upon loading.
*   No "flash of unstyled content" or broken colors in dark mode.
*   Data charts "glow" against the background.
*   Typography feels distinct (not default Streamlit sans).


