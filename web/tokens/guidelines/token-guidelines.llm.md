# Token Guidelines

## background color

### bgColor-accent-emphasis
Strong accent background for active states and focused states
**Usage:** active-states, active-toggle, current, selected
**Rules:** MUST use for selected or active states. Pair with fgColor.onEmphasis for text.

### bgColor-accent-muted
Subtle accent background for informational or selected elements
**Usage:** active-nav-item, highlight, info-banner, selected-row
**Rules:** Use for selected states or informational highlights. Pair with fgColor.accent for text.

### bgColor-attention-emphasis
Strong attention background for prominent warnings
**Usage:** attention-indicator, caution-label, warning-badge
**Rules:** Use for prominent warnings. Consider fgColor.default for text due to yellow contrast.

### bgColor-attention-muted
Subtle attention background for warnings and caution states
**Usage:** caution-message, pending-state, warning-banner
**Rules:** Use for warnings that need user attention. Pair with fgColor.attention for text.

### bgColor-closed-emphasis
Strong background for closed state badges and labels
**Usage:** closed-badge, closed-label, declined-status-badge
**Rules:** Use for prominent closed/declined state indicators. Pair with fgColor.onEmphasis for text.

### bgColor-closed-muted
Subtle background for closed state indicators (issues, PRs)
**Usage:** closed-issue, closed-pr, declined-state
**Rules:** Use for closed/declined status indicators. Specifically for GitHub issues and PRs.

### bgColor-danger-emphasis
Emphasized danger background for critical errors and delete confirmations
**Usage:** critical-alert, delete-button, destructive-confirmation
**Rules:** MUST use for destructive action buttons like delete. Use fgColor.onEmphasis for text on this background.

### bgColor-danger-muted
Muted danger background for error states and destructive action contexts
**Usage:** destructive-action, error-message, validation-error
**Rules:** Use for error states and destructive action backgrounds. Pair with fgColor.danger for text.

### bgColor-default
Default background color for pages and main content areas
**Usage:** card-background, main-content, page-background
**Rules:** Use as the primary background for pages and content areas. Do NOT use for emphasis or highlighting.

### bgColor-disabled
Background for disabled interactive elements
**Usage:** disabled-button, disabled-input, inactive-element
**Rules:** MUST use for disabled state backgrounds. Pair with fgColor.disabled for text. Do NOT use for active elements.

### bgColor-done-emphasis
Strong background for completed/done state badges and labels
**Usage:** completed-indicator, done-badge, merged-label
**Rules:** Use for prominent done/completed state indicators. Pair with fgColor.onEmphasis for text.

### bgColor-done-muted
Subtle background for completed/done state indicators
**Usage:** completed-task, done-state, merged-pr
**Rules:** Use for completed/done status indicators. Conveys finished or merged state.

### bgColor-draft-emphasis
Strong background for draft state badges and labels
**Usage:** draft-badge, draft-label, wip-indicator
**Rules:** Use for prominent draft state indicators. Pair with fgColor.onEmphasis for text.

### bgColor-draft-muted
Subtle background for draft state indicators
**Usage:** draft-issue, draft-pr, work-in-progress
**Rules:** Use for draft/WIP status indicators. Conveys incomplete or pending state.

### bgColor-emphasis
High-emphasis dark background for strong visual contrast
**Usage:** badge-background, high-contrast-element, tooltip
**Rules:** Use for elements needing strong visual emphasis. Pair with fgColor.onEmphasis for text. Do NOT use for large areas.

### bgColor-inset
Inset background for recessed content areas like wells or sunken panels
**Usage:** input-background, recessed-area, sunken-panel, well
**Rules:** Use for visually recessed areas. Creates depth hierarchy. Suitable for input fields and wells.

### bgColor-inverse
Inverse background that flips between light and dark modes
**Usage:** inverse-theme-element, overlay-content
**Rules:** Use when you need opposite theme background. Pair with fgColor.onInverse for text.

### bgColor-muted
Muted background for secondary content areas and subtle grouping
**Usage:** code-block-background, secondary-content, sidebar, table-header
**Rules:** Use for secondary content areas or to create visual grouping. Do NOT use for primary page backgrounds.

### bgColor-neutral-emphasis
Strong neutral background for prominent neutral elements
**Usage:** neutral-button, neutral-indicator, secondary-action
**Rules:** Use for emphasized neutral elements. Pair with fgColor.onEmphasis for text.

### bgColor-neutral-muted
Subtle neutral background for tags, labels, and secondary UI elements
**Usage:** counter, neutral-badge, neutral-label, secondary-tag
**Rules:** Use for neutral semantic meaning. Pair with fgColor.default for text. Do NOT use for status indicators.

### bgColor-open-emphasis
Strong background for open state badges and labels
**Usage:** active-status-badge, open-badge, open-label
**Rules:** Use for prominent open/active state indicators. Pair with fgColor.onEmphasis for text.

### bgColor-open-muted
Subtle background for open state indicators (issues, PRs)
**Usage:** active-status, open-issue, open-pr
**Rules:** Use for open/active status indicators. Specifically for GitHub issues and PRs.

### bgColor-severe-emphasis
Strong severe background for prominent high-priority warnings
**Usage:** high-priority-indicator, severe-badge, urgent-label
**Rules:** Use for prominent severe warnings. Pair with fgColor.onEmphasis for text.

### bgColor-severe-muted
Subtle severe background for high-priority warnings
**Usage:** escalation, high-priority-warning, urgent-message
**Rules:** Use for severe warnings, less critical than danger. Pair with fgColor.severe for text.

### bgColor-sponsors-emphasis
Strong background for prominent GitHub Sponsors elements
**Usage:** funding-cta, sponsor-badge, sponsor-button
**Rules:** Use for prominent Sponsors CTAs. Pair with fgColor.onEmphasis for text.

### bgColor-sponsors-muted
Subtle background for GitHub Sponsors content
**Usage:** funding-prompt, sponsor-card, sponsor-highlight
**Rules:** Use for GitHub Sponsors related content. Do NOT use for general pink-colored elements.

### bgColor-success-emphasis
Strong success background for prominent positive actions
**Usage:** confirm-action, merge-button, success-badge
**Rules:** Use for positive action buttons like merge or confirm. Pair with fgColor.onEmphasis for text.

### bgColor-success-muted
Subtle success background for positive feedback and completed states
**Usage:** completed-state, positive-feedback, success-message
**Rules:** Use for success states and positive feedback. Pair with fgColor.success for text.

### bgColor-transparent
Fully transparent background
**Usage:** ghost-button, icon-button, overlay-trigger
**Rules:** Use for ghost/icon buttons or when element should blend with parent. Ensure sufficient contrast for interactive states.

### bgColor-upsell-emphasis
Strong background for prominent upsell elements
**Usage:** premium-badge, upgrade-button, upsell-cta
**Rules:** Use for prominent upgrade/upsell CTAs. Pair with fgColor.onEmphasis for text.

### bgColor-upsell-muted
Subtle background for upsell and promotional content
**Usage:** premium-feature, promotional-banner, upgrade-prompt
**Rules:** Use for upgrade prompts and premium feature highlights. Do NOT use for regular content.

## Border

**Usage:** card-border, container-border, divider, input-border, panel-border
**Rules:** Use default for standard container borders. Use muted for subtle dividers and low-emphasis boundaries. Use emphasis when borders need to stand out. Match status border colors to their semantic meaning (danger for errors, success for positive states). Use disabled only for non-interactive or disabled elements. Prefer semantic variants (open/closed, done) over raw color variants when the context matches.

### accent
Accent-colored borders for selected, focused, or highlighted elements.
**Tokens:** border-accent-emphasis, border-accent-muted

### attention
Yellow borders for warnings, caution states, and items requiring user attention.
**Tokens:** border-attention-emphasis, border-attention-muted

### closed
Borders for closed state indicators. Use for closed pull requests, issues, and discussions. Aliases border.danger.
**Tokens:** border-closed-emphasis, border-closed-muted

### danger
Red borders for errors, destructive actions, and critical warnings.
**Tokens:** border-danger-emphasis, border-danger-muted

### general
Semantic border tokens combining color, style, and width for consistent UI boundaries.
**Tokens:** border-default, border-disabled, border-emphasis, border-muted, border-transparent

### done
Purple borders for completed or merged states. Use for merged pull requests and completed items.
**Tokens:** border-done-emphasis, border-done-muted

### neutral
Neutral gray borders for non-semantic boundaries and subtle UI separation.
**Tokens:** border-neutral-emphasis, border-neutral-muted

### open
Borders for open state indicators. Use for open pull requests, issues, and discussions. Aliases border.success.
**Tokens:** border-open-emphasis, border-open-muted

### severe
Orange borders for severe warnings, more urgent than attention but less critical than danger.
**Tokens:** border-severe-emphasis, border-severe-muted

### sponsors
Borders for GitHub Sponsors content and sponsor-related UI elements.
**Tokens:** border-sponsors-emphasis, border-sponsors-muted

### success
Green borders for positive states, confirmations, and successful operations.
**Tokens:** border-success-emphasis, border-success-muted

### upsell
Borders for promotional content and feature upsells. Use for upgrade prompts and premium feature highlights.
**Tokens:** border-upsell-emphasis, border-upsell-muted

## BorderColor

### borderColor-accent-emphasis
Strong accent border for selected or focused elements
**Usage:** accent-emphasis, focus-border, selected-border
**Rules:** Use for selected elements and focus states. Strong visual emphasis.

### borderColor-accent-muted
Subtle accent border for selected or focused elements
**Usage:** accent-muted, info-muted, selected-muted
**Rules:** Use for accent-colored borders on selected elements. Pair with bgColor.accent.muted.

### borderColor-attention-emphasis
Strong attention border for prominent warnings
**Usage:** attention-emphasis, caution-emphasis, warning-emphasis
**Rules:** Use for emphasized warning borders. Pair with bgColor.attention.emphasis.

### borderColor-attention-muted
Subtle attention border for warnings and caution states
**Usage:** attention-muted, caution-muted, warning-muted
**Rules:** Use for warning state borders. Pair with bgColor.attention.muted.

### borderColor-closed-emphasis
Strong border for closed state badges
**Usage:** closed-emphasis, closed-status
**Rules:** Use for emphasized closed state borders. Pair with bgColor.closed.emphasis.

### borderColor-closed-muted
Subtle border for closed state indicators
**Usage:** closed-issue, closed-muted, closed-pr
**Rules:** Use for closed/declined status borders. Specifically for GitHub issues and PRs.

### borderColor-danger-emphasis
Strong danger border for destructive actions and errors
**Usage:** danger-emphasis, destructive-emphasis, error-emphasis
**Rules:** MUST use for destructive action borders. Pair with bgColor.danger.emphasis.

### borderColor-danger-muted
Subtle danger border for errors and destructive contexts
**Usage:** danger-muted, destructive-muted, error-muted
**Rules:** Use for error state borders. Pair with bgColor.danger.muted.

### borderColor-default
Default border color for most UI elements
**Usage:** card-border, default-border, divider, input-border
**Rules:** RECOMMENDED default for all borders. Use for cards, inputs, and dividers.

### borderColor-disabled
Border color for disabled interactive elements
**Usage:** disabled-border, inactive-border, unavailable
**Rules:** MUST use for disabled state borders. Pair with bgColor.disabled. Do NOT use for active elements.

### borderColor-done-emphasis
Strong border for completed/done state badges
**Usage:** done-emphasis, merged-status
**Rules:** Use for emphasized done state borders. Pair with bgColor.done.emphasis.

### borderColor-done-muted
Subtle border for completed/done state indicators
**Usage:** completed, done-muted, merged
**Rules:** Use for completed/done status borders. Conveys finished or merged state.

### borderColor-draft-emphasis
Strong border for draft state badges
**Usage:** draft-emphasis, draft-status
**Rules:** Use for emphasized draft state borders. Pair with bgColor.draft.emphasis.

### borderColor-draft-muted
Subtle border for draft state indicators
**Usage:** draft-issue, draft-muted, draft-pr
**Rules:** Use for draft/WIP status borders. Conveys incomplete or pending state.

### borderColor-emphasis
Strong border for emphasis and visual weight
**Usage:** emphasis-border, selected-border, strong-border
**Rules:** Use for borders needing more visual weight. Darker than borderColor.default.

### borderColor-muted
Subtle border for secondary elements and light separators
**Usage:** light-divider, secondary-border, subtle-border
**Rules:** Use for subtle borders and separators. Less prominent than borderColor.default.

### borderColor-neutral-emphasis
Strong neutral semantic border
**Usage:** neutral-emphasis, neutral-strong
**Rules:** Use for emphasized neutral semantic borders. Stronger than neutral.muted.

### borderColor-neutral-muted
Subtle neutral semantic border
**Usage:** neutral-muted, neutral-secondary
**Rules:** Use for neutral semantic borders on tags and badges. Prefer borderColor.muted for general use.

### borderColor-open-emphasis
Strong border for open state badges
**Usage:** open-emphasis, open-status
**Rules:** Use for emphasized open state borders. Pair with bgColor.open.emphasis.

### borderColor-open-muted
Subtle border for open state indicators
**Usage:** open-issue, open-muted, open-pr
**Rules:** Use for open/active status borders. Specifically for GitHub issues and PRs.

### borderColor-severe-emphasis
Strong severe border for prominent high-priority warnings
**Usage:** severe-emphasis, urgent-emphasis
**Rules:** Use for emphasized severe borders. Pair with bgColor.severe.emphasis.

### borderColor-severe-muted
Subtle severe border for high-priority warnings
**Usage:** severe-muted, urgent-muted
**Rules:** Use for severe warning borders. More critical than attention, less than danger.

### borderColor-sponsors-emphasis
Strong border for prominent GitHub Sponsors elements
**Usage:** funding-emphasis, sponsors-emphasis
**Rules:** Use for emphasized Sponsors borders. Pair with bgColor.sponsors.emphasis.

### borderColor-sponsors-muted
Subtle border for GitHub Sponsors content
**Usage:** funding-muted, sponsors-muted
**Rules:** Use for GitHub Sponsors related borders. Do NOT use for general pink-colored elements.

### borderColor-success-emphasis
Strong success border for prominent positive elements
**Usage:** confirmed, positive-emphasis, success-emphasis
**Rules:** Use for emphasized success borders. Pair with bgColor.success.emphasis.

### borderColor-success-muted
Subtle success border for positive feedback elements
**Usage:** positive-muted, success-muted, valid-muted
**Rules:** Use for success state borders. Pair with bgColor.success.muted.

### borderColor-translucent
Semi-transparent border for overlays and layered elements
**Usage:** overlay-border, translucent-border
**Rules:** Use for semi-transparent borders on overlays. Works well with translucent backgrounds.

### borderColor-upsell-emphasis
Strong border for prominent upsell elements
**Usage:** premium-emphasis, upsell-emphasis
**Rules:** Use for emphasized upsell borders. Pair with bgColor.upsell.emphasis.

### borderColor-upsell-muted
Subtle border for upsell and promotional content
**Usage:** premium-muted, upsell-muted
**Rules:** Use for upgrade prompts and premium feature borders. Do NOT use for regular content.

## BorderRadius

### borderRadius-full
Use this border radius for pill shaped elements
**Usage:** avatar, circular-button, pill-badge
**Rules:** Use for avatars and pill-shaped elements. Do NOT use for rectangular containers.

### borderRadius-large
Large border radius (12px). Use for larger containers, dialogs, or when more visual softness is desired. Always use this for buttons.
**Usage:** card, dialog, modal
**Rules:** Recommended for dialogs and modals.

### borderRadius-medium
Medium border radius (6px). The default choice for most buttons, cards, and containers
**Usage:** button, card, container, input, select, textarea
**Rules:** Default choice for most components. Use for inputs, cards, and general containers.

### borderRadius-small
Small border radius (3px). Use for small variants of components or small UI elements like badges, tags, or anything below 16px in height
**Usage:** badge, label, small-input, tag
**Rules:** Use for small UI elements under 16px height. Do NOT use for buttons or cards.

## BorderWidth

### borderWidth-thick
Thick 2px border for emphasis. Use for focus indicators, selected states, or to emphasize important boundaries
**Usage:** emphasis-border, focus-indicator, selected-state
**Rules:** MUST use for focus rings on interactive elements. Do NOT use for subtle dividers.

## Control

**Usage:** button, form-control, form-control, input, interactive-element, select
**Rules:** Use size variants consistently across related controls. Match control size to context: xsmall/small for dense UIs, medium for standard forms, large/xlarge for prominent CTAs. Use minTarget values to ensure touch accessibility on coarse pointer devices.

### general
Size tokens for interactive controls (buttons, inputs, selects). Includes overall size, padding, line height, and gap values for each size variant.
**Tokens:** control-bgColor-active, control-bgColor-disabled, control-bgColor-hover, control-bgColor-rest, control-bgColor-selected, control-borderColor-danger, control-borderColor-disabled, control-borderColor-emphasis, control-borderColor-rest, control-borderColor-selected, control-borderColor-success, control-borderColor-warning, control-fgColor-disabled, control-fgColor-placeholder, control-fgColor-rest, control-iconColor-rest, control-large-gap, control-large-lineBoxHeight, control-large-paddingBlock, control-large-paddingInline-normal, control-large-paddingInline-spacious, control-large-size, control-medium-gap, control-medium-lineBoxHeight, control-medium-paddingBlock, control-medium-paddingInline-condensed, control-medium-paddingInline-normal, control-medium-paddingInline-spacious, control-medium-size, control-minTarget-coarse, control-minTarget-fine, control-small-gap, control-small-lineBoxHeight, control-small-paddingBlock, control-small-paddingInline-condensed, control-small-paddingInline-normal, control-small-size, control-xlarge-gap, control-xlarge-lineBoxHeight, control-xlarge-paddingBlock, control-xlarge-paddingInline-normal, control-xlarge-paddingInline-spacious, control-xlarge-size, control-xsmall-gap, control-xsmall-lineBoxHeight, control-xsmall-paddingBlock, control-xsmall-paddingInline-condensed, control-xsmall-paddingInline-normal, control-xsmall-paddingInline-spacious, control-xsmall-size

### checked
Colors for checked/selected state of checkboxes, radio buttons, and toggle switches.
**Tokens:** control-checked-bgColor-active, control-checked-bgColor-disabled, control-checked-bgColor-hover, control-checked-bgColor-rest, control-checked-borderColor-active, control-checked-borderColor-disabled, control-checked-borderColor-hover, control-checked-borderColor-rest, control-checked-fgColor-disabled, control-checked-fgColor-rest

### danger
Danger control colors for destructive actions like delete buttons and remove actions.
**Tokens:** control-danger-bgColor-active, control-danger-bgColor-hover, control-danger-fgColor-hover, control-danger-fgColor-rest

### transparent
Transparent control colors for ghost buttons and tertiary actions with no visible background at rest.
**Tokens:** control-transparent-bgColor-active, control-transparent-bgColor-disabled, control-transparent-bgColor-hover, control-transparent-bgColor-rest, control-transparent-bgColor-selected, control-transparent-borderColor-active, control-transparent-borderColor-hover, control-transparent-borderColor-rest

## ControlKnob

**Usage:** slider-thumb, switch-handle, toggle-knob
**Rules:** Use for the movable handle/thumb of toggle switches and sliders. Pair with controlTrack tokens for the background rail.

Colors for toggle switch knobs (the circular handle that moves along the track).
**Tokens:** controlKnob-bgColor-checked, controlKnob-bgColor-disabled, controlKnob-bgColor-rest, controlKnob-borderColor-checked, controlKnob-borderColor-disabled, controlKnob-borderColor-rest

## ControlStack

**Usage:** action-bar, button-group, form-control-group, toolbar
**Rules:** Use with groups of related controls. Match gap size to control size (small controls use small gaps). Condensed for tight groupings, spacious for separated actions.

Gap tokens for groups of controls (buttons, form elements) arranged in a row or column.
**Tokens:** controlStack-large-gap-auto, controlStack-large-gap-condensed, controlStack-large-gap-spacious, controlStack-medium-gap-condensed, controlStack-medium-gap-spacious, controlStack-small-gap-condensed, controlStack-small-gap-spacious

## ControlTrack

**Usage:** slider-track, switch-track, toggle-track
**Rules:** Use for the track/rail element of toggle switches and sliders. Pair with controlKnob tokens for the movable handle.

Colors for toggle switch tracks (the background rail that the knob slides along).
**Tokens:** controlTrack-bgColor-active, controlTrack-bgColor-disabled, controlTrack-bgColor-hover, controlTrack-bgColor-rest, controlTrack-borderColor-disabled, controlTrack-borderColor-rest, controlTrack-fgColor-disabled, controlTrack-fgColor-rest

## Data

**Usage:** area-chart, bar-fill, chart-series, data-point, graph-line, pie-segment
**Rules:** Use data colors for visualizations only. Do NOT use for semantic meaning (use bgColor.success/danger instead). When using multiple series, ensure sufficient contrast between adjacent colors. Pair emphasis with muted variants of the same color for cohesive styling.

Colors for data visualization including charts, graphs, and diagrams. Use these colors to differentiate data series. Emphasis variants are for lines, bars, and borders. Muted variants are for area fills and backgrounds.
**Tokens:** data-auburn-color-emphasis, data-auburn-color-muted, data-blue-color-emphasis, data-blue-color-muted, data-brown-color-emphasis, data-brown-color-muted, data-coral-color-emphasis, data-coral-color-muted, data-gray-color-emphasis, data-gray-color-muted, data-green-color-emphasis, data-green-color-muted, data-lemon-color-emphasis, data-lemon-color-muted, data-lime-color-emphasis, data-lime-color-muted, data-olive-color-emphasis, data-olive-color-muted, data-orange-color-emphasis, data-orange-color-muted, data-pine-color-emphasis, data-pine-color-muted, data-pink-color-emphasis, data-pink-color-muted, data-plum-color-emphasis, data-plum-color-muted, data-purple-color-emphasis, data-purple-color-muted, data-red-color-emphasis, data-red-color-muted, data-teal-color-emphasis, data-teal-color-muted, data-yellow-color-emphasis, data-yellow-color-muted

## Easing

### base-easing-ease
CSS default easing. Use for hover state changes and micro-interactions.
**Usage:** button-hover, hover-state, link-hover, micro-interaction
**Rules:** Use for hover state changes.

### base-easing-easeIn
Accelerating motion. Use for elements exiting the viewport (moving off-screen).
**Usage:** element-leaving, exit-animation, off-screen-motion
**Rules:** Rarely used alone. Prefer ease-out for most exit animations.

### base-easing-easeInOut
Smooth acceleration and deceleration. Use for elements moving or morphing within the viewport.
**Usage:** expand-collapse, morph-animation, position-change, size-change, slide-transition
**Rules:** Use if an element moves or morphs on screen.

### base-easing-easeOut
Decelerating motion. Use for elements entering the viewport or appearing on screen.
**Usage:** dropdown-open, element-appearing, enter-animation, modal-open, tooltip-appear
**Rules:** RECOMMENDED default. Use if an element enters or exits the viewport.

### base-easing-linear
Constant motion with no acceleration. Use for continuous animations like progress bars or loaders.
**Usage:** continuous-animation, loader, progress-bar, scrolling
**Rules:** Use if the motion is constant.

## text and foreground color

### fgColor-accent
Accent text for links and interactive elements
**Usage:** accent-icon, accent-text, info-text
**Rules:** Use for accent-colored text and icons. Pair with bgColor.accent.muted for backgrounds.

### fgColor-attention
Attention text for warnings and caution states
**Usage:** attention-text, caution-text, warning-text
**Rules:** Use for warning and caution text. Pair with bgColor.attention.muted for backgrounds.

### fgColor-closed
Text color for closed state indicators (issues, PRs)
**Usage:** closed-issue, closed-pr, closed-text
**Rules:** Use for closed/declined status text. Specifically for GitHub issues and PRs.

### fgColor-danger
Danger text for errors and destructive actions
**Usage:** danger-text, destructive-text, error-text
**Rules:** Use for error messages and destructive action text. Pair with bgColor.danger.muted for backgrounds.

### fgColor-default
Default text color for primary content and headings
**Usage:** body-text, default-text, heading
**Rules:** RECOMMENDED default for all text. Use for headings, body text, and primary labels.

### fgColor-disabled
Text color for disabled interactive elements
**Usage:** disabled-text, inactive-text, unavailable
**Rules:** MUST use for disabled state text. Pair with bgColor.disabled. Do NOT use for active elements.

### fgColor-done
Text color for completed/done state indicators
**Usage:** completed, done-text, merged
**Rules:** Use for completed/done status text. Conveys finished or merged state.

### fgColor-draft
Text color for draft state indicators
**Usage:** draft-issue, draft-pr, draft-text
**Rules:** Use for draft/WIP status text. Conveys incomplete or pending state.

### fgColor-link
Text color for hyperlinks
**Usage:** hyperlink, link-text
**Rules:** MUST use for all text links. Provides expected link affordance.

### fgColor-muted
Muted text for secondary content and less important information
**Usage:** helper-text, muted-text, placeholder, secondary-text
**Rules:** Use for secondary text like timestamps, metadata, and helper text. Do NOT use for primary content.

### fgColor-neutral
Neutral semantic text for icons and secondary elements
**Usage:** neutral-icon, neutral-text
**Rules:** Use for neutral semantic elements. Prefer fgColor.muted for secondary text.

### fgColor-onEmphasis
Text color for use on emphasis backgrounds
**Usage:** contrast-text, text-on-emphasis
**Rules:** MUST use for text on any emphasis background (bgColor.*.emphasis). Ensures accessibility contrast.

### fgColor-onInverse
Text color for use on inverse backgrounds
**Usage:** inverse-text, text-on-inverse
**Rules:** Use for text on bgColor.inverse. Provides appropriate contrast in both themes.

### fgColor-open
Text color for open state indicators (issues, PRs)
**Usage:** open-issue, open-pr, open-text
**Rules:** Use for open/active status text. Specifically for GitHub issues and PRs.

### fgColor-severe
Severe text for high-priority warnings
**Usage:** severe-icon, severe-text, urgent-text
**Rules:** Use for severe warnings, less critical than danger. Pair with bgColor.severe.muted for backgrounds.

### fgColor-sponsors
Text color for GitHub Sponsors content
**Usage:** funding-text, sponsors-icon, sponsors-text
**Rules:** Use for GitHub Sponsors related text. Do NOT use for general pink-colored text.

### fgColor-success
Success text for positive feedback and completed states
**Usage:** positive-text, success-icon, success-text
**Rules:** Use for success states and positive feedback text. Pair with bgColor.success.muted for backgrounds.

### fgColor-upsell
Text color for upsell and promotional content
**Usage:** premium-text, promotional, upsell-text
**Rules:** Use for upgrade prompts and premium feature text. Do NOT use for regular content.

## Focus

### focus-outline
Focus ring outline for keyboard navigation and accessibility.
**Usage:** accessibility-indicator, focus-ring, keyboard-navigation
**Rules:** Always ensure focus states are visible. Do not override with custom focus styles that reduce visibility. Use for interactive elements like buttons, links, and form controls.

### focus-outlineColor
Outline color for focus states on interactive elements
**Usage:** accessibility-indicator, focus-ring, keyboard-navigation
**Rules:** Use for focus outlines on interactive elements like buttons, links, and form controls. MUST be visible for keyboard navigation accessibility. Do NOT use for decorative borders or non-interactive elements.

## FontStack

### fontStack-monospace
Monospace font stack for code, technical content, and tabular data.
**Usage:** code-block, inline-code, tabular-numbers, technical-data, terminal
**Rules:** MUST use for all code display. Use for technical content requiring fixed-width characters.

### fontStack-sansSerif
Sans-serif font stack for body text and general UI elements.
**Usage:** body-text, form-inputs, labels, paragraphs
**Rules:** Default font stack for all UI text. Use for body text and standard UI elements. MUST use for readable content.

### fontStack-sansSerifDisplay
Display font stack for headings and titles. Same as sansSerif but semantically distinct.
**Usage:** display-text, heading, hero-text, title
**Rules:** Use for headings and display text. Prefer over sansSerif for titles.

## Outline

### outline-focus-width
Focus outline width (2px). Standard width for keyboard focus indicators to meet WCAG 2.4.7 accessibility requirements
**Usage:** accessibility, focus-ring, keyboard-focus
**Rules:** MUST use for all keyboard focus indicators. Required for WCAG 2.4.7 compliance.

## Overlay

**Usage:** dialog, dropdown, modal, overlay-panel, popover, tooltip
**Rules:** Match overlay size to content: xsmall/small for menus and tooltips, medium for dialogs, large/xlarge for complex modals. Use height tokens to constrain scrollable overlays. Always consider viewport size constraints.

Dimension tokens for overlay components like modals, dialogs, popovers, and dropdown menus.
**Tokens:** overlay-backdrop-bgColor, overlay-bgColor, overlay-borderColor, overlay-borderRadius, overlay-height-large, overlay-height-medium, overlay-height-small, overlay-height-xlarge, overlay-offset, overlay-padding-condensed, overlay-padding-normal, overlay-paddingBlock-condensed, overlay-paddingBlock-normal, overlay-width-large, overlay-width-medium, overlay-width-small, overlay-width-xlarge, overlay-width-xsmall

## Selection

### selection-bgColor
Background color for text selection highlights
**Usage:** highlighted-text, selected-content, text-selection
**Rules:** Use for native text selection (::selection) and programmatic text highlighting. Do NOT use for general emphasis or background colors on containers.

## Shadow

### shadow-floating-large
Large floating shadow for modals and dialogs
**Usage:** dialog, full-screen-overlay, modal
**Rules:** MUST use for modals and dialogs. Do NOT use for small floating elements.

### shadow-floating-small
Small floating shadow for dropdowns, tooltips, and small overlays
**Usage:** dropdown, menu, popover, tooltip
**Rules:** Use for small floating elements like dropdowns and tooltips. Do NOT use for modals or dialogs.

## Spinner

**Usage:** async-loading, loading-spinner, progress-indicator
**Rules:** Match spinner size to context: small for inline loading, medium for buttons and cards, large for full-page loading states.

Size and stroke tokens for loading spinner components.
**Tokens:** spinner-size-large, spinner-size-medium, spinner-size-small, spinner-strokeWidth-default

## Stack

**Usage:** flex-container, horizontal-stack, stack-layout, vertical-stack
**Rules:** Use condensed for dense lists, normal for standard layouts, spacious for prominent sections. Stack tokens provide consistent spacing within layout containers.

Spacing tokens for Stack layout components including padding and gap values.
**Tokens:** stack-gap-condensed, stack-gap-normal, stack-gap-spacious, stack-padding-condensed, stack-padding-normal, stack-padding-spacious

## Text

### text-body-shorthand-large
User-generated content, markdown rendering.
**Usage:** article-text, comment-body, markdown-content, readme
**Rules:** Use for user-generated content and markdown. Better readability for longer text.

### text-body-shorthand-medium
Default UI font. Most commonly used for body text.
**Usage:** body-text, button-text, form-label, navigation, ui-text
**Rules:** RECOMMENDED default for UI text. Use for buttons, labels, and general interface text.

### text-body-shorthand-small
Small body text for discrete UI applications, such as helper, footnote text. Should be used sparingly across pages. Line-height matches Body (medium) at 20px.
**Usage:** footnote, helper-text, metadata, timestamp
**Rules:** Use sparingly for secondary information. Do NOT use for primary content or interactive elements.

### text-caption-shorthand
Compact small font with a smaller line height of 16px. Use it for single-line scenarios, as the small sizing doesn’t pass accessibility requirements.
**Usage:** badge-text, caption, label, single-line-metadata
**Rules:** Use only for single-line or short text. Does NOT meet accessibility requirements for body text.

### text-codeBlock-shorthand
Default style for rendering code blocks.
**Usage:** code-block, code-snippet, pre-element, terminal-output
**Rules:** MUST use for multi-line code. Use monospace font stack.

### text-codeInline-shorthand
Inline code blocks using em units to inherit size from its parent.
**Usage:** code-element, command, inline-code, variable-name
**Rules:** Use for inline code within text. Size inherits from parent using em units.

### text-display-shorthand
Hero-style text for brand to product transition pages. Utilize Title (large) styles on narrow viewports.
**Usage:** hero-section, landing-page, marketing-header
**Rules:** Use sparingly for hero sections. Switch to title.large on narrow viewports.

### text-subtitle-shorthand
Page sections/sub headings, or less important object names in page titles (automated action titles, for example). Same line-height as title (medium).
**Usage:** description, lead-text, secondary-heading, subtitle
**Rules:** Use below titles for supporting text. Normal weight distinguishes from title styles.

### text-title-shorthand-large
Page headings for user-created objects, such as issues or pull requests. Utilize title (medium) styles on narrow viewports.
**Usage:** h1, issue-title, page-heading, pr-title
**Rules:** Use for primary page headings. Switch to title.medium on narrow viewports.

### text-title-shorthand-medium
Default page title. The 32px-equivalent line-height matches with button and other medium control heights. Great for page header composition.
**Usage:** card-title, dialog-title, h2, section-heading
**Rules:** RECOMMENDED default for page titles. Use for section headings and dialog titles.

### text-title-shorthand-small
Uses the same size as body (large) with a heavier weight of semibold (600).
**Usage:** h3, list-title, sidebar-heading, subsection-heading
**Rules:** Use for smaller headings within sections. Same size as body.large but semibold.
