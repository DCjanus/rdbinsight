# RDBInsight HTML Report Design Specification

## 1. Core Requirements

- **Tech Stack:**
  - **Styling:** Tailwind CSS
  - **Component Library:** DaisyUI
  - **View Layer:** Vue.js 3
  - **Flame Graph:** d3-flame-graph
- **Distribution:** All CSS and JavaScript resources are inlined to generate a single HTML file.
- **Theme:** Supports light/dark mode and automatically switches based on system settings.
- **Componentization:** Encapsulate and reuse interaction logic through Vue components (e.g., `CopyableText`).

## 2. Page Layout

Adopts a responsive dashboard layout.

- **Top Navigation Bar (Header):**
  - **Left:** Title "RDBInsight".
  - **Right:** Metadata (cluster name, report batch) and an "Export JSON" button.
  - **Metadata:** Uses the `CopyableText` component for text truncation, displaying the full name in a tooltip, and click-to-copy functionality.
- **Main Content:**
  - Uses a responsive grid layout, with content composed of independent cards.

## 3. Functional Modules

Retains existing report features, refactoring with DaisyUI components.

### 3.1. Overview Stats

- **Component:** DaisyUI `stats`.
- **Content:**
  - Total RDB Size
  - Total Keys Count
  - Number of Instances
  - Number of Databases
- **Interaction:** On mouse hover, provides detailed units or descriptions via a tooltip.

### 3.2. Distribution Charts

- **Format:** Three side-by-side cards, using DaisyUI `progress` or bar charts.
- **Content:**
  - **By Data Type:** Memory usage distribution for `string`, `hash`, `list`, etc.
  - **By Database:** Memory usage distribution for `DB0`, `DB1`, etc.
  - **By Instance:** Memory usage distribution across Redis instances.
- **Visuals:** Displays percentages and specific sizes, using different colors for distinction.
- **Layout:** When content overflows, the card's maximum height is `24rem` (384px), and vertical scrolling is enabled.

### 3.3. Memory Flame Graph

- **Layout:** Bottom-up, with rectangle width proportional to memory percentage. All percentages are calculated based on the total cluster memory.
- **Implementation:**
  - **Data Processing:** The frontend parses `top_prefixes` data, builds a parent-child hierarchy using the colon delimiter, and recursively calculates the self-value for each node (`self-value = total_size - children's total_size`).
  - **Rendering:** Uses the `d3-flame-graph` library and is configured with `selfValue(true)`.
- **Interaction:**
  - **Zoom:** Click a node to drill down; a reset button is provided.
  - **Tooltip:** On hover, displays the full prefix, key count, memory size, and percentage of total memory.
  - **Info Bar:** Displays detailed information for the hovered node in real-time.
  - **Search:** Performs a case-insensitive search by full prefix path (e.g., `a:b:c`) and highlights matches.
- **Performance:**
  - **Responsive Width:** Adapts to window changes.
  - **Rendering:** Limits the minimum render size to avoid displaying excessively small memory blocks.

### 3.4. Top 100 Big Keys

- **Component:** Responsive DaisyUI `table`.
- **Features:**
  - **Columns:** Key Name, RDB Size, Data Type, Instance, DB, Member Count, Encoding, Expiry Time.
  - **Interaction:**
    - The Key Name column supports real-time filtering.
    - All columns support click-to-sort.
- **Cell (`<td>`) Enhancements:**
  - **Copyable Content:** Key names, instance names, and expiry times use the `CopyableText` component to provide truncation, tooltips, and click-to-copy functionality.
  - **Data Display:**
    - Non-UTF-8 keys are hex-escaped.
    - RDB size and member count are formatted for human readability (e.g., `1.2 MB`, `10.5k`).
    - Expiry time is displayed in the local timezone format.
