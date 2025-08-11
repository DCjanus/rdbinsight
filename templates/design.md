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

## 2. Data Processing

### 2.1. Prefix Data Handling

The `top_prefixes` data structure uses binary-safe prefix handling:

- **Backend:** The `PrefixAggregate` struct stores prefix data as `Bytes` type and serializes it as `prefix_base64` field using base64 encoding.
- **Frontend:** The HTML template includes a `decodeAndFormatPrefix()` function that:
  - Decodes base64-encoded prefix data back to binary
  - Attempts UTF-8 decoding for displayable text
  - Falls back to hex-escaped representation for non-UTF-8 data
  - Handles binary prefixes safely without data corruption

This approach ensures that Redis key prefixes containing non-UTF-8 bytes (such as compressed data, binary protocols, or encoded identifiers) are preserved accurately throughout the analysis pipeline.

## 3. Page Layout

Adopts a responsive dashboard layout.

- **Top Navigation Bar (Header):**
  - **Left:** Title "RDBInsight".
  - **Right:** Metadata (cluster name, report batch) and an "Export JSON" button.
  - **Metadata:** Uses the `CopyableText` component for text truncation, displaying the full name in a tooltip, and click-to-copy functionality.
- **Main Content:**
  - Uses a responsive grid layout, with content composed of independent cards.

## 4. Functional Modules

Retains existing report features, refactoring with DaisyUI components.

### 4.1. Overview Stats

- **Component:** DaisyUI `stats`.
- **Content:**
  - Total RDB Size
  - Total Keys Count
  - Number of Instances
  - Number of Databases
- **Interaction:** On mouse hover, provides detailed units or descriptions via a tooltip.

### 4.2. Distribution Charts

- **Format:** Three side-by-side cards, using DaisyUI `progress` or bar charts.
- **Content:**
  - **By Data Type:** Memory usage distribution for `string`, `hash`, `list`, etc.
  - **By Database:** Memory usage distribution for `DB0`, `DB1`, etc.
  - **By Instance:** Memory usage distribution across Redis instances.
- **Visuals:** Displays percentages and specific sizes, using different colors for distinction.
- **Layout:** When content overflows, the card's maximum height is `24rem` (384px), and vertical scrolling is enabled.

### 4.3. Memory Flame Graph

- **Layout:** Bottom-up, with rectangle width proportional to memory percentage. All percentages are calculated based on the total cluster memory.
- **Implementation:**
  - **Data Processing:** The frontend parses `top_prefixes` data, decoding `prefix_base64` fields to get the original prefix strings. It then builds a parent-child hierarchy based on string prefix containment relationships (compatible with dynamic LCP algorithm results), and recursively calculates the self-value for each node (`self-value = total_size - children's total_size`).
  - **Rendering:** Uses the `d3-flame-graph` library and is configured with `selfValue(true)`.
- **Interaction:**
  - **Zoom:** Click a node to drill down; a reset button is provided.
  - **Tooltip:** On hover, displays the full prefix, key count, memory size, and percentage of total memory.
  - **Info Bar:** Displays detailed information for the hovered node in real-time.
  - **Search:** Performs a case-insensitive search by full prefix path (e.g., `a:b:c`) and highlights matches.
- **Performance:**
  - **Responsive Width:** Adapts to window changes.
  - **Rendering:** Limits the minimum render size to avoid displaying excessively small memory blocks.

### 4.4. Data Export Features

- **JSON Export:**
  - Exports complete report data with enhanced human-readable fields.
  - For `top_keys`: Adds decoded `key` field alongside original `key_base64`.
  - For `top_prefixes`: Adds decoded `prefix` field alongside original `prefix_base64`.
  - Uses `decodeAndFormatKey()` and `decodeAndFormatPrefix()` functions for consistent binary-safe decoding.
  - File naming convention: `rdb_report_{cluster}_{batch}.json`.

### 4.5. Top 100 Big Keys

- **Component:** Responsive DaisyUI `table`.
- **Features:**
  - **Columns:** Key Name, RDB Size, Data Type, Instance, DB, Member Count, Encoding, Expiry Time.
  - **Interaction:**
    - The Key Name column supports real-time filtering.
    - All columns support click-to-sort.
    - **Export:** CSV export functionality for the current filtered and sorted data.
- **Layout:** When content overflows, the table's maximum height is `24rem` (384px), and vertical scrolling is enabled with sticky table headers for better navigation.
- **Cell (`<td>`) Enhancements:**
  - **Copyable Content:** Key names, instance names, and expiry times use the `CopyableText` component to provide truncation, tooltips, and click-to-copy functionality.
  - **Data Display:**
    - Non-UTF-8 keys are hex-escaped.
    - RDB size and member count are formatted for human readability (e.g., `1.2 MB`, `10.5k`).
    - Expiry time is displayed in the local timezone format.
  - **CSV Export:**
    - Exports all currently displayed keys (respecting search filters and sort order).
    - Includes both raw numeric values and human-readable formatted data:
      - RDB Size: Raw bytes and human-readable format (e.g., `1048576` and `1.0 MB`)
      - Member Count: Raw numbers and formatted counts (e.g., `15000` and `15.0K`)
    - Properly handles CSV escaping for special characters in key names and instance names.
    - File naming convention: `rdb_top_keys_{cluster}_{batch}.csv`.

### 4.6. Cluster Issues Insight

- **Purpose:** Proactively identifies and displays common Redis cluster operational issues to help with performance optimization and maintenance.
- **Layout:** A dedicated card positioned after the overview stats section, always visible regardless of whether issues are detected.
- **Component:** Uses DaisyUI `collapse` components for expandable issue categories and `table` components for detailed issue data.

#### 4.6.1. Issue Categories

**Big Keys Detection:**

- **Criteria:**
  - String type keys larger than 1MB
  - Non-string type keys larger than 1GB
- **Display:**
  - Expandable section with issue count badge
  - Direct text explanation below title: "String keys > 1MB, non-string keys > 1GB"
  - Responsive table showing key name, instance, database, type, and RDB size
  - Uses `CopyableText` component for key names and instance names
  - Type badges with color coding consistent with other sections

**Cross-Instance Slot Issues:**

- **Codis Cross-Instance Slot:** Detects when Codis slot data is distributed across multiple Redis instances
- **Redis Cluster Cross-Instance Slot:** Detects when Redis Cluster slot data spans multiple instances
- **Display:**
  - Warning alerts with descriptive messages about potential configuration or migration issues
  - Only appears when the respective issue is detected

#### 4.6.2. No Issues State

- **Design:** When no cluster issues are detected, displays a subtle, centered message: "No cluster issues detected."
- **Styling:** Uses muted text color (`text-base-content/60`), small font size, and italic styling to avoid drawing excessive attention
- **Purpose:** Provides confirmation that the system has performed the analysis without being visually intrusive

#### 4.6.3. Data Integration

- **Backend Integration:** Consumes `cluster_issues` data from the report generation system
- **Export Enhancement:** JSON export includes human-readable key names for big keys alongside base64-encoded data
- **Conditional Rendering:** Each issue category only displays when relevant problems are detected, maintaining a clean interface

## 5. Footer

### 5.1. Project Information Footer

- **Purpose:** Provides subtle project attribution and source code access
- **Design Philosophy:** Minimal, unobtrusive design that doesn't distract from main content
- **Layout:**
  - Positioned at the bottom of the page with adequate spacing (`py-6 mt-8`)
  - Center-aligned text for balanced appearance
- **Styling:**
  - **Font Size:** Extra small (`text-xs`) for minimal visual footprint
  - **Color:** Very low contrast (`text-base-content/40`) for subtle appearance
  - **Link:** Slightly higher contrast (`text-base-content/50`) with hover effects (`link link-hover`)
- **Content:** Simple "Generated by RDBInsight" text with link to project repository
- **Accessibility:** Link opens in new tab (`target="_blank"`) to preserve user's current session
