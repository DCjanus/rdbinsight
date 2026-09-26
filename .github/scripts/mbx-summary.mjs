#!/usr/bin/env node

import fs from "node:fs";

const routineProbes = new Set([
  "compiler-query",
  "standard-input",
  "cc-compiler-query",
  "cc-standard-input",
]);

function duration(nanoseconds) {
  return `${(nanoseconds / 1e9).toFixed(1)} s`;
}

function bytes(count) {
  if (count >= 1073741824) return `${(count / 1073741824).toFixed(1)} GiB`;
  return `${(count / 1048576).toFixed(1)} MiB`;
}

function reportRow(label, file) {
  if (!fs.existsSync(file)) {
    return {
      row: `| ${label} | No completed mbx build report | - | - | - | - | - |`,
      bypasses: "",
    };
  }

  const report = JSON.parse(fs.readFileSync(file, "utf8"));
  if (report.version !== 5) {
    throw new Error(`Unsupported mbx stats report version in ${file}: ${report.version}`);
  }
  const bypasses = Object.entries(report.bypasses)
    .filter(([reason, count]) => !routineProbes.has(reason) && count > 0)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  const bypassCount = bypasses.reduce((sum, [, count]) => sum + count, 0);
  const fields = [
    label,
    `${report.hits} / ${report.misses}`,
    report.unconsulted,
    report.incremental_compilations,
    bypassCount,
    duration(report.estimated_compiler_duration_avoided_ns),
    bytes(report.stored_bytes),
  ];
  return {
    row: `| ${fields.join(" | ")} |`,
    bypasses: bypasses.length
      ? `- ${label} bypass: ${bypasses.map(([reason, count]) => `${reason} ${count}`).join(", ")}`
      : "",
  };
}

const entries = process.argv.slice(2).map((argument) => {
  const separator = argument.indexOf("=");
  if (separator < 1) throw new Error(`Expected label=path, got ${argument}`);
  return reportRow(argument.slice(0, separator), argument.slice(separator + 1));
});
if (!entries.length || !process.env.GITHUB_STEP_SUMMARY) {
  throw new Error("Summary path and at least one mbx report are required");
}

const summary = [
  "## mr boxington build cache",
  "",
  "| Build | Hits / misses | Not looked up | Incremental | Bypassed | Estimated compiler time avoided | Stored locally |",
  "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
  ...entries.map(({ row }) => row),
  "",
  ...entries.map(({ bypasses }) => bypasses).filter(Boolean),
  "",
  "These figures describe compiler actions observed by mbx. Cargo reuse and GitHub cache archive transfer are excluded; avoided compiler time is summed across compilations, not job time saved.",
  "",
].join("\n");
fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
