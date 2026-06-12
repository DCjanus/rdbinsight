#!/usr/bin/env node

import * as core from "@actions/core";
import * as exec from "@actions/exec";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const BASE_OUTPUT = "parser-benchmark-base.txt";
const CURRENT_OUTPUT = "parser-benchmark-current.txt";
const COMMENT_MARKER = "<!-- rdbinsight-parser-benchmark -->";
const CRITERION_DIR = repoPath("target", "criterion");
const BASE_CRITERION_DIR = repoPath("target", "criterion-base");
const CURRENT_CRITERION_DIR = repoPath("target", "criterion-current");
const DEFAULT_CURRENT_PROFILES =
  "string,string-int,list,list-ziplist,list-quicklist,list-quicklist2,set,set-intset,set-listpack,hash,hash-ziplist,hash-listpack,hash-zipmap,hash-metadata,hash-listpack-ex,zset,zset2,zset-ziplist,zset-listpack,mixed";

function repoPath(...segments) {
  return path.join(process.env.GITHUB_WORKSPACE || process.cwd(), ...segments);
}

function fileExists(filePath) {
  return fs.existsSync(filePath);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function copyCriterionOutput(targetPath) {
  fs.rmSync(targetPath, { recursive: true, force: true });
  if (fileExists(CRITERION_DIR)) {
    fs.cpSync(CRITERION_DIR, targetPath, { recursive: true });
  }
}

function envWith(overrides) {
  return {
    ...process.env,
    ...overrides,
  };
}

function quoteCommand(command, args) {
  return [command, ...args].join(" ");
}

async function run(command, args, options = {}) {
  const cwd = options.cwd || repoPath();
  const outputPath = options.outputPath;
  const output = outputPath ? fs.createWriteStream(outputPath) : null;

  core.info(`$ ${quoteCommand(command, args)}`);

  try {
    const exitCode = await exec.exec(command, args, {
      cwd,
      env: options.env || process.env,
      ignoreReturnCode: true,
      listeners: {
        stdout: (chunk) => output?.write(chunk),
      },
    });
    if (exitCode !== 0) {
      throw new Error(`${quoteCommand(command, args)} exited with code ${exitCode}`);
    }
  } finally {
    await new Promise((resolve) => {
      if (output) {
        output.end(resolve);
      } else {
        resolve();
      }
    });
  }
}

async function benchmarkBase() {
  const baseSha = process.env.BASE_SHA;
  if (process.env.GITHUB_EVENT_NAME !== "pull_request" || !baseSha) {
    return false;
  }

  const worktree = path.join(os.tmpdir(), `rdbinsight-base-${process.env.GITHUB_RUN_ID}`);
  await run("git", ["worktree", "add", worktree, baseSha]);

  if (!fileExists(path.join(worktree, "benches", "parser.rs"))) {
    fs.writeFileSync(BASE_OUTPUT, "Base branch has no parser benchmark; skipping base baseline.\n");
    return false;
  }

  fs.copyFileSync(repoPath("benches", "parser.rs"), path.join(worktree, "benches", "parser.rs"));
  await run(
    "cargo",
    ["+nightly", "bench", "--bench", "parser", "--", "--noplot", "--save-baseline", "base"],
      {
        cwd: worktree,
        env: envWith({
          CARGO_TARGET_DIR: repoPath("target"),
          RDBINSIGHT_BENCH_PROFILES:
            process.env.RDBINSIGHT_BENCH_PROFILES ||
            process.env.RDBINSIGHT_BENCH_PROFILE ||
            DEFAULT_CURRENT_PROFILES,
        }),
        outputPath: BASE_OUTPUT,
      },
    );
  copyCriterionOutput(BASE_CRITERION_DIR);
  return true;
}

async function benchmarkCurrent() {
  fs.rmSync(CRITERION_DIR, { recursive: true, force: true });
  await run("cargo", ["+nightly", "bench", "--bench", "parser", "--", "--noplot"], {
    env: envWith({
      RDBINSIGHT_BENCH_PROFILES:
        process.env.RDBINSIGHT_BENCH_PROFILES ||
        process.env.RDBINSIGHT_BENCH_PROFILE ||
        DEFAULT_CURRENT_PROFILES,
    }),
    outputPath: CURRENT_OUTPUT,
  });
  copyCriterionOutput(CURRENT_CRITERION_DIR);
}

function walkFiles(root, basename, out = []) {
  if (!fileExists(root)) {
    return out;
  }

  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      walkFiles(entryPath, basename, out);
    } else if (entry.name === basename) {
      out.push(entryPath);
    }
  }

  return out;
}

function benchmarkName(criterionRoot, estimatesPath) {
  const relative = path.relative(criterionRoot, estimatesPath);
  const segments = relative.split(path.sep);
  return segments.slice(0, -2).join("/");
}

function benchmarkJsonPath(name) {
  return path.join(CURRENT_CRITERION_DIR, ...name.split("/"), "new", "benchmark.json");
}

function formatDuration(ns) {
  if (!Number.isFinite(ns)) {
    return "-";
  }
  if (ns < 1_000) {
    return `${ns.toFixed(2)} ns`;
  }
  if (ns < 1_000_000) {
    return `${(ns / 1_000).toFixed(2)} us`;
  }
  if (ns < 1_000_000_000) {
    return `${(ns / 1_000_000).toFixed(2)} ms`;
  }
  return `${(ns / 1_000_000_000).toFixed(2)} s`;
}

function formatThroughput(bytes, ns) {
  if (!Number.isFinite(bytes) || !Number.isFinite(ns) || ns <= 0) {
    return "-";
  }

  const mibPerSecond = bytes / (1024 * 1024) / (ns / 1_000_000_000);
  return `${mibPerSecond.toFixed(2)} MiB/s`;
}

function formatChange(currentNs, baseNs) {
  if (!Number.isFinite(currentNs) || !Number.isFinite(baseNs) || baseNs <= 0) {
    return "-";
  }

  const change = (currentNs / baseNs - 1) * 100;
  const prefix = change > 0 ? "+" : "";
  const direction = change > 0 ? "slower" : change < 0 ? "faster" : "same";
  return `${prefix}${change.toFixed(2)}% ${direction}`;
}

function markdownTableCell(value) {
  return String(value)
    .slice(0, 200)
    .replaceAll("\\", "\\\\")
    .replaceAll("|", "\\|")
    .replaceAll("`", "\\`")
    .replaceAll("\r", " ")
    .replaceAll("\n", " ")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function shortSha(sha) {
  return (sha || "").slice(0, 7);
}

function markdownLink(label, url) {
  return `[${markdownTableCell(label)}](${url})`;
}

function commitLink(repo, sha) {
  if (!repo || !sha) {
    return null;
  }
  return markdownLink(shortSha(sha), `https://github.com/${repo}/commit/${sha}`);
}

function actionsRunLink(label, runId) {
  const repository = process.env.GITHUB_REPOSITORY;
  if (!repository || !runId) {
    return null;
  }
  return markdownLink(label, `https://github.com/${repository}/actions/runs/${runId}`);
}

function medianPointEstimate(filePath) {
  return readJson(filePath).median.point_estimate;
}

function benchmarkBytes(name) {
  const filePath = benchmarkJsonPath(name);
  if (!fileExists(filePath)) {
    return Number.NaN;
  }

  const benchmark = readJson(filePath);
  if (benchmark.throughput?.Bytes !== undefined) {
    return benchmark.throughput.Bytes;
  }
  return Number.NaN;
}

function collectBenchmarkRows(hasBaseBaseline) {
  const rows = [];

  for (const estimatesPath of walkFiles(CURRENT_CRITERION_DIR, "estimates.json")) {
    if (!estimatesPath.endsWith(`${path.sep}new${path.sep}estimates.json`)) {
      continue;
    }

    const name = benchmarkName(CURRENT_CRITERION_DIR, estimatesPath);
    const baseEstimatesPath = path.join(BASE_CRITERION_DIR, ...name.split("/"), "base", "estimates.json");
    const currentNs = medianPointEstimate(estimatesPath);
    const baseNs =
      hasBaseBaseline && fileExists(baseEstimatesPath)
        ? medianPointEstimate(baseEstimatesPath)
        : Number.NaN;
    const bytes = benchmarkBytes(name);

    rows.push({
      name,
      baseNs,
      currentNs,
      bytes,
    });
  }

  rows.sort((a, b) => a.name.localeCompare(b.name));
  return rows;
}

function benchmarkMarkdownTable(rows) {
  if (rows.length === 0) {
    return "_No Criterion benchmark estimates were found._";
  }

  const table = [
    "| Benchmark | Base median | Current median | Change | Current throughput |",
    "| --- | ---: | ---: | ---: | ---: |",
  ];

  for (const row of rows) {
    table.push(
      `| \`${markdownTableCell(row.name)}\` | ${markdownTableCell(formatDuration(row.baseNs))} | ${markdownTableCell(formatDuration(row.currentNs))} | ${markdownTableCell(formatChange(row.currentNs, row.baseNs))} | ${markdownTableCell(formatThroughput(row.bytes, row.currentNs))} |`,
    );
  }

  return table.join("\n");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function benchmarkHtmlTable(rows) {
  if (rows.length === 0) {
    return "<p><em>No Criterion benchmark estimates were found.</em></p>";
  }

  const body = rows
    .map(
      (row) => `<tr>
<td><code>${escapeHtml(row.name)}</code></td>
<td align="right">${escapeHtml(formatDuration(row.baseNs))}</td>
<td align="right">${escapeHtml(formatDuration(row.currentNs))}</td>
<td align="right">${escapeHtml(formatChange(row.currentNs, row.baseNs))}</td>
<td align="right">${escapeHtml(formatThroughput(row.bytes, row.currentNs))}</td>
</tr>`,
    )
    .join("\n");

  return `<table>
<thead>
<tr>
<th>Benchmark</th>
<th align="right">Base median</th>
<th align="right">Current median</th>
<th align="right">Change</th>
<th align="right">Current throughput</th>
</tr>
</thead>
<tbody>
${body}
</tbody>
</table>`;
}

function benchmarkNotes() {
  if (process.env.GITHUB_STEP_SUMMARY && !fileExists(process.env.GITHUB_STEP_SUMMARY)) {
    fs.closeSync(fs.openSync(process.env.GITHUB_STEP_SUMMARY, "a"));
  }

  const generatedBytes = process.env.RDBINSIGHT_BENCH_GENERATED_BYTES || "16777216";
  const profiles =
    process.env.RDBINSIGHT_BENCH_PROFILES ||
    process.env.RDBINSIGHT_BENCH_PROFILE ||
    DEFAULT_CURRENT_PROFILES;
  const inputDescription = process.env.RDBINSIGHT_BENCH_RDB
    ? `Input: external RDB from ${process.env.RDBINSIGHT_BENCH_RDB}.`
    : `Input: generated ${generatedBytes} byte synthetic RDB profiles: ${profiles}.`;

  return [
    inputDescription,
    "Benchmark excludes disk I/O; RDB bytes are prepared before timing starts.",
    "Positive change means the current PR is slower than the base commit.",
  ];
}

function pullRequestBenchmarkLinks() {
  const pullRequest = githubEvent().pull_request;
  if (!pullRequest) {
    return [];
  }

  const baseCommit = commitLink(pullRequest.base?.repo?.full_name, pullRequest.base?.sha);
  const headCommit = commitLink(pullRequest.head?.repo?.full_name, pullRequest.head?.sha);
  const workflowRun = actionsRunLink(`CI run ${process.env.GITHUB_RUN_ID}`, process.env.GITHUB_RUN_ID);
  const notes = [];

  if (baseCommit && headCommit) {
    notes.push(`Base commit: ${baseCommit}; head commit: ${headCommit}.`);
  }
  if (workflowRun) {
    notes.push(`Benchmark and comment workflow: ${workflowRun}.`);
  }

  return notes;
}

async function publishSummary(rows) {
  const notes = benchmarkNotes();

  core.summary
    .addHeading("Parser benchmark", 2)
    .addList(notes)
    .addHeading("Comparison", 3)
    .addRaw(benchmarkHtmlTable(rows), true);

  if (fileExists(BASE_OUTPUT)) {
    core.summary
      .addHeading("Base", 3)
      .addCodeBlock(fs.readFileSync(BASE_OUTPUT, "utf8"), "text");
  }

  if (fileExists(CURRENT_OUTPUT)) {
    core.summary
      .addHeading("Current", 3)
      .addCodeBlock(fs.readFileSync(CURRENT_OUTPUT, "utf8"), "text");
  }

  await core.summary.write();
}

function githubEvent() {
  if (!process.env.GITHUB_EVENT_PATH || !fileExists(process.env.GITHUB_EVENT_PATH)) {
    return {};
  }
  return readJson(process.env.GITHUB_EVENT_PATH);
}

async function githubRequest(method, route, body) {
  const token = process.env.GITHUB_TOKEN;
  const repository = process.env.GITHUB_REPOSITORY;
  if (!token || !repository) {
    throw new Error("GITHUB_TOKEN and GITHUB_REPOSITORY are required to update PR comments");
  }

  const response = await fetch(`https://api.github.com/repos/${repository}${route}`, {
    method,
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      "x-github-api-version": "2022-11-28",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${method} ${route} failed with ${response.status}: ${text}`);
  }

  return response.status === 204 ? null : response.json();
}

async function upsertPullRequestComment(rows) {
  if (process.env.GITHUB_EVENT_NAME !== "pull_request") {
    return;
  }
  if (!process.env.GITHUB_TOKEN) {
    core.info("GITHUB_TOKEN is not available; skipping parser benchmark PR comment.");
    return;
  }

  const event = githubEvent();
  const number = event.pull_request?.number;
  if (!number) {
    core.info("Pull request number is not available; skipping parser benchmark PR comment.");
    return;
  }

  const notes = [...benchmarkNotes(), ...pullRequestBenchmarkLinks()]
    .map((note) => `- ${note}`)
    .join("\n");
  const body = [
    COMMENT_MARKER,
    "## Parser benchmark",
    "",
    notes,
    "",
    benchmarkMarkdownTable(rows),
    "",
    "_This comment is updated automatically by CI._",
  ].join("\n");

  const comments = await githubRequest("GET", `/issues/${number}/comments?per_page=100`);
  const existing = comments.find((comment) => comment.body?.includes(COMMENT_MARKER));

  if (existing) {
    await githubRequest("PATCH", `/issues/comments/${existing.id}`, { body });
  } else {
    await githubRequest("POST", `/issues/${number}/comments`, { body });
  }
}

async function cleanupBaseWorktree() {
  if (!process.env.GITHUB_RUN_ID) {
    return;
  }
  const worktree = path.join(os.tmpdir(), `rdbinsight-base-${process.env.GITHUB_RUN_ID}`);
  if (!fileExists(worktree)) {
    return;
  }
  try {
    await run("git", ["worktree", "remove", "--force", worktree]);
  } catch (err) {
    core.warning(`Failed to remove base worktree: ${err.message}`);
  }
}

async function main() {
  let runError = null;
  let hasBaseBaseline = false;

  try {
    fs.rmSync(repoPath("target", "criterion"), { recursive: true, force: true });
    fs.rmSync(BASE_CRITERION_DIR, { recursive: true, force: true });
    fs.rmSync(CURRENT_CRITERION_DIR, { recursive: true, force: true });
    hasBaseBaseline = await benchmarkBase();
    await benchmarkCurrent();
  } catch (err) {
    runError = err;
  } finally {
    const rows = collectBenchmarkRows(hasBaseBaseline);
    await publishSummary(rows);
    try {
      await upsertPullRequestComment(rows);
    } catch (err) {
      core.warning(`Failed to update parser benchmark PR comment: ${err.message}`);
    }
    await cleanupBaseWorktree();
  }

  if (runError) {
    core.setFailed(runError.message);
  }
}

main().catch((err) => {
  core.setFailed(err.message);
});
