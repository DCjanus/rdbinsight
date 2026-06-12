#!/usr/bin/env node

import * as core from "@actions/core";
import * as exec from "@actions/exec";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const BASE_OUTPUT = "parser-benchmark-base.txt";
const CURRENT_OUTPUT = "parser-benchmark-current.txt";
const COMMENT_MARKER = "<!-- rdbinsight-parser-benchmark -->";

function repoPath(...segments) {
  return path.join(process.env.GITHUB_WORKSPACE || process.cwd(), ...segments);
}

function fileExists(filePath) {
  return fs.existsSync(filePath);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
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

  await run(
    "cargo",
    ["+nightly", "bench", "--bench", "parser", "--", "--noplot", "--save-baseline", "base"],
    {
      cwd: worktree,
      env: envWith({ CARGO_TARGET_DIR: repoPath("target") }),
      outputPath: BASE_OUTPUT,
    },
  );
  return true;
}

async function benchmarkCurrent(hasBaseBaseline) {
  const args = ["+nightly", "bench", "--bench", "parser", "--", "--noplot"];
  if (hasBaseBaseline) {
    args.push("--baseline", "base");
  }
  await run("cargo", args, { outputPath: CURRENT_OUTPUT });
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

function benchmarkJsonPath(criterionRoot, name) {
  return path.join(criterionRoot, ...name.split("/"), "new", "benchmark.json");
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

function medianPointEstimate(filePath) {
  return readJson(filePath).median.point_estimate;
}

function benchmarkBytes(criterionRoot, name) {
  const filePath = benchmarkJsonPath(criterionRoot, name);
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
  const criterionRoot = repoPath("target", "criterion");
  const rows = [];

  for (const estimatesPath of walkFiles(criterionRoot, "estimates.json")) {
    if (!estimatesPath.endsWith(`${path.sep}new${path.sep}estimates.json`)) {
      continue;
    }

    const name = benchmarkName(criterionRoot, estimatesPath);
    const baseEstimatesPath = path.join(path.dirname(path.dirname(estimatesPath)), "base", "estimates.json");
    const currentNs = medianPointEstimate(estimatesPath);
    const baseNs =
      hasBaseBaseline && fileExists(baseEstimatesPath)
        ? medianPointEstimate(baseEstimatesPath)
        : Number.NaN;
    const bytes = benchmarkBytes(criterionRoot, name);

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

function benchmarkTable(rows) {
  if (rows.length === 0) {
    return "_No Criterion benchmark estimates were found._";
  }

  const table = [
    "| Benchmark | Base median | Current median | Change | Current throughput |",
    "| --- | ---: | ---: | ---: | ---: |",
  ];

  for (const row of rows) {
    table.push(
      `| \`${row.name}\` | ${formatDuration(row.baseNs)} | ${formatDuration(row.currentNs)} | ${formatChange(row.currentNs, row.baseNs)} | ${formatThroughput(row.bytes, row.currentNs)} |`,
    );
  }

  return table.join("\n");
}

function benchmarkNotes() {
  if (process.env.GITHUB_STEP_SUMMARY && !fileExists(process.env.GITHUB_STEP_SUMMARY)) {
    fs.closeSync(fs.openSync(process.env.GITHUB_STEP_SUMMARY, "a"));
  }

  const generatedBytes = process.env.RDBINSIGHT_BENCH_GENERATED_BYTES || "16777216";
  const profiles =
    process.env.RDBINSIGHT_BENCH_PROFILES ||
    process.env.RDBINSIGHT_BENCH_PROFILE ||
    "string,list,set,hash,zset,zset2,mixed";
  const inputDescription = process.env.RDBINSIGHT_BENCH_RDB
    ? `Input: external RDB from ${process.env.RDBINSIGHT_BENCH_RDB}.`
    : `Input: generated ${generatedBytes} byte synthetic RDB profiles: ${profiles}.`;

  return [
    inputDescription,
    "Benchmark excludes disk I/O; RDB bytes are prepared before timing starts.",
    "Positive change means the current PR is slower than the base commit.",
  ];
}

async function publishSummary(rows) {
  const notes = benchmarkNotes();

  core.summary
    .addHeading("Parser benchmark", 2)
    .addList(notes)
    .addHeading("Comparison", 3)
    .addRaw(`${benchmarkTable(rows)}\n`);

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

  const notes = benchmarkNotes().map((note) => `- ${note}`).join("\n");
  const body = [
    COMMENT_MARKER,
    "## Parser benchmark",
    "",
    notes,
    "",
    benchmarkTable(rows),
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
    hasBaseBaseline = await benchmarkBase();
    await benchmarkCurrent(hasBaseBaseline);
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
