#!/usr/bin/env node

import * as core from "@actions/core";
import * as exec from "@actions/exec";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const BASE_OUTPUT = "parser-benchmark-base.txt";
const CURRENT_OUTPUT = "parser-benchmark-current.txt";

function repoPath(...segments) {
  return path.join(process.env.GITHUB_WORKSPACE || process.cwd(), ...segments);
}

function fileExists(filePath) {
  return fs.existsSync(filePath);
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

async function publishSummary() {
  if (process.env.GITHUB_STEP_SUMMARY && !fileExists(process.env.GITHUB_STEP_SUMMARY)) {
    fs.closeSync(fs.openSync(process.env.GITHUB_STEP_SUMMARY, "a"));
  }

  const generatedBytes = process.env.RDBINSIGHT_BENCH_GENERATED_BYTES || "67108864";

  core.summary
    .addHeading("Parser benchmark", 2)
    .addList([
      `Input: generated ${generatedBytes} byte synthetic string-record RDB unless RDBINSIGHT_BENCH_RDB is set.`,
      "Benchmark excludes disk I/O; RDB bytes are prepared before timing starts.",
    ]);

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

  try {
    const hasBaseBaseline = await benchmarkBase();
    await benchmarkCurrent(hasBaseBaseline);
  } catch (err) {
    runError = err;
  } finally {
    await publishSummary();
    await cleanupBaseWorktree();
  }

  if (runError) {
    core.setFailed(runError.message);
  }
}

main().catch((err) => {
  core.setFailed(err.message);
});
