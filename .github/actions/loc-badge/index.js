#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { makeBadge } = require("badge-maker");

function getInput(name, defaultValue = "") {
  const key = `INPUT_${name.replace(/ /g, "_").toUpperCase()}`;
  return process.env[key] ?? defaultValue;
}

function parseBool(value, defaultValue = false) {
  if (value == null || value === "") return defaultValue;
  const v = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(v);
}

function run(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, {
    encoding: "utf8",
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    ...options,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const stderr = result.stderr ? `\n${result.stderr}` : "";
    throw new Error(`Command failed: ${cmd} ${args.join(" ")}${stderr}`);
  }
  return result.stdout ?? "";
}

function runStatus(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, {
    encoding: "utf8",
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    ...options,
  });
  if (result.error) throw result.error;
  return result;
}

function setOutput(name, value) {
  const outputFile = process.env.GITHUB_OUTPUT;
  if (outputFile) {
    fs.appendFileSync(outputFile, `${name}=${value}\n`);
  } else {
    console.log(`::set-output name=${name}::${value}`);
  }
}

function appendSummary(lines) {
  const summary = process.env.GITHUB_STEP_SUMMARY;
  if (!summary) return;
  fs.appendFileSync(summary, `${lines.join("\n")}\n`);
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function escapeXml(value) {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function normalizeRemoteWithToken(token) {
  if (!token) return;

  const origin = run("git", ["remote", "get-url", "origin"], { capture: true }).trim();
  let normalized = "";

  if (origin.startsWith("git@github.com:")) {
    normalized = `https://x-access-token:${encodeURIComponent(token)}@github.com/${origin.slice("git@github.com:".length)}`;
  } else if (origin.startsWith("https://github.com/")) {
    normalized = origin.replace("https://", `https://x-access-token:${encodeURIComponent(token)}@`);
  } else if (origin.startsWith("http://github.com/")) {
    normalized = origin.replace("http://", `https://x-access-token:${encodeURIComponent(token)}@`);
  }

  if (normalized) {
    run("git", ["remote", "set-url", "origin", normalized]);
  }
}

function parseTotalsFromTokei(jsonText) {
  const parsed = JSON.parse(jsonText);

  if (parsed.Total) {
    return {
      code: Number(parsed.Total.code || 0),
      comments: Number(parsed.Total.comments || 0),
      blanks: Number(parsed.Total.blanks || 0),
    };
  }

  const total = { code: 0, comments: 0, blanks: 0 };
  for (const [lang, stat] of Object.entries(parsed)) {
    if (lang === "Total" || !stat || typeof stat !== "object") continue;
    total.code += Number(stat.code || 0);
    total.comments += Number(stat.comments || 0);
    total.blanks += Number(stat.blanks || 0);
  }
  return total;
}

function generateSvg(label, formattedValue, style) {
  const normalized = style.trim().toLowerCase();
  const supported = new Set(["flat", "flat-square", "for-the-badge", "plastic", "social"]);
  if (!supported.has(normalized)) {
    throw new Error(`Unsupported style: ${style} (expected: flat|flat-square|for-the-badge|plastic|social)`);
  }
  return makeBadge({
    label: escapeXml(label),
    message: escapeXml(formattedValue),
    color: "4c1",
    style: normalized,
  });
}

async function main() {
  const badgePath = getInput("badge_path", ".github/badges/loc.svg");
  const scope = getInput("scope", "code");
  const label = getInput("label", "lines of code");
  const style = getInput("style", "flat");
  const exclude = getInput("exclude", "");
  const dryRun = parseBool(getInput("dry_run", "false"), false);
  const commitMessage = getInput("commit_message", "ci(badge): refresh loc badge");
  const commitUserName = getInput("commit_user_name", "github-actions[bot]");
  const commitUserEmail = getInput("commit_user_email", "41898282+github-actions[bot]@users.noreply.github.com");
  const token = getInput("github_token", "");
  const targetBranchInput = getInput("target_branch", "").trim();

  fs.mkdirSync(path.dirname(badgePath), { recursive: true });

  const excludePatterns = new Set(
    exclude
      .split(",")
      .map((it) => it.trim())
      .filter(Boolean),
  );
  excludePatterns.add(badgePath);

  const tokeiArgs = [".", "--output", "json"];
  for (const pattern of excludePatterns) {
    tokeiArgs.push("--exclude", pattern);
  }

  const tokeiJson = run("tokei", tokeiArgs, { capture: true });
  const totals = parseTotalsFromTokei(tokeiJson);

  let value = 0;
  if (scope === "code") {
    value = totals.code;
  } else if (scope === "code_comments") {
    value = totals.code + totals.comments;
  } else if (scope === "all") {
    value = totals.code + totals.comments + totals.blanks;
  } else {
    throw new Error(`Unsupported scope: ${scope} (expected: code|code_comments|all)`);
  }

  const valueText = formatNumber(value);
  const svg = generateSvg(label, valueText, style);
  fs.writeFileSync(badgePath, svg, "utf8");

  const changed = run("git", ["status", "--porcelain", "--", badgePath], { capture: true }).trim().length > 0;
  let committed = false;
  let pushed = false;
  let commitSha = "";

  if (changed && !dryRun) {
    run("git", ["config", "user.name", commitUserName]);
    run("git", ["config", "user.email", commitUserEmail]);
    run("git", ["add", badgePath]);

    const stagedChanged = runStatus("git", ["diff", "--cached", "--quiet"]).status !== 0;
    if (stagedChanged) {
      run("git", ["commit", "-m", commitMessage]);
      committed = true;
      commitSha = run("git", ["rev-parse", "--short", "HEAD"], { capture: true }).trim();

      try {
        normalizeRemoteWithToken(token);
        const branch = targetBranchInput || process.env.GITHUB_HEAD_REF || process.env.GITHUB_REF_NAME;
        if (!branch) throw new Error("Cannot determine target branch for push.");
        run("git", ["push", "origin", `HEAD:${branch}`]);
        pushed = true;
      } catch (error) {
        const msg = String(error && error.message ? error.message : error);
        console.log(`::warning::LOC badge push failed: ${msg}`);
        appendSummary([
          "### LOC badge update warning",
          "",
          `Computed LOC value \`${valueText}\` and updated \`${badgePath}\`, but push failed:`,
          "",
          "```text",
          msg,
          "```",
        ]);
      }
    }
  }

  const summaryLines = [];
  if (!changed) {
    summaryLines.push("### LOC badge unchanged");
    summaryLines.push("");
    summaryLines.push(`No update needed for \`${badgePath}\`.`);
    summaryLines.push(`Current value remains **${valueText}** (\`scope=${scope}\`, \`style=${style}\`).`);
  } else if (changed && dryRun) {
    summaryLines.push("### LOC badge generated (dry run)");
    summaryLines.push("");
    summaryLines.push(`Generated \`${badgePath}\` with **${valueText}** (\`scope=${scope}\`, \`style=${style}\`).`);
    summaryLines.push("Dry run is enabled, so no commit or push was attempted.");
  } else if (pushed) {
    summaryLines.push("### LOC badge updated");
    summaryLines.push("");
    summaryLines.push(`Updated \`${badgePath}\` to **${valueText}** (\`scope=${scope}\`, \`style=${style}\`).`);
    summaryLines.push(`- Commit: \`${commitSha}\``);
    summaryLines.push(`- Push: succeeded to \`${targetBranchInput || process.env.GITHUB_HEAD_REF || process.env.GITHUB_REF_NAME || "unknown"}\``);
  } else if (committed) {
    summaryLines.push("### LOC badge committed, but push failed");
    summaryLines.push("");
    summaryLines.push(`Generated \`${badgePath}\` with **${valueText}** (\`scope=${scope}\`, \`style=${style}\`) and created commit \`${commitSha}\`.`);
    summaryLines.push("Please check branch write permissions or branch protection settings.");
  } else {
    summaryLines.push("### LOC badge changed");
    summaryLines.push("");
    summaryLines.push(`Detected changes for \`${badgePath}\` with value **${valueText}**, but nothing was committed.`);
  }
  appendSummary(summaryLines);

  setOutput("changed", String(changed));
  setOutput("committed", String(committed));
  setOutput("pushed", String(pushed));
  setOutput("value", String(value));
}

main().catch((error) => {
  const message = String(error && error.message ? error.message : error);
  console.error(message);
  console.log(`::error::${message.replace(/\n/g, " ")}`);
  process.exit(1);
});
