#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

function fail(message) {
  console.error(`::error::${message}`);
  process.exitCode = 1;
}

function readFileUtf8(filePath) {
  return fs.readFileSync(filePath, { encoding: "utf8" });
}

function parseTomlForPackageVersion(tomlText) {
  // Minimal TOML parser focused on extracting [package].version
  // Supports comments (# ...), section headers [section], and key = "value"
  let inPackage = false;
  const lines = tomlText.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.replace(/#.*/, "").trim();
    if (!line) continue;

    const sectionMatch = line.match(/^\[([^\]]+)\]$/);
    if (sectionMatch) {
      inPackage = sectionMatch[1] === "package";
      continue;
    }

    if (!inPackage) continue;

    const kvMatch = line.match(/^([A-Za-z0-9_-]+)\s*=\s*(.+)$/);
    if (!kvMatch) continue;

    const key = kvMatch[1];
    let valueRaw = kvMatch[2].trim();
    if (key !== "version") continue;

    // Strip quotes if present
    if (valueRaw.startsWith('"')) {
      const q = valueRaw.indexOf('"', 1);
      const lastQ = valueRaw.lastIndexOf('"');
      if (lastQ > 0) {
        return valueRaw.slice(1, lastQ);
      }
    }
    // Fallback: unquoted value until comment or EOL
    return valueRaw.replace(/#.*/, "").trim();
  }
  return "";
}

function getCargoVersion(repoRoot) {
  const cargoPath = path.join(repoRoot, "Cargo.toml");
  const tomlText = readFileUtf8(cargoPath);
  const version = parseTomlForPackageVersion(tomlText);
  if (!version) throw new Error("Failed to parse version from Cargo.toml");
  return version;
}

function getChangelogVersionAndUnreleasedClean(repoRoot) {
  const changelogPath = path.join(repoRoot, "CHANGELOG.md");
  const content = readFileUtf8(changelogPath);
  const lines = content.split(/\r?\n/);

  let latestReleasedVersion = "";
  let inUnreleased = false;
  let unreleasedHasContent = false;

  for (let i = 0; i < lines.length; i += 1) {
    const raw = lines[i];
    const line = raw.trim();

    if (line.startsWith("## [")) {
      const header = line;
      const m = header.match(/^## \[([^\]]+)\]/);
      if (m) {
        const sectionName = m[1];
        if (sectionName === "Unreleased") {
          inUnreleased = true;
          unreleasedHasContent = false; // reset when entering
        } else {
          if (!latestReleasedVersion) latestReleasedVersion = sectionName;
          inUnreleased = false; // leaving unreleased
        }
      }
      continue;
    }

    if (inUnreleased) {
      // Any non-empty line counts as content
      if (line.length > 0) {
        unreleasedHasContent = true;
      }
    }
  }

  return { latestReleasedVersion, unreleasedIsEmpty: !unreleasedHasContent };
}

function main() {
  const repoRoot = process.cwd();
  const inputTag = process.argv[2] || process.env.GITHUB_REF_NAME || "";
  if (!inputTag) {
    fail("Tag is not provided (pass as arg or set GITHUB_REF_NAME)");
    return;
  }
  const tagVersion = inputTag.replace(/^v/, "");

  let cargoVersion = "";
  try {
    cargoVersion = getCargoVersion(repoRoot);
  } catch (err) {
    fail("Failed to parse version from Cargo.toml");
    return;
  }

  const { latestReleasedVersion, unreleasedIsEmpty } =
    getChangelogVersionAndUnreleasedClean(repoRoot);
  if (!latestReleasedVersion) {
    fail("Failed to parse latest released version from CHANGELOG.md");
    return;
  }

  let ok = true;
  if (tagVersion !== cargoVersion) {
    fail(`Tag version (${tagVersion}) != Cargo.toml version (${cargoVersion})`);
    ok = false;
  }
  if (tagVersion !== latestReleasedVersion) {
    fail(
      `Tag version (${tagVersion}) != CHANGELOG latest version (${latestReleasedVersion})`
    );
    ok = false;
  }
  if (!unreleasedIsEmpty) {
    fail(`CHANGELOG.md has non-empty Unreleased section on tag ${inputTag}`);
    ok = false;
  }

  if (!ok) {
    console.log(`TAG=${tagVersion}`);
    console.log(`CARGO=${cargoVersion}`);
    console.log(`CHANGELOG=${latestReleasedVersion}`);
    process.exit(1);
  }

  console.log(`Version check passed: ${tagVersion}`);
}

main();


