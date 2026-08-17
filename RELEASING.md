# Release Guide for Outboxify Maintainers

This document describes the step-by-step process for publishing new releases of **Outboxify** across Java (Maven), Node.js (npm), and Python (PyPI).

---

## 1. Release Process Overview

Outboxify follows [Semantic Versioning 2.0.0](https://semver.org/).

A release is published automatically by GitHub Actions whenever a git tag matching `v*.*.*` is pushed to `main`.

```
               ┌───────────────────────┐
               │ Update Version Files  │
               └──────────┬────────────┘
                          │
               ┌──────────▼────────────┐
               │  Update CHANGELOG.md  │
               └──────────┬────────────┘
                          │
               ┌──────────▼────────────┐
               │ Commit & Push to Main │
               └──────────┬────────────┘
                          │
               ┌──────────▼────────────┐
               │  Tag & Push (v0.1.0)  │
               └──────────┬────────────┘
                          │
               ┌──────────▼────────────┐
               │ GitHub Actions Build  │
               │ & Release Packaging   │
               └───────────────────────┘
```

---

## 2. Release Steps

### Step 1: Ensure Main Branch Quality
Make sure the latest `main` branch passes all CI checks:
```bash
# Java
mvn clean test

# Node.js
npm test --prefix node

# Python
pytest python/tests
```

### Step 2: Bump Versions Across Modules
Update the version strings across all package manifests:
- **Java**: `pom.xml`, `java/*/pom.xml`, `examples/java/*/pom.xml`
- **Node.js**: `node/package.json`, `node/*/package.json`, `examples/node/*/package.json`
- **Python**: `python/pyproject.toml`, `examples/python/*/pyproject.toml`

### Step 3: Update `CHANGELOG.md`
Move unreleased notes under a new section `## [X.Y.Z] - YYYY-MM-DD`.

### Step 4: Commit and Push
```bash
git commit -am "chore(release): prepare v0.1.0 release"
git push origin main
```

### Step 5: Create and Push Git Tag
```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

### Step 6: Automated GitHub Release
The [Release Pipeline](.github/workflows/release.yml) will trigger automatically:
1. Compiles and packages Java JARs (`io.outboxify`).
2. Builds and packs Node.js npm packages (`@outboxify/*`).
3. Builds Python wheels and source distributions (`outboxify`).
4. Creates a GitHub Release with auto-generated release notes and attached distribution assets.

---

## 3. Manual Workflow Dispatch

Releases can also be triggered manually without pushing a tag via GitHub Actions:
1. Navigate to **Actions** -> **Release & Packaging Pipeline**.
2. Click **Run workflow**.
3. Specify the tag name (e.g. `v0.1.0`).
