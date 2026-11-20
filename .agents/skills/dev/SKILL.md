---
name: dev
description: "Use first for any ble-gatt development task. Gives the dev process."
---

# ble-gatt development

The development pattern is **Spike and Stabilize** (Dan North). Spike:
implement the requested feature first, without writing tests, then verify it
manually against the requirements. Stabilize: only then refactor and bring the
tests up to date.

Follow these steps in order. `AGENTS.md` holds the project instructions.

## 1. Understand the change

1. Read `AGENTS.md`.
2. Read `docs/architecture.md` and find the layer that owns the change.
3. Read the ADRs in `docs/adr/` that the change touches. For a load-bearing
   file, read the doc listed for it in the README table "Load-bearing files".
4. Read `docs/glossary.md` for the terms you will use.

## 2. Decide the design

1. Apply "Design" in `AGENTS.md`.
2. When the user must choose, follow "Asking the user for a decision" in `AGENTS.md`.
3. Record a new design decision as an ADR in `docs/adr/`.

## 3. Spike: write the code

1. Before you write or review code, read `docs/code-style.md`.
2. Implement the requested feature. Do not write tests yet.

## 4. Spike: verify manually

1. Check the feature by hand against the requirements until it works as
   expected. Keep the evidence for the stabilize step.

## 5. Stabilize

1. Refactor, then review the diff against the "Review checklist" in
   `docs/code-style.md`. Report each exception it asks you to explain, and
   whether the change is a style rule or a code migration.
2. Bring the tests up to date with the `testing` skill
   (`.agents/skills/testing/SKILL.md`).

## 6. Update docs and hand off

1. Update the docs that describe the changed behavior, and `docs/glossary.md`
   for new terms.
2. Changes reach `main` only through pull requests.
3. For a release, follow "Releasing" in `README.md`.
