---
name: dev
description: "Use first for any ble-gatt development task. Gives the dev process."
---

# ble-gatt development

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

## 3. Write the code

1. Before you write or review code, read `docs/code-style.md`.
2. After coding, review the diff against its "Review checklist". Report each
   exception it asks you to explain, and whether the change is a style rule or
   a code migration.

## 4. Test

1. Run the CI commands from `.github/workflows/ci.yml` for the crates you changed.
   For `ble-gatt-iroh`, also run the clippy command in `ble-gatt-iroh/README.md`,
   "Examples and checks".
2. For more checks, use "Software checks for the current implementation"
   in `docs/hardware-verification.md`.
3. Device testing needs separate authorization from the user.
   Follow `docs/hardware-verification.md`, and ADR-0011 for Windows.
4. Report software checks and hardware acceptance as separate claims.

## 5. Update docs and hand off

1. Update the docs that describe the changed behavior, and `docs/glossary.md`
   for new terms.
2. Changes reach `main` only through pull requests.
3. For a release, follow "Releasing" in `README.md`.
