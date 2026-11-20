---
name: ble-gatt-code-style
description: "Use before writing, changing or reviewing ble-gatt Rust code."
---

# ble-gatt code style

The rules are in `docs/code-style.md`. This skill is the process for applying them.

## Before you write code

1. Read `docs/code-style.md`.
2. Read `docs/architecture.md` for the layer of each module you touch.
3. Read `docs/glossary.md` for names and writing rules.
4. For a load-bearing file, read the doc listed for it in the README table
   "Load-bearing files".

## While you write code

1. Find the existing type that owns the state, value or protocol.
2. Put the change on that owner, as `docs/code-style.md` describes.
3. When no owner fits, record the free-function exception for the review.

## Check your work

1. Review the diff against "Review checklist" in `docs/code-style.md`.
2. Run the CI commands from `.github/workflows/ci.yml` for the crates you changed.
3. For `ble-gatt-iroh`, also run the clippy command in `ble-gatt-iroh/README.md`,
   "Examples and checks".
4. In the review, report each free-function or separate-`impl` exception and
   whether the change is a style rule or a code migration.
