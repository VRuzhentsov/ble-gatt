# ble-gatt — Agent Instructions

A reusable, cross-platform BLE library for Rust applications.

- Layers, crates and module responsibilities: `docs/architecture.md`.
- What each term means: `docs/glossary.md`. Use its words in code and
  documents; add a term there when you introduce one.
- Decisions: `docs/adr/`.

## Design

- Place every module in one layer of `docs/architecture.md` and keep it to
  that layer's job.
- Prefer designs established projects already use (other BLE libraries,
  the platforms' own APIs, Rust idioms). Name the project you follow. Design
  something new only when nothing existing fits, and record why in an ADR.
- Copy code only from permissively licensed projects (MIT, Apache-2.0, BSD),
  with attribution. From copyleft or source-available projects (AGPL, GPL,
  BUSL), take ideas only.

## Asking the user for a decision

- Explain before asking. Define every term the decision depends on in plain
  words ("what it is"), as if the user has not met it, and add it to
  `docs/glossary.md`.
- Present each decision as options, each with a concrete example (code or a
  real scenario) and its tradeoffs spelled out in full sentences, not
  three-word labels.
- For each option, name the existing project or library that does it that
  way and show how.
- Say explicitly whether you are asking the user to choose or reporting what
  you have decided.
- When you propose or decide something yourself, still show the options,
  examples and tradeoffs behind it, so the user can check the choice.
