# Code style

These rules apply to new and changed code. Names, comments, logs and documents
also follow the [writing and naming rules](glossary.md#writing-and-naming-rules).
Layer boundaries are in [the architecture](architecture.md).

## Object-oriented code

Use object-oriented programming (OOP) as the default for new and changed code.
In Rust, use structs or enums with `impl` methods and associated functions.
Put behavior on the type that owns its state, rules or protocol. Use traits for
shared contracts.

- Put state access on the owning object. A helper that takes `inner` or a
  context object belongs on that object or its existing service.
- Put protocol parsing, validation and conversion on the protocol type or its
  codec. A pure operation still has an owner.
- Reuse existing types and traits. Give a new type a real responsibility,
  rather than making one wrapper type for each function.
- Keep language and framework entry points as thin calls to owning objects.
  Keep a generic free helper only when no domain type owns the operation;
  explain that exception in review.
- Use existing interfaces. Add traits for concrete shared contracts or interchangeable
  implementations, rather than for every internal object.

## Source layout

- Give every platform driver the same layout: `drivers/<platform>/mod.rs`,
  with that driver's submodules as files next to it.
- Place a type near its inherent implementation. Group related methods, such
  as encode/decode pairs, and consolidate compatible `impl` blocks instead of
  leaving one block at each former function location.
- Keep trait implementations separate. Group public compatibility functions and
  foreign entry points apart from owning implementations.
- Preserve each item's attributes, visibility, conditional compilation and
  documentation when you move it.
- Separate inherent blocks only when generic bounds or conditions require it.
  Explain that separation in review.
- Split a module by responsibility when a file goes past about 2,500 lines.
  Known exceptions to split later: `drivers/linux/mod.rs`,
  `drivers/android/mod.rs`, `connection/managed_tests.rs`,
  `ble-gatt-iroh/src/lib.rs`, `connection/managed_worker.rs`.
- Functions: clippy `too_many_lines` at 400 lines; lower it as long functions are split.

## Style-only changes

- Keep layer boundaries, current interfaces, wire bytes, ownership and lifecycle
  behavior. A separate implementation change updates them.
- A recorded design rule is not an implemented migration of old code. State
  which one a change is.

## Review checklist

- Each behavior has an owner. Free functions are limited to recorded exceptions.
- Vocabulary agrees with the [glossary](glossary.md).
- Types and related methods form coherent groups.
- Trait implementations and grouped compatibility or foreign entry points stay separate.
- Moved bodies and their effective conditions are unchanged, not only the
  number of `impl` blocks.
