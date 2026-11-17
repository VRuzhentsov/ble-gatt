---
name: ble-gatt-code-style
description: "Use for ble-gatt code changes and reviews. Apply OOP."
---

# ble-gatt code style

Read `docs/architecture.md`, "Object-oriented code", before code changes or
review. Read `docs/glossary.md` for names and STE-based writing rules.

## Procedure

1. Find the existing type that owns the state, value or protocol.
2. Put behavior on that type. Use Rust structs or enums with `impl` methods
   and associated functions; use traits for shared contracts.
3. Put state access on its owner, rather than passing `inner` or a context
   object to a free helper.
4. Put parsing, validation and conversion on the value type or its codec,
   even when the operation is pure.
5. Reuse existing owners. Give a new type a real responsibility instead of
   wrapping each free function in a separate type.
6. Keep required framework entry points thin. For a generic free helper with
   no domain owner, explain the exception in review.
7. Place each type near its inherent implementation. Group related methods
   together, including encode/decode pairs. Consolidate compatible inherent
   `impl` blocks instead of leaving one block at each former function location.
8. Keep trait implementations separate. Group public compatibility functions
   and foreign entry points apart from owning implementations. Preserve each
   item's attributes, conditional compilation, visibility and documentation.
   Separate inherent blocks when generic bounds or conditions require them;
   explain that separation in review.
9. Reuse existing interfaces. Add a trait for a concrete shared contract or
   interchangeable implementation, rather than for every internal object.
10. Check changed free functions and implementation layout against these rules.
    Preserve layer boundaries, wire bytes, ownership and lifecycle behavior
    during a style-only change.

## Verification

Review the diff for an owner for each behavior, limited free-function
exceptions, consistent vocabulary and the existing project's required checks.
Check that types and related methods form coherent groups. Check that trait
implementations and grouped compatibility or foreign entry points remain separate.
Compare moved bodies and effective conditions, not only the number of `impl` blocks.
Distinguish a recorded design rule from an implemented migration of old code.
