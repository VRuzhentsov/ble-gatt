---
name: testing
description: "Use at the stabilize step of the dev skill. Gives the ble-gatt testing process."
---

# ble-gatt testing

This is the stabilize phase of Spike and Stabilize (see the `dev` skill).
Start only after the feature is done and manually verified against the
requirements.

1. Update or add tests so they describe the final, verified behaviour,
   characterization-style: write them from the requirements and the
   manual-verification evidence, not by mirroring the implementation.
   Delete tests for behaviour that no longer exists.
2. Run only the tests affected by the change, not the whole CI suite.
   CI runs on the pull request. Candidate commands are in "Software checks
   for the current implementation" in `docs/hardware-verification.md`.
3. Device or hardware testing needs separate authorization from the user.
   Follow `docs/hardware-verification.md`, and
   `docs/adr/0011-windows-winrt-driver.md` for Windows.
4. Report software checks and hardware acceptance as separate claims.
