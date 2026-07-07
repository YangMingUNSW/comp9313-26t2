---
name: no-local-testing
description: Never run/test COMP9313 code locally — all debugging happens on Ed; user runs and reports back.
metadata:
  type: feedback
---

For all COMP9313 (9313) projects, do NOT run or test code locally (no `spark-submit`, `python`, `pyspark`, etc.). All debugging happens on the **Ed** platform, not the local machine.

**Why:** The runtime/cluster environment only exists on Ed; local runs are meaningless and waste effort. The user runs the code on Ed and gives feedback.

**How to apply:** Just write/edit the code as requested, then hand it back. Let the user execute on Ed and report results. Iterate based on their feedback rather than attempting local verification. Related: [[auto-push-after-changes]].
