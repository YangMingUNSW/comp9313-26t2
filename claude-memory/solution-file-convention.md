---
name: solution-file-convention
description: COMP9313 deliverables are code / auto-marked (Ed) — source + tests, no .tex/_solution.md pair
metadata:
  node_type: memory
  type: feedback
---

**COMP9313 deliverables are code / auto-marked, not written-up PDFs.** Every deliverable so far
(project1 MRJob, project2 Spark RDD/DataFrame, project3 Spark join) is a programming task graded
by an autograder on **Ed**. The deliverable IS the **source + any tests** in the deliverable
folder — e.g. `proj1.py` + `run_tests_ed.sh`, `project2_rdd.py` / `project2_df.py` + `TESTING.md`,
`proj3.py` + reference outputs. There is **NO `<name>_solution.md` / `.tex` pair** — the
Markdown + LaTeX "written solution" convention does **not** apply to this course.

**Why:** the two-file (`_solution.md` + `.tex`) workflow only exists for human-graded PDF
deliverables. COMP9313 has none of those; producing solution write-ups here would be dead files
that no grader reads. (An older version of this note wrongly required a
`_solution.md` + `_solution_pdf.md` pair for "every deliverable"; that pattern is retired and
never fit this course.)

**How to apply:** focus on the code's correctness and on verifying it — see [[no-local-testing]]
(all 9313 debugging happens on Ed, the user runs and reports back). Keep each deliverable's
source, `spec.md`, and test artifacts together in its folder. Only if a future COMP9313
deliverable is ever a human-graded PDF would the `.tex` + `_solution.md` pair come back. See
[[comp9313-repo]].
