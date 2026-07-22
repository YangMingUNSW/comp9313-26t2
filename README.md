<div align="center">

# COMP9313 · Big Data Management
### 大数据管理 · UNSW 课程作业仓库

![UNSW](https://img.shields.io/badge/UNSW-COMP9313-FFD200?style=flat-square&labelColor=000000)
![Topic](https://img.shields.io/badge/Topic-Big%20Data-4c8bf5?style=flat-square)
![Python](https://img.shields.io/badge/Python-MRJob-3776AB?style=flat-square&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-RDD%20%26%20DataFrame-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Paradigm](https://img.shields.io/badge/Paradigm-MapReduce%20%2B%20Spark-ff9900?style=flat-square)

</div>

> **EN** — Personal cross-platform workspace for everything in **COMP9313 — Big Data Management** (UNSW): assignments, projects, and any written deliverable.
>
> **中文** — 这是 **COMP9313（大数据管理）** 的个人作业仓库,跨平台同步课程里的作业、项目及一切书面产出。

> 🧩 This repo follows the **same reusable template** as the COMP9312 / COMP9319 repos —
> the Claude-Code memory handling and the one-folder-per-deliverable layout carry over to any
> new course unchanged. **This course's deliverables are all code** (MRJob / Spark), graded by
> an autograder on Ed — see the deliverable-type note below.
> 本仓库与 COMP9312 / COMP9319 共用**同一套可复用模板**(记忆管理、每个交付物独立文件夹的布局),换课直接沿用。**本课交付物全部为代码**(MRJob / Spark),由 Ed 上的自动评测器评分,详见下方「交付物类型」说明。

---

## 📖 Overview · 简介

Coursework for COMP9313. Each deliverable lives in its own top-level folder, named
for what it actually is — e.g. `assignment1/`, `project1/` — added as the course
progresses.

本仓库汇总 COMP9313 的课程产出。每个交付物各占一个顶层文件夹,按实际名称命名
(如 `assignment1/`、`project1/`),随课程推进逐步加入。

## 📂 Structure · 目录结构

| Folder · 文件夹 | Contents · 内容 |
| --- | --- |
| [`project1/`](project1/) | **Latency Stability Analysis** (12 marks) — MRJob / MapReduce job that flags IoT devices whose **daily** average latency deviates from their **overall** average beyond a threshold `T`.<br>**延迟稳定性分析** — 用 MRJob / MapReduce 找出「单日平均延迟」相对「整体平均延迟」超过阈值 `T` 的 IoT 设备。<br>核心 `proj1.py`,题面 `spec.md`,数据 `latency2401*.csv` / `sample.csv`,测试脚本 `run_tests_ed.sh`。 |
| [`project2/`](project2/) | **Indoor Air Quality Risk Analysis** (16 marks) — Apache Spark job scoring each sensor reading (CO₂ / VOC / PM2.5) and reporting each device's **risky dates**; delivered as **two** solutions, one RDD-only and one DataFrame-only.<br>**室内空气质量风险分析** — 用 Spark 对每条读数计算风险分,输出各设备的高风险日期;分别提供仅 RDD 与仅 DataFrame 两个版本。<br>`project2_rdd.py` / `project2_df.py`,题面 `spec.md`,数据 `air_bedroom.csv` / `sample.csv`,参考输出 `output_airbedroom_2.8`,测试说明 `TESTING.md`。 |
| [`project3/`](project3/) | **Scalable Spatio-Textual Similarity Joins** (22 marks) — Spark job finding record pairs within Euclidean distance `d` **and** Jaccard similarity `s`, using frequency sorting (broadcast token→rank), **prefix filtering** and a **grid index** to avoid the O(N²) pairwise blow-up.<br>**可扩展的时空相似性连接** — 用 Spark 找出欧氏距离 ≤ `d` 且 Jaccard 相似度 ≥ `s` 的记录对;以频率排序 + 前缀过滤 + 网格索引避免 O(N²) 两两比较。<br>核心 `proj3.py`,题面 `spec.md`,数据 `A.txt` / `B.txt` / `*_large.txt`,参考输出 `output_d=100_s=0.5.txt`。 |
| [`claude-memory/`](claude-memory/) | Claude Code 跨机记忆镜像(见下方说明)。 |

## 🧠 `claude-memory/`

A mirror of Claude Code's persistent memory for this project, committed to the repo
so prior context follows you across machines.

这是 Claude Code 针对本项目的持久记忆镜像,随仓库一起提交,让 AI 的历史上下文能跨机器
跟随你。在新机器上 clone 后,把这些文件复制到 Claude Code 的项目记忆目录
(`~/.claude/projects/<project-key>/memory/`,`project-key` 由该机器上的项目绝对路径推导)。

## 📝 Deliverable Type · 交付物类型

Every COMP9313 deliverable is a **code / auto-marked** task graded by an autograder on **Ed** —
so the deliverable **is** the source **+ its tests** in each folder (`proj1.py` + `run_tests_ed.sh`,
`project2_rdd.py` / `project2_df.py` + `TESTING.md`, `proj3.py` + reference outputs). There is
**no** `.tex` / `_solution.md` write-up pair — that Markdown + LaTeX solution convention applies
only to human-graded PDF deliverables, and this course has none.

本课每个交付物都是**代码 / 自动评测**任务,由 **Ed** 上的自动评测器评分——因此交付物**就是**
各文件夹内的**源码 + 测试**(`proj1.py` + `run_tests_ed.sh`、`project2_rdd.py` / `project2_df.py`
+ `TESTING.md`、`proj3.py` + 参考输出)。**没有** `.tex` / `_solution.md` 这一对书面解答文件——
那套 Markdown + LaTeX 约定只用于人工评分的 PDF 交付物,而本课没有此类交付物。
