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
> the Claude-Code memory handling and the Markdown + LaTeX solution layout carry over
> to any new course unchanged.
> 本仓库与 COMP9312 / COMP9319 共用**同一套可复用模板**(记忆管理、Markdown + LaTeX 解答布局),换课直接沿用。

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

## 📝 File Naming Convention · 文件命名约定

For every deliverable (assignment **or** project), each written-up solution comes in
two forms —— 每个交付物(作业或项目)的书面解答都提供两种形式:

- **`*_solution.md`** — readable Markdown solution · 可读的 Markdown 解答。
- **`*_solution_pdf.md`** — the same content as **LaTeX source**; copy the whole file
  into Overleaf and compile to get the submission PDF · 同样内容的 **LaTeX 源**,整份
  复制进 Overleaf 编译即得提交用 PDF。
