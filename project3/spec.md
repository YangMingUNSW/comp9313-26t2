# Scalable Spatio-Textual Similarity Joins (22 marks)

## Background

In modern data science, joining datasets based on similarity is a critical task. This challenge is particularly complex and relevant in the domain of **Geographic Information Systems (GIS)** and location-based services. Real-world entities, such as restaurants, landmarks, or social media posts, are often described by both their physical location and a rich set of textual attributes. The ability to connect records from different data sources—for instance, matching Foursquare venues with Google Maps listings, or linking tweets to news articles about the same event—is crucial for data enrichment, entity resolution, and contextual analysis.

This project tackles the problem of performing a **spatio-textual similarity join**. You will develop a scalable system to identify pairs of records from two distinct datasets that are not only **spatially close** but also **textually similar**. This requires moving beyond simple database joins and implementing advanced, two-dimensional filtering techniques on the Apache Spark parallel computing framework.

## Task

Given two sets of records, **Dataset A** and **Dataset B**, where each record `R = (L, S)` has two attributes: a spatial location in 2D space (i.e., `R.L` is a coordinate `(x, y)`) and a textual description (i.e., `R.S` is a set of terms/words).

The task is to find all pairs of records, `(R_A, R_B)`, where `R_A` is in Dataset A and `R_B` is in Dataset B, which satisfy **both** of the following conditions simultaneously:

- Their **Euclidean distance** is less than or equal to a given threshold `d`.
- Their **Jaccard Similarity** on the set of terms is greater than or equal to a given threshold `s`.

Their **Euclidean distance** is less than or equal to a given threshold `d`:

```text
sqrt( (R_A.L.x - R_B.L.x)^2 + (R_A.L.y - R_B.L.y)^2 ) <= d
```

Their **Jaccard Similarity** is larger than or equal to a given threshold `s`, that is:

```text
|R_A.S ∩ R_B.S| / |R_A.S ∪ R_B.S| >= s
```

## Sample Data

Given the following two sample files, `A.txt` and `B.txt`, and the thresholds `d = 2.0` and `s = 0.5`:

File: `A.txt`

```text
A0#(1,1)#apple banana orange
A1#(10,10)#grape kiwi pear
A2#(2,1)#apple banana kiwi
```

File: `B.txt`

```text
B0#(1,2)#apple banana grape
B1#(11,11)#kiwi pear mango
B2#(8,8)#apple lemon
```

## Output Format

The output file contains all qualifying pairs along with their calculated distance and similarity. The output format is `(recordA.id,recordB.id):distance_value,jaccard_similarity_value`.

- Each pair must consist of one record from Dataset A and one from Dataset B.
- For the distance and similarity values, round the results to 6 decimal places.
- There should be no duplicate pairs in the output.
- **Sorting Rule:** The output pairs must be strictly sorted according to the following two-level rule. The sorting must be based on the **numerical value** of the IDs, not their string representation (e.g., `A10` should come after `A2`).
  - **Primary Key:** Sort in ascending order based on the numerical value of the ID from Dataset A (e.g., for `A2` and `A10`, sort by `2` and `10`).
  - **Secondary Key:** For pairs with the same `recordA.id`, sort in ascending order based on the numerical value of the ID from Dataset B.

Given the sample datasets above with the threshold `d = 2.0` and `s = 0.5`, the output result should be:

```text
(A0,B0):1.0,0.5
(A1,B1):1.414214,0.5
(A2,B0):1.414214,0.5
```

We also released larger datasets `A_large.txt`, `B_large.txt` and output with `d = 100` and `s = 0.5` in `output_d=100_s=0.5.txt` for your reference.

## Code and Execution

The code template has been provided in `proj3.py`. Your code should take five parameters: the path to the first input file, the path to the second input file, the output folder, the distance threshold `d`, and the similarity threshold `s`. You must use the following command to run your code:

```bash
spark-submit proj3.py <input_A_path> <input_B_path> <output_path> <d> <s>
```

## Some Notes

- You must design an **exact** approach to finding similar records (*Please revisit Week 8 slides for more tips*).
- You need to use a **grid index** to perform the distance join.
- For similarity join, check the paper mentioned in the slides *Efficient Parallel Set-Similarity Joins Using MapReduce. SIGMOD'10*.
- **You cannot compute the pairwise similarities directly (this method has O(N^2) complexity)!!! You cannot use the naive solution, as shown in Lecture 8.1 slides page 21, which is also considered a pairwise solution.**
- Regular Python programming is not permitted (must use RDD or DataFrame, no third-party libraries).
- `Cache()`/`Persist()` must **not** be used anywhere in your code.
- When testing the correctness and efficiency of submissions, all the code will be run with *two local threads* using the default setting of Spark. Please be careful with your runtime and memory usage.
- **Input File Format & Assumptions:** <u>You can assume that the input file structure is consistent and well-formed</u>. Each line will strictly follow the `record_id#(x,y)#term1 term2 ...` format. The main components are always separated by `#`, and terms are always separated by single spaces. You are **not** required to handle malformed lines. You can assume that all `record_id`s from the first input file (Dataset A) will begin with the prefix 'A', and all `record_id`s from the second input file (Dataset B) will begin with the prefix 'B'. The character following the prefix will be the start of a unique numerical identifier.

## Submission

**Deadline:** Sunday, 9 August 11:59:59 PM

If you need an extension, please apply for special consideration via **myUNSW** first. You can submit multiple times before the due date and we will only mark your final submission. To prove a successful submission, please take a screenshot as the assignment submission instructions show and keep it to yourself. If you have any problems with submissions, please email [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au) or [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au).

## Late Submission Penalty

5% of the maximum possible mark per 24 hours for up to 5 days. Submissions delayed for over 5 days will be rejected.

## Marking Criteria

Your source code will be inspected and marked based on readability, correctness, and efficiency.

- Submission can be compiled and run on Spark => +6
- Correctness (no unexpected pairs, no missing pairs, correct order, correct distance and similarity values, correct format) => +4
- Technical Implementations => +8
  - Frequency Sorting (2 marks): string to number conversion, broadcasting
  - Correct Prefix Filtering Implementation (3 marks): calculating prefix, filtering based on prefix
  - Correct Grid Index Implementation (3 marks): partitioning the space, filtering based on the grid
- Efficiency => +4
  - This score is based on the rank of your program's runtime on the largest test case (e.g., the top 25% got all 4 marks).
  - To be eligible for efficiency marks, your submission must produce the correct result and not be a pairwise method.

---

# 可扩展的时空相似性连接（Spatio-Textual Similarity Joins，22分）- 中文版

## 背景

在现代数据科学中，基于相似性对数据集进行连接（join）是一项关键任务。在 **地理信息系统（GIS）** 与基于位置的服务领域，这一挑战尤为复杂且具有现实意义。现实世界中的实体（例如餐厅、地标或社交媒体帖子）往往同时由其物理位置和一组丰富的文本属性来描述。将来自不同数据源的记录关联起来——例如将 Foursquare 场所与 Google Maps 列表匹配，或将推文与报道同一事件的新闻文章关联——对于数据增强、实体消解和上下文分析都至关重要。

本项目要解决的是 **时空相似性连接（spatio-textual similarity join）** 问题。你需要开发一个可扩展的系统，从两个不同的数据集中识别出既 **空间上邻近** 又 **文本上相似** 的记录对。这要求你超越简单的数据库连接，在 Apache Spark 并行计算框架上实现高级的二维过滤技术。

## 题目定义

给定两组记录 **Dataset A** 与 **Dataset B**，其中每条记录 `R = (L, S)` 包含两个属性：二维空间中的位置（即 `R.L` 是坐标 `(x, y)`）和文本描述（即 `R.S` 是一组词/词项的集合）。

任务是找出所有记录对 `(R_A, R_B)`，其中 `R_A` 属于 Dataset A，`R_B` 属于 Dataset B，且 **同时** 满足以下两个条件：

- 两者的 **欧氏距离** 小于或等于给定阈值 `d`。
- 两者词项集合上的 **Jaccard 相似度** 大于或等于给定阈值 `s`。

**欧氏距离** 小于或等于给定阈值 `d`：

```text
sqrt( (R_A.L.x - R_B.L.x)^2 + (R_A.L.y - R_B.L.y)^2 ) <= d
```

**Jaccard 相似度** 大于或等于给定阈值 `s`，即：

```text
|R_A.S ∩ R_B.S| / |R_A.S ∪ R_B.S| >= s
```

## 样例数据

给定以下两个样例文件 `A.txt` 与 `B.txt`，阈值 `d = 2.0`、`s = 0.5`：

文件：`A.txt`

```text
A0#(1,1)#apple banana orange
A1#(10,10)#grape kiwi pear
A2#(2,1)#apple banana kiwi
```

文件：`B.txt`

```text
B0#(1,2)#apple banana grape
B1#(11,11)#kiwi pear mango
B2#(8,8)#apple lemon
```

## 输出格式

输出文件包含所有满足条件的记录对及其计算出的距离与相似度。输出格式为 `(recordA.id,recordB.id):distance_value,jaccard_similarity_value`。

- 每一对必须由 Dataset A 中的一条记录和 Dataset B 中的一条记录组成。
- 距离与相似度数值均 **四舍五入保留 6 位小数**。
- 输出中不得出现重复的记录对。
- **排序规则：** 输出对必须严格按照以下两级规则排序。排序必须基于 ID 的 **数值大小**，而非字符串表示（例如 `A10` 应排在 `A2` 之后）。
  - **主键（Primary Key）：** 按 Dataset A 中 ID 的数值大小升序排列（例如对于 `A2` 与 `A10`，按 `2` 和 `10` 排序）。
  - **次键（Secondary Key）：** 对于 `recordA.id` 相同的记录对，按 Dataset B 中 ID 的数值大小升序排列。

给定上述样例数据、阈值 `d = 2.0` 与 `s = 0.5`，输出结果应为：

```text
(A0,B0):1.0,0.5
(A1,B1):1.414214,0.5
(A2,B0):1.414214,0.5
```

我们还发布了更大的数据集 `A_large.txt`、`B_large.txt`，以及 `d = 100`、`s = 0.5` 时的输出 `output_d=100_s=0.5.txt`，供你参考。

## 代码与运行

已在 `proj3.py` 中提供代码模板。程序需接收 5 个参数：第一个输入文件路径、第二个输入文件路径、输出目录、距离阈值 `d`、相似度阈值 `s`。你必须使用以下命令运行代码：

```bash
spark-submit proj3.py <input_A_path> <input_B_path> <output_path> <d> <s>
```

## 其他说明

- 你必须设计一个 **精确（exact）** 的相似记录查找方法（*请复习 Week 8 的课件以获取更多提示*）。
- 距离连接需要使用 **网格索引（grid index）**。
- 相似性连接部分，请参阅课件中提到的论文 *Efficient Parallel Set-Similarity Joins Using MapReduce. SIGMOD'10*。
- **不允许直接计算两两相似度（该方法复杂度为 O(N^2)）！！！也不允许使用 Lecture 8.1 课件第 21 页所示的朴素解法，因为它同样属于两两（pairwise）计算方案。**
- 不允许使用常规 Python 编程实现（必须使用 RDD 或 DataFrame，且不得使用第三方库）。
- 代码中任何位置都 **不得** 使用 `Cache()`/`Persist()`。
- 评测正确性与效率时，所有代码都将使用 Spark 默认设置、以 *两个本地线程（two local threads）* 运行。请务必注意你的运行时间与内存占用。
- **输入文件格式与假设：** <u>你可以假设输入文件结构一致且格式规范</u>。每一行都严格遵循 `record_id#(x,y)#term1 term2 ...` 格式。主要部分之间始终以 `#` 分隔，词项之间始终以单个空格分隔。你 **无需** 处理格式错误的行。你可以假设第一个输入文件（Dataset A）中所有 `record_id` 都以前缀 'A' 开头，第二个输入文件（Dataset B）中所有 `record_id` 都以前缀 'B' 开头。前缀之后的字符即为唯一数值标识符的起始。

## 提交说明

**截止时间：** Sunday, 9 August 11:59:59 PM

如需延期，请先通过 **myUNSW** 申请 special consideration。截止前可多次提交，最终以最后一次提交为准。请按作业说明保留提交成功截图以备查验。提交问题可联系：

- [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au)
- [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au)

## 迟交扣分

最多 5 天内，每迟交 24 小时扣减总分的 5%；迟交超过 5 天不予接受。

## 评分标准

你的源代码将根据 **可读性、正确性与效率** 进行检查与评分。

- 能在 Spark 上成功编译并运行 => +6
- 正确性（无多余对、无遗漏对、顺序正确、距离与相似度数值正确、格式正确）=> +4
- 技术实现 => +8
  - 频率排序 Frequency Sorting（2 分）：字符串到数值的转换、广播（broadcasting）
  - 正确的前缀过滤实现 Prefix Filtering（3 分）：计算前缀、基于前缀过滤
  - 正确的网格索引实现 Grid Index（3 分）：划分空间、基于网格过滤
- 效率 => +4
  - 该分数依据你的程序在最大测试用例上的运行时间排名（例如运行时间排名前 25% 可获得全部 4 分）。
  - 要有资格获得效率分，你的提交必须产出正确结果，且不能是 pairwise（两两计算）方法。
