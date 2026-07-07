# Indoor Air Quality Risk Analysis using Spark (16 marks)

## Background

In Project 1, you analysed IoT device logs to identify abnormal latency behaviour. Modern smart homes also generate large volumes of environmental sensor data, including measurements of carbon dioxide (CO2), volatile organic compounds (VOC), and fine particulate matter (PM2.5). In this project, you will use Apache Spark to analyse smart-home sensor logs and identify high-risk air-quality days for different devices.

## Input Format

The input CSV file contains one sensor reading per line, in the format:

```text
timestamp,device_id,co2_ppm,voc_ppb,pm25_ugm3
```

Field definitions:

- `timestamp`: format `m/d/yy hour:minute`; all years in the input dataset are in the range 2000-2099.
- `device_id`: the sensor identifier.
- `co2_ppm`: CO2 concentration (ppm).
- `voc_ppb`: VOC concentration (ppb).
- `pm25_ugm3`: PM2.5 concentration (ug/m3).

Each field is separated by a comma. A sample input is shown below:

```text
1/1/23 12:56,DEV_00021,,145.8,16.21
1/2/23 1:37,DEV_00021,609.5,155.2,12.41
1/4/23 7:39,DEV_00021,460.5,128.6,10.98
1/4/23 14:51,DEV_00021,624.8,149.7,12.48
1/4/23 15:25,DEV_00021,600.2,162.3,18.13
1/4/23 17:41,DEV_00021,480.4,126.3,13.68
1/5/23 19:24,DEV_00021,438.6,125.9,10.71
1/7/23 19:31,DEV_00021,427.6,193.4,8.63
1/7/23 19:56,DEV_00021,423.8,139.8,12.78
1/9/23 12:15,DEV_00021,619.5,164.3,15.44
1/9/23 12:15,DEV_00022,619.5,164.3,55.44
1/10/23 12:15,DEV_00022,619.5,164.3,45.44
1/11/23 12:15,DEV_00022,619.5,164.3,65.44
2/9/23 12:15,DEV_00022,619.5,164.3,55.44
```

A reading is considered invalid if any field is missing. Invalid readings must be ignored in all subsequent computations.

## Air Quality Risk Analysis

For each valid reading, compute an air quality risk score using:

```text
risk_score = co2_ppm / 1000 + voc_ppb / 300 + pm25_ugm3 / 15
```

where the predefined reference thresholds are:

```text
CO2 threshold = 1000 ppm
VOC threshold = 300 ppb
PM2.5 threshold = 15 ug/m3
```

A reading is considered a risky reading if `risk_score >= tau`, where `tau` is a user-provided threshold.

A date is considered a risky date for a device if the device has at least one risky reading on that date.

For each device, the average risk score for a date is computed as the average of the risk scores of all valid readings associated with that device on that date.

## Output Format

Your program should output results in the following format.

For each device that has at least one risky date:

- Output the device identifier and the number of risky dates:
  `device_id:number_of_risky_dates`
- Followed by all risky dates for that device, one per line:
  `date\taverage_risk_score`

Ordering requirements:

- Devices must be sorted by:
  - `number_of_risky_dates` (descending)
  - device identifier (ascending, to break ties)
- Risky dates for a device must be sorted by:
  - `date` (ascending)

For example, given the sample dataset `sample.csv` and `tau = 2.0`, the generated output should be:

```text
DEV_00022:4
1/9/23	4.863166666666666
1/10/23	4.1965
1/11/23	5.529833333333333
2/9/23	4.863166666666666
DEV_00021:2
1/4/23	1.935058333333333
1/9/23	2.1965
```

## Note

- Perform all calculations in full precision; do not round at any stage.
- There may be very small rounding differences in decimal values due to floating-point precision. These do not affect correctness and will not impact your marks.

## Code Format

The code template has been provided. You need to submit two solutions:

- one using only RDD APIs
- one using only DataFrame APIs

Your code should take three parameters: the input file, the output folder, and the value of `tau`, for example:

```bash
spark-submit project2_rdd.py "file:///home/air_bedroom.csv" "file:///home/output" 2.8
```

The output for `air_bedroom.csv` with `tau = 2.8` has been provided in `testcase_airbedroom_2.8` for your reference.

## Submission

**Deadline:** Sunday 19 July 11:59:59 PM

If you need an extension, please apply for special consideration via **myUNSW** first. You can submit multiple times before the due date and we will only mark your final submission. To prove a successful submission, please take a screenshot as the assignment submission instructions show and keep it to yourself. If you have any problems with submissions, please email [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au) or [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au).

## Late Submission Penalty

5% reduction of your marks for up to 5 days. Submissions delayed for over 5 days will be rejected.

## Some Notes

- You can read files from either HDFS or the local file system. Using local files is more convenient, but you must use the prefix `file:///...`. Spark uses HDFS by default if the path does not have a prefix.
- You are not allowed to use `numpy` or `pandas`, since we aim to assess your understanding of RDD/DataFrame APIs.
- The required output ordering must be implemented using Spark operations. The use of Python sorting functions (e.g., `sorted()`) or collecting data to the driver for sorting is not allowed.
- You should use `coalesce(1)` to merge data into a single partition and then save the data to disk.
- In the DataFrame solution, it is not allowed to use `spark.sql()` to pass SQL statements to Spark directly.
- It does not matter if you have a newline at the end of the output file or not. It will not affect correctness.

## Marking Criteria (8 marks for each solution)

- You must complete this assignment using Spark RDD/DataFrame APIs. Submissions that only contain regular Python techniques will be marked as 0.
- Submission can be compiled and run on Spark => +3
- Submission can obtain correct device risk analysis results (counts, scores, format, and order) => +3
- Submission correctly uses Spark APIs (RDD/DataFrame solution: only RDD/DataFrame APIs allowed) => +1
- Submission implements the required output ordering using Spark operations rather than Python sorting functions (e.g., `sorted()`) or driver-side sorting => +0.5
- Submission has excellent code format and structure, readability, and documentation => +0.5

---

# 室内空气质量风险分析（Spark，16分）- 中文版

## 背景

在 Project 1 中，你已经通过 IoT 设备日志分析了异常延迟行为。现代智能家居还会产生大量环境传感器数据，包括二氧化碳（CO2）、挥发性有机物（VOC）以及细颗粒物（PM2.5）等指标。本项目要求你使用 Apache Spark 分析智能家居传感器日志，并识别不同设备的高风险空气质量日期。

## 输入格式

输入为 CSV 文件，每行一条传感器读数，格式如下：

```text
timestamp,device_id,co2_ppm,voc_ppb,pm25_ugm3
```

字段定义：

- `timestamp`：格式为 `m/d/yy hour:minute`；输入数据中的年份范围为 2000-2099。
- `device_id`：传感器设备标识。
- `co2_ppm`：CO2 浓度（ppm）。
- `voc_ppb`：VOC 浓度（ppb）。
- `pm25_ugm3`：PM2.5 浓度（ug/m3）。

各字段以英文逗号分隔。样例输入如下：

```text
1/1/23 12:56,DEV_00021,,145.8,16.21
1/2/23 1:37,DEV_00021,609.5,155.2,12.41
1/4/23 7:39,DEV_00021,460.5,128.6,10.98
1/4/23 14:51,DEV_00021,624.8,149.7,12.48
1/4/23 15:25,DEV_00021,600.2,162.3,18.13
1/4/23 17:41,DEV_00021,480.4,126.3,13.68
1/5/23 19:24,DEV_00021,438.6,125.9,10.71
1/7/23 19:31,DEV_00021,427.6,193.4,8.63
1/7/23 19:56,DEV_00021,423.8,139.8,12.78
1/9/23 12:15,DEV_00021,619.5,164.3,15.44
1/9/23 12:15,DEV_00022,619.5,164.3,55.44
1/10/23 12:15,DEV_00022,619.5,164.3,45.44
1/11/23 12:15,DEV_00022,619.5,164.3,65.44
2/9/23 12:15,DEV_00022,619.5,164.3,55.44
```

若任一字段缺失，则该条读数视为无效读数。无效读数必须在后续所有计算中忽略。

## 空气质量风险分析

对于每条有效读数，按如下公式计算空气质量风险分数：

```text
risk_score = co2_ppm / 1000 + voc_ppb / 300 + pm25_ugm3 / 15
```

其中预定义参考阈值为：

```text
CO2 threshold = 1000 ppm
VOC threshold = 300 ppb
PM2.5 threshold = 15 ug/m3
```

当 `risk_score >= tau`（`tau` 为用户给定阈值）时，该读数为高风险读数。

若某设备在某天至少有一条高风险读数，则该天为该设备的高风险日期。

对每个设备在每个日期，平均风险分数定义为：该设备在该日期所有有效读数风险分数的平均值。

## 输出格式

程序输出应满足以下格式要求。

对于每个至少存在一个高风险日期的设备：

- 首先输出设备标识及其高风险日期数量：
  `device_id:number_of_risky_dates`
- 随后逐行输出该设备的所有高风险日期：
  `date\taverage_risk_score`

排序要求：

- 设备排序：
  - 按 `number_of_risky_dates` 降序；
  - 若并列，按设备标识升序。
- 每个设备内部的高风险日期排序：
  - 按 `date` 升序。

例如，给定样例数据 `sample.csv` 且 `tau = 2.0`，输出应为：

```text
DEV_00022:4
1/9/23	4.863166666666666
1/10/23	4.1965
1/11/23	5.529833333333333
2/9/23	4.863166666666666
DEV_00021:2
1/4/23	1.935058333333333
1/9/23	2.1965
```

## 注意事项

- 所有计算必须保持完整精度，任何阶段都不要四舍五入。
- 若小数末位出现极小差异，通常由浮点精度导致，不影响正确性与评分。

## 代码要求

已提供代码模板。你需要提交两个版本：

- 仅使用 RDD APIs 的实现；
- 仅使用 DataFrame APIs 的实现。

程序应接收 3 个参数：输入文件、输出目录、阈值 `tau`。例如：

```bash
spark-submit project2_rdd.py "file:///home/air_bedroom.csv" "file:///home/output" 2.8
```

`air_bedroom.csv` 在 `tau = 2.8` 时的参考输出已提供在 `testcase_airbedroom_2.8` 中。

## 提交说明

**截止时间：** Sunday 19 July 11:59:59 PM

如需延期，请先通过 **myUNSW** 申请 special consideration。截止前可多次提交，最终以最后一次提交为准。请按作业说明保留提交成功截图以备查验。提交问题可联系：

- [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au)
- [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au)

## 迟交扣分

最多 5 天内，每天扣减 5% 分数；迟交超过 5 天不予接受。

## 其他说明

- 输入可来自 HDFS 或本地文件系统。若使用本地文件，路径需带前缀 `file:///...`；若无前缀，Spark 默认按 HDFS 路径处理。
- 不允许使用 `numpy` 或 `pandas`，本项目用于考查你对 RDD/DataFrame APIs 的理解。
- 输出排序必须通过 Spark 操作完成；不允许使用 Python 排序（如 `sorted()`）或将数据收集到 driver 端后再排序。
- 请使用 `coalesce(1)` 将结果合并为单分区后再保存到磁盘。
- DataFrame 解法中，不允许使用 `spark.sql()` 直接提交 SQL 语句。
- 输出文件末尾是否包含换行不影响正确性。

## 评分标准（每个解法 8 分）

- 必须使用 Spark RDD/DataFrame APIs 完成；仅用常规 Python 技术实现将记 0 分。
- 能在 Spark 上成功编译并运行 => +3
- 设备风险分析结果正确（计数、分数、格式与顺序）=> +3
- 正确使用 Spark APIs（RDD/DataFrame 解法仅允许对应 API）=> +1
- 使用 Spark 操作实现所需排序，而非 Python 排序或 driver 端排序 => +0.5
- 代码格式与结构优秀、可读性与文档性良好 => +0.5
