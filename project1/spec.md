# Latency Stability Analysis (12 marks)

## Background

Modern smart homes rely on hundreds of interconnected IoT devices such as sensors, smart plugs, thermostats, and cameras. Network latency is a key indicator of communication quality between devices and cloud services. While occasional fluctuations are expected, persistent increases in latency may indicate network congestion, device malfunction, or connectivity issues. Identifying devices whose latency significantly increase from their normal behavior can help operators detect and troubleshoot problems before they impact users.

## Problem Definition

You are given a dataset of IoT device readings collected over time. Each record contains a datestamp, a device ID and a latency measurement (in milliseconds).

Your task is to utilize MRJob to identify unusual latency behavior for each device based on the following steps:

- For each device, calculate its **overall average latency** across all readings.
- For each device on each day, calculate its **daily average latency**.
- For each device on each day, calculate the **latency increase** using the following formula:
`LatencyIncrease = DailyAvgLatency - OverallAvgLatency`
- Report all device-day records whose latency increase exceeds a given threshold `T`.

## Output Format

The output should contain three fields: `"DeviceID\tDate:LatencyIncrease"`.

The results must be sorted by **DeviceID** in ascending alphabetical order first and then by **Date** in descending order.

Given the sample input file `sample.csv`, the output for threshold value `T = 5` might look like (there is no need to remove the quotation marks that MRJob generates):

```text
"DEV_00001"    "2024-01-01:7.669999999999998"
"DEV_00004"    "2024-01-05:6.225000000000001"
"DEV_00008"    "2024-01-23:33.963333333333324"
"DEV_00008"    "2024-01-19:32.153333333333332"
```

## Note

- Perform all calculations in full precision; do not round at any stage.
- There may be very small rounding differences in decimal values (e.g., `10.4076820344127862` vs `10.4076820344127863`) due to floating-point precision. These do not affect correctness and will not impact your marks.
- Due to the limited resources available on Ed, we provide a **10% sampled dataset** (`latency2401_10pct.csv`) to reduce the chance of resource-related failures when running Hadoop jobs. Your code should still work correctly for the full dataset (`latency2401.csv`) and for data from any month or year.

The outputs for `latency2401_10pct.csv` with `T = 50` and for `latency2401.csv` with `T = 50` are provided in `output_10pct_tau50` and `output_tau50`, respectively, for your reference.

## Code Format

The code template has been provided. Your code should take three parameters: the input file, the output folder on HDFS, and the `T` value.

We will also use more than 1 reducer to test your code. Assuming `T = 50` and using 2 reducers, you need to use the command below to run your code:

```bash
python3 proj1.py -r hadoop latency2401.csv -o output --jobconf myjob.settings.tau=50 --jobconf mapreduce.job.reduces=2
```

Note: You can access the value of `T` in your program like
`tau = jobconf_from_env('myjob.settings.tau')`,
and you need to import `jobconf_from_env` by
`from mrjob.compat import jobconf_from_env`
(see the code template).

## Submission

**Deadline:** Monday 29th June 11:59:59 PM

If you need an extension, please apply for special consideration via **myUNSW** first. You can submit multiple times before the due date, and we will only mark your final submission. To prove a successful submission, please take a screenshot as the assignment submission instructions show and keep it to yourself. If you have any problems with submissions, please email [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au) or [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au).

## Late Submission Penalty

5% of the maximum possible mark per 24 hours for up to 5 days. Submissions delayed for over 5 days will be rejected.

## Marking Criteria

- You must complete this assignment based on MRJob and Hadoop. Submissions that only contain regular Python techniques will be marked as 0.
- You cannot simply emit all key-value pairs from mappers and buffer them in memory on reducers to do the task, and such a method will receive no more than 4 marks.
- Submissions that cannot be compiled and run on Hadoop on the Ed environment will receive no more than 4 marks.
- Submissions can be compiled on Ed and run on Hadoop => +4
- All the `LatencyIncrease` values in the output are correct => +1
- The order in the output is correct => +1 (Note: You only need to guarantee the order within each reducer output)
- The output format is correct => +1
- Submissions correctly implement the combiner or in-mapper combining => +1
- Submissions correctly implement order inversion (i.e., using special keys) => +1
- Submissions correctly implement secondary sort => +1
- Submissions can produce the correct result using one MRStep => +1
- Submissions can produce the correct result with multiple reducers => +1 (Note: You do not need to include `mapreduce.job.reduces` in `JOBCONF` since the number of reducers will be received from the command)

---

# 延迟稳定性分析（12分）- 中文版

## 背景

现代智能家居依赖大量互联的 IoT 设备，例如传感器、智能插座、温控器和摄像头。网络延迟是设备与云服务之间通信质量的关键指标。虽然偶发波动是正常现象，但持续性延迟升高可能意味着网络拥塞、设备故障或连接异常。识别出延迟相较于常态显著升高的设备，有助于运维人员在影响用户前提前发现并定位问题。

## 题目定义

给定一份按时间采集的 IoT 设备数据集。每条记录包含：日期、设备 ID、延迟值（毫秒）。

请使用 MRJob 完成如下任务：

- 对每个设备，计算其全量记录上的**总体平均延迟**（overall average latency）。
- 对每个设备在每天，计算其**日平均延迟**（daily average latency）。
- 对每个设备在每天，按下式计算**延迟增量**：
`LatencyIncrease = DailyAvgLatency - OverallAvgLatency`
- 输出所有延迟增量超过阈值 `T` 的“设备-日期”记录。

## 输出格式

输出应包含三部分：`"DeviceID\tDate:LatencyIncrease"`。

输出顺序要求：
- 先按 **DeviceID** 字母升序；
- 再按 **Date** 降序。

以 `sample.csv` 为例，当 `T = 5` 时，输出可能如下（MRJob 生成的引号无需去除）：

```text
"DEV_00001"    "2024-01-01:7.669999999999998"
"DEV_00004"    "2024-01-05:6.225000000000001"
"DEV_00008"    "2024-01-23:33.963333333333324"
"DEV_00008"    "2024-01-19:32.153333333333332"
```

## 注意事项

- 所有计算必须使用完整精度，不要在任何阶段进行四舍五入。
- 小数最后若出现极细微差异（例如 `10.4076820344127862` 与 `10.4076820344127863`）属于浮点精度现象，不影响正确性与评分。
- 由于 Ed 资源有限，提供了 **10% 抽样数据集** `latency2401_10pct.csv` 以降低资源失败概率。你的代码仍需可正确处理全量数据 `latency2401.csv`，并适配任意月份或年份的数据。

参考输出：
- `latency2401_10pct.csv` 在 `T = 50` 时的输出：`output_10pct_tau50`
- `latency2401.csv` 在 `T = 50` 时的输出：`output_tau50`

## 代码要求

已提供代码模板。程序需接收 3 个参数：
- 输入文件
- HDFS 输出目录
- 阈值 `T`

评测时会使用多 reducer。假设 `T = 50` 且 reducer 数为 2，可用以下命令运行：

```bash
python3 proj1.py -r hadoop latency2401.csv -o output --jobconf myjob.settings.tau=50 --jobconf mapreduce.job.reduces=2
```

`T` 在代码中的读取方式：
- `tau = jobconf_from_env('myjob.settings.tau')`
- 并导入：`from mrjob.compat import jobconf_from_env`

## 提交说明

**截止时间：** Monday 29th June 11:59:59 PM

如需延期，请先通过 **myUNSW** 申请 special consideration。截止时间前可多次提交，最终只以最后一次提交为准。请按作业提交说明保留提交成功截图以备查验。提交问题可联系：
- [cs9313@cse.unsw.edu.au](mailto:cs9313@cse.unsw.edu.au)
- [yi.k.ding@unsw.edu.au](mailto:yi.k.ding@unsw.edu.au)

## 迟交扣分

最多 5 天内，每迟交 24 小时扣总分 5%；超过 5 天将不予接受。

## 评分标准

- 必须基于 MRJob 与 Hadoop 完成。仅用普通 Python 技术实现将记 0 分。
- 不能将 mapper 全量键值对直接输出后在 reducer 内存中整体缓存计算；此类方法最多 4 分。
- 若代码无法在 Ed 环境成功编译并在 Hadoop 上运行，最多 4 分。
- 能在 Ed 上编译并在 Hadoop 运行通过 => +4
- 输出中所有 `LatencyIncrease` 数值正确 => +1
- 输出顺序正确 => +1（只需保证每个 reducer 输出文件内部顺序）
- 输出格式正确 => +1
- 正确实现 combiner 或 in-mapper combining => +1
- 正确实现 order inversion（使用特殊 key）=> +1
- 正确实现 secondary sort => +1
- 使用一个 MRStep 产出正确结果 => +1
- 多 reducers 下仍可正确产出结果 => +1（无需在 `JOBCONF` 中硬编码 `mapreduce.job.reduces`，评测命令会传入 reducer 数）
