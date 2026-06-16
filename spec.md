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

- Report all device-day records whose latency increase exceeds a given threshold `τ`.

## Output Format
The output should contain three fields: `"DeviceID\tDate:LatencyIncrease"`.

The results must be sorted by **DeviceID** in ascending alphabetical order first and then by **Date** in descending order.

Given the sample input file `sample.csv`, the output for threshold value `τ = 5` might look like (there is no need to remove the quotation marks that MRJob generates):

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

The outputs for `latency2401_10pct.csv` with `τ = 50` and for `latency2401.csv` with `τ = 50` are provided in `output_10pct_tau50` and `output_tau50`, respectively, for your reference.

## Code Format
The code template has been provided. Your code should take three parameters: the input file, the output folder on HDFS, and the `τ` value.

We will also use more than 1 reducer to test your code. Assuming `τ = 50` and using 2 reducers, you need to use the command below to run your code:

```bash
python3 proj1.py -r hadoop latency2401.csv -o output --jobconf myjob.settings.tau=50 --jobconf mapreduce.job.reduces=2
```

Note: You can access the value of `τ` in your program like
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