# Project 2 测试教程 · Testing Guide

在 **Ed** 的 Spark 环境里验证 `project2_rdd.py` 和 `project2_df.py` 是否正确。
按顺序照抄命令即可。命令里的路径已按 Ed 上确认的 `/home` 填好,可直接粘贴运行。

---

## 0. 准备:确认文件和当前路径

Workspace 里需要有这些文件:

| 文件 | 作用 |
| --- | --- |
| `project2_rdd.py` | RDD 版解法(提交) |
| `project2_df.py` | DataFrame 版解法(提交) |
| `air_bedroom.csv` | 官方测试数据 |
| `output_airbedroom_2.8` | 官方参考答案(用来比对) |
| `sample.csv` | spec 里的样例数据(自检排序用) |

先看清楚当前目录的绝对路径,后面 `file:///` 要用它:

```bash
pwd
ls -l
```

本教程里 `pwd` 已确认为 `/home`,文件都直接放在 `/home` 下,所以 `file:///` 后接 `/home/...`。若你的 workspace 路径不同,把命令里的 `/home` 换成 `pwd` 的真实输出即可。

> ⚠️ **为什么要 `file:///`**:Spark 默认把路径当 HDFS。加 `file:///` 前缀才是读本地文件。
> ⚠️ **输出目录必须不存在**:Spark 若发现输出目录已存在会直接报错。重跑前先 `rm -rf` 掉旧目录。

---

## 1. 跑 RDD 版(tau = 2.8)

```bash
rm -rf out_rdd
spark-submit project2_rdd.py "file:///home/air_bedroom.csv" "file:///home/out_rdd" 2.8
```

看结果(`coalesce(1)` 保证只有一个 `part-00000`):

```bash
cat out_rdd/part-00000
```

---

## 2. 跑 DataFrame 版(tau = 2.8)

```bash
rm -rf out_df
spark-submit project2_df.py "file:///home/air_bedroom.csv" "file:///home/out_df" 2.8
```

```bash
cat out_df/part-00000
```

---

## 3. 和官方参考答案比对(最关键)

```bash
diff out_rdd/part-00000 output_airbedroom_2.8
diff out_df/part-00000  output_airbedroom_2.8
```

**判读结果:**

- **无任何输出** = 完全一致,✅ 通过。
- 只报**浮点末位极小差异**(如 `...4.86316666666` vs `...4.86316666667`)= spec 明确说不影响正确性,✅ 忽略。
- 报**行数不同 / 顺序不同 / count 不同 / 设备缺失** = ❌ 真 bug,把 diff 结果贴回来我帮你查。

想只看差了几行、忽略末位小数,可以用:

```bash
diff <(cut -c1-40 out_rdd/part-00000) <(cut -c1-40 output_airbedroom_2.8)
```

---

## 4. 用样例数据自检排序(tau = 2.0)

这一步专门验证排序逻辑(设备排序 + 日期排序):

```bash
rm -rf out_sample
spark-submit project2_rdd.py "file:///home/sample.csv" "file:///home/out_sample" 2.0
cat out_sample/part-00000
```

**期望输出**(和 spec 完全一致才算对):

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

DataFrame 版也跑一遍换 `project2_df.py`,结果应该一样。

---

## 5. 对照评分点自查

| 评分点 | 怎么验证 |
| --- | --- |
| 能编译运行(各 +3) | 没报错,且有 `part-00000` 输出 |
| 结果正确(各 +3) | 第 3 步 `diff` 通过 |
| 只用对应 API(各 +1) | RDD 版无 DataFrame;DF 版无 `spark.sql()` |
| 排序用 Spark 实现(各 +0.5) | 用 `sortByKey`/`orderBy`,无 `sorted()`、无 driver 端排序 |
| 代码可读性(各 +0.5) | 有注释和清晰结构 |

**特别要肉眼确认的三点正确性:**

1. **无效行被丢弃** —— `sample.csv` 里 `DEV_00021` 有一条 CO2 为空(`1/1/23 12:56,DEV_00021,,145.8,16.21`),它不能进入任何计算。
2. **日均分是当天「所有有效读数」的平均** —— 不是只对高风险读数求平均。例:`DEV_00021` 的 `1/4/23` 有 4 条有效读数,只有 1 条高风险,但日均分要对 4 条求平均 = `1.935058333333333`。
3. **排序**:设备按「风险天数降序 → 设备名升序」;每个设备内部按日期升序(真实年月日,不是字符串排序)。

---

## 常见报错速查

| 报错 | 原因 / 解决 |
| --- | --- |
| `Output directory ... already exists` | 输出目录已存在。`rm -rf out_rdd` 后重跑 |
| `Path does not exist` / 找不到输入 | 忘了 `file:///` 前缀,或路径写错。用 `pwd` 核对绝对路径 |
| `cat: out_rdd/part-00000: No such file` | job 没成功,往上翻 spark 日志看真正的异常栈 |
| 结果为空文件 | 输入路径不对读到空数据,或所有行都被判无效 |
