from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys


class Project2:
    """
    DataFrame-only solution for the indoor air-quality risk analysis.

    Pipeline:
      1. Read the CSV as raw string columns; a reading is invalid if any field
         is missing, so cast the three measurements to double and drop any row
         with a null field (a missing/empty value casts to null).
      2. Derive the date string and a (year, month, day) key for chronological
         ordering, and the per-reading risk score.
      3. Group by (device, date): average risk over ALL valid readings of the
         day and flag the day as risky if any reading reaches tau.
      4. Number the risky dates per device with a window count.
      5. Build header rows ("device:count") and detail rows ("date\tavg") as a
         single text column, ordered entirely by Spark, and write one file with
         coalesce(1).

    All ordering is done with DataFrame orderBy over the compound key
    (-num, device, row_type, year, month, day):
      -num       -> risky-date count descending
       device    -> ascending tie-break
       row_type  -> 0 header before 1 detail within a device
       y, m, d   -> dates ascending in true chronological order
    """

    CO2_REF, VOC_REF, PM25_REF = 1000.0, 300.0, 15.0

    def run(self, inputPath, outputPath, tau):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        tau = float(tau)

        # 1. read raw columns and keep only fully-populated numeric readings.
        raw = spark.read.csv(inputPath)
        df = raw.select(
            F.col("_c0").alias("ts"),
            F.col("_c1").alias("device"),
            F.col("_c2").cast("double").alias("co2"),
            F.col("_c3").cast("double").alias("voc"),
            F.col("_c4").cast("double").alias("pm25"),
        ).dropna()

        # 2. date string, chronological key, and per-reading risk score.
        date_str = F.split(F.col("ts"), " ").getItem(0)          # "m/d/yy"
        d_parts = F.split(date_str, "/")
        df = df.select(
            F.col("device"),
            date_str.alias("date"),
            (2000 + d_parts.getItem(2).cast("int")).alias("y"),
            d_parts.getItem(0).cast("int").alias("m"),
            d_parts.getItem(1).cast("int").alias("d"),
            (F.col("co2") / self.CO2_REF
             + F.col("voc") / self.VOC_REF
             + F.col("pm25") / self.PM25_REF).alias("risk"),
        )

        # 3. per (device, date): daily average and whether the day is risky.
        daily = df.groupBy("device", "date", "y", "m", "d").agg(
            F.avg("risk").alias("avg_risk"),
            F.max((F.col("risk") >= F.lit(tau)).cast("int")).alias("is_risky"),
        ).filter(F.col("is_risky") == 1)

        # 4. count risky dates per device.
        daily = daily.withColumn("num", F.count("*").over(Window.partitionBy("device")))

        # 5. header rows and detail rows sharing one compound ordering key.
        headers = daily.select("device", "num").distinct().select(
            (-F.col("num")).alias("k_num"),
            F.col("device").alias("k_dev"),
            F.lit(0).alias("k_type"),
            F.lit(0).alias("y"), F.lit(0).alias("m"), F.lit(0).alias("d"),
            F.concat(F.col("device"), F.lit(":"), F.col("num")).alias("line"),
        )
        details = daily.select(
            (-F.col("num")).alias("k_num"),
            F.col("device").alias("k_dev"),
            F.lit(1).alias("k_type"),
            F.col("y"), F.col("m"), F.col("d"),
            F.concat(F.col("date"), F.lit("\t"), F.col("avg_risk")).alias("line"),
        )

        (headers.unionByName(details)
                .orderBy("k_num", "k_dev", "k_type", "y", "m", "d")
                .select("line")
                .coalesce(1)
                .write.text(outputPath))

        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])
