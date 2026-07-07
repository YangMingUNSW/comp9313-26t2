from pyspark import SparkContext, SparkConf
import sys


class Project2:
    """
    RDD-only solution for the indoor air-quality risk analysis.

    Pipeline:
      1. Parse each line; drop invalid readings (any missing field or a
         non-numeric measurement).
      2. Fold every valid reading into a (device, date) group carrying
         (sum_risk, count, risky_count). reduceByKey does the aggregation.
      3. Keep only risky dates (risky_count > 0) and compute the daily
         average over ALL valid readings of that day.
      4. Count risky dates per device, then attach that count to every
         record so headers and detail lines share the same device ordering.
      5. Emit one text line per output row keyed by a compound sort key and
         let Spark (sortByKey) produce the required ordering -- no driver-side
         sorting. coalesce(1) merges to a single ordered partition on save.

    Compound sort key = (-num_risky_dates, device_id, row_type, (y, m, d)):
      -num_risky_dates -> count descending
       device_id       -> ascending tie-break
       row_type         -> 0 = header before 1 = date rows of the same device
       (y, m, d)        -> dates ascending in true chronological order
    """

    # risk_score = co2_ppm/1000 + voc_ppb/300 + pm25_ugm3/15
    CO2_REF, VOC_REF, PM25_REF = 1000.0, 300.0, 15.0

    @staticmethod
    def _parse(line, tau):
        """line -> (device, date_str, (y, m, d), risk, is_risky) or None."""
        parts = line.split(",")
        if len(parts) != 5:
            return None
        ts, dev, co2, voc, pm = (p.strip() for p in parts)
        if not (ts and dev and co2 and voc and pm):
            return None
        try:
            co2, voc, pm = float(co2), float(voc), float(pm)
        except ValueError:
            return None

        date_str = ts.split(" ")[0]                 # "m/d/yy"
        m, d, y = date_str.split("/")
        sort_key = (2000 + int(y), int(m), int(d))  # years are 2000-2099
        risk = co2 / Project2.CO2_REF + voc / Project2.VOC_REF + pm / Project2.PM25_REF
        return (dev, date_str, sort_key, risk, 1 if risk >= tau else 0)

    def run(self, inputPath, outputPath, tau):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        tau = float(tau)

        # 1-2. parse, drop invalid, aggregate per (device, date).
        valid = (sc.textFile(inputPath)
                   .map(lambda line: Project2._parse(line, tau))
                   .filter(lambda r: r is not None))

        # (device, date_str, sort_key) -> (sum_risk, count, risky_count)
        grouped = (valid
                   .map(lambda r: ((r[0], r[1], r[2]), (r[3], 1, r[4])))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])))

        # 3. keep risky dates; carry device -> (date_str, sort_key, avg_risk).
        risky = (grouped
                 .filter(lambda kv: kv[1][2] > 0)
                 .map(lambda kv: (kv[0][0], (kv[0][1], kv[0][2], kv[1][0] / kv[1][1]))))

        # 4. risky-date count per device.
        counts = (risky
                  .map(lambda kv: (kv[0], 1))
                  .reduceByKey(lambda a, b: a + b))

        # 5a. header rows: "device_id:number_of_risky_dates".
        headers = counts.map(lambda kv: (
            (-kv[1], kv[0], 0, (0, 0, 0)),
            "{}:{}".format(kv[0], kv[1])))

        # 5b. detail rows: "date\taverage_risk_score", joined with device count.
        details = (risky.join(counts).map(lambda kv: (
            (-kv[1][1], kv[0], 1, kv[1][0][1]),
            "{}\t{}".format(kv[1][0][0], kv[1][0][2]))))

        # Spark performs all ordering; coalesce(1) writes one ordered file.
        (headers.union(details)
                .sortByKey()
                .map(lambda kv: kv[1])
                .coalesce(1)
                .saveAsTextFile(outputPath))

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])
