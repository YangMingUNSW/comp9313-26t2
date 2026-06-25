from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class proj1(MRJob):
    """
    Single-step MRJob solution for latency stability analysis.

    Full design-pattern implementation:
      - In-mapper combining (mapper_init / mapper_final) shrinks intermediate
        data before it ever hits the shuffle.
      - Value-to-key conversion: the sort field is folded into the KEY as
        "device#token". Hadoop always sorts keys, so this gives a guaranteed
        total order (it does NOT rely on SORT_VALUES, which is ineffective on
        this Hadoop setup).
      - Order inversion: the overall stats use token "0", which sorts before
        every daily token ("1" + date), so a device's OVERALL average arrives
        at the reducer BEFORE any of its daily records. The reducer keeps the
        overall average in state and streams the days -- no buffering.
      - Secondary sort: daily tokens use a digit-complemented date, so ascending
        key order == DESCENDING date order, produced by the framework sort.
      - KeyFieldBasedPartitioner partitions on the device part of the key only
        (field 1, separator '#'), so every record for a device reaches the same
        reducer even with multiple reducers.
    """

    SEP = "#"
    OVERALL = "0"
    DAILY_PREFIX = "1"

    # Pass the partitioner via streaming's -partitioner flag (mrjob does this
    # for PARTITIONER). Using the -D mapreduce.job.partitioner.class jobconf
    # instead fails with "incompatible with map compatibility mode".
    PARTITIONER = "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partial = {}
        self.tau = 0.0
        self.cur_device = None
        self.overall_sum = 0.0
        self.overall_count = 0
        self.overall_avg = None

    @staticmethod
    def _inv_date(date):
        # Digit-wise 9's complement of a fixed-width YYYY-MM-DD string, so that
        # ascending lexicographic order corresponds to DESCENDING date order.
        return "".join(str(9 - int(ch)) if ch.isdigit() else ch for ch in date)

    # ---- map side: in-mapper combining -------------------------------------

    def mapper_init(self):
        self.partial = {}

    def mapper(self, _, line):
        parts = line.strip().split(",")
        if len(parts) != 3:
            return

        date = parts[0].strip()
        device = parts[1].strip()
        try:
            latency = float(parts[2].strip())
        except ValueError:
            return

        # (device, token) -> [sum, count, real_marker]
        ov = self.partial.setdefault((device, self.OVERALL), [0.0, 0, "*"])
        ov[0] += latency
        ov[1] += 1

        token = self.DAILY_PREFIX + self._inv_date(date)
        day = self.partial.setdefault((device, token), [0.0, 0, date])
        day[0] += latency
        day[1] += 1

    def mapper_final(self):
        for (device, token), (s, c, real) in self.partial.items():
            # Composite key "device#token": Hadoop sorts it, the partitioner
            # splits on '#' to partition by device only.
            yield device + self.SEP + token, (s, c, real)

    # ---- reduce side: stateful streaming across reducer() calls ------------

    def reducer_init(self):
        tau_str = jobconf_from_env("myjob.settings.tau")
        self.tau = float(tau_str) if tau_str is not None else 0.0
        self.cur_device = None
        self.overall_sum = 0.0
        self.overall_count = 0
        self.overall_avg = None

    def reducer(self, key, values):
        device, token = key.split(self.SEP, 1)

        # New device => reset the carried-over overall state.
        if device != self.cur_device:
            self.cur_device = device
            self.overall_sum = 0.0
            self.overall_count = 0
            self.overall_avg = None

        # Aggregate the partials grouped under this exact key.
        ksum = 0.0
        kcount = 0
        real = None
        for s, c, r in values:
            ksum += s
            kcount += c
            real = r

        if token == self.OVERALL:
            # Order inversion: overall key sorts first, finalize the average.
            self.overall_sum += ksum
            self.overall_count += kcount
            if self.overall_count:
                self.overall_avg = self.overall_sum / self.overall_count
            return

        # Daily key: overall is already known (it sorted earlier).
        if self.overall_avg is None:
            return

        daily_avg = ksum / kcount
        increase = daily_avg - self.overall_avg
        if increase > self.tau:
            yield device, "{}:{}".format(real, increase)

    def steps(self):
        jobconf = {
            # KeyFieldBasedPartitioner (set via PARTITIONER) reads these:
            # partition by the device part of the key only (field 1, sep '#'),
            # so a device is never split across reducers.
            "mapreduce.map.output.key.field.separator": self.SEP,
            "mapreduce.partition.keypartitioner.options": "-k1,1",
        }
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       mapper_final=self.mapper_final,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                       jobconf=jobconf)]


if __name__ == '__main__':
    proj1.run()
