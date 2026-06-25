from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class proj1(MRJob):
    """
    Single-step MRJob solution for latency stability analysis.

    Design:
      - In-mapper combining (mapper_init / mapper_final) + a combiner shrink
        the intermediate data.
      - Order inversion: a special sort token "0" is attached to each device's
        OVERALL partials so they sort before the daily partials ("1" + date).
      - Secondary sort: SORT_VALUES asks Hadoop to deliver each device's values
        already ordered -- overall first, then days DESCENDING (via a
        digit-complemented date). The reducer therefore emits the days in the
        order they arrive, without an in-memory sort.

    Correctness does NOT depend on the ordering: the reducer fully accumulates
    every overall partial before computing the overall average, and aggregates
    each day's partials, so the LatencyIncrease values are always exact even if
    the framework ordering were unavailable.
    """

    # Partition + group by key (device); also sort values, so one reducer call
    # receives a device's values in sorted order (overall first, days desc).
    SORT_VALUES = True

    OVERALL = "0"          # sort token for the per-device overall stats
    DAILY_PREFIX = "1"     # sort token prefix for the per-day stats

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partial = {}
        self.tau = 0.0

    @staticmethod
    def _inv_date(date):
        # Digit-wise 9's complement of a fixed-width YYYY-MM-DD string so that
        # ascending lexicographic order == DESCENDING date order (separators
        # stay in place and keep the same relative positions).
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

        # Entry: (device, sort_token) -> [real_marker, sum, count]
        ov = self.partial.setdefault((device, self.OVERALL), ["*", 0.0, 0])
        ov[1] += latency
        ov[2] += 1

        token = self.DAILY_PREFIX + self._inv_date(date)
        day = self.partial.setdefault((device, token), [date, 0.0, 0])
        day[1] += latency
        day[2] += 1

    def mapper_final(self):
        for (device, token), (real, s, c) in self.partial.items():
            # Key = device (partition + group). Value leads with the sort token
            # so SORT_VALUES puts overall-before-daily and orders days desc.
            yield device, [token, real, s, c]

    # ---- combiner: merge partials that share the same sort token ------------

    def combiner(self, device, values):
        agg = {}
        for token, real, s, c in values:
            row = agg.setdefault(token, [real, 0.0, 0])
            row[1] += s
            row[2] += c
        for token, (real, s, c) in agg.items():
            yield device, [token, real, s, c]

    # ---- reduce side -------------------------------------------------------

    def reducer_init(self):
        tau_str = jobconf_from_env("myjob.settings.tau")
        self.tau = float(tau_str) if tau_str is not None else 0.0

    def reducer(self, device, values):
        overall_sum = 0.0
        overall_count = 0

        order = []          # distinct dates in framework-delivered order
        agg = {}            # date -> [sum, count]

        for token, real, s, c in values:
            if token == self.OVERALL:
                # Accumulate ALL overall partials (correct regardless of order).
                overall_sum += s
                overall_count += c
            else:
                if real not in agg:
                    agg[real] = [0.0, 0]
                    order.append(real)
                agg[real][0] += s
                agg[real][1] += c

        if overall_count == 0:
            return

        overall_avg = overall_sum / overall_count

        # Emit in the order the days arrived (descending when SORT_VALUES is in
        # effect) -- no in-memory sort, so this reflects the secondary sort.
        for date in order:
            day_sum, day_count = agg[date]
            increase = (day_sum / day_count) - overall_avg
            if increase > self.tau:
                yield device, "{}:{}".format(date, increase)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       mapper_final=self.mapper_final,
                       combiner=self.combiner,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer)]


if __name__ == '__main__':
    proj1.run()
