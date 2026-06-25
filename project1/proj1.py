from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class proj1(MRJob):
    """
    Single-step MRJob solution for latency stability analysis.

    Techniques used:
      - In-mapper combining (mapper_init / mapper_final) plus a combiner to
        shrink the amount of intermediate data.
      - Order inversion: a special sort token ("0") makes every device's
        OVERALL partials arrive at the reducer BEFORE any of its daily
        partials, so the overall average is fully known before the first
        daily record is processed.
      - Secondary sort: SORT_VALUES streams each device's daily partials to
        the reducer already ordered by date DESCENDING (achieved with a
        digit-complemented date string), so the reducer never has to buffer
        or sort all of a device's days in memory.
    """

    # Partition + group by the key (device), but ALSO sort by the value, so a
    # single reducer() call receives one device's values in fully sorted order.
    SORT_VALUES = True

    OVERALL = "0"          # sort token for the per-device overall stats
    DAILY_PREFIX = "1"     # sort token prefix for the per-day stats

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partial = {}
        self.tau = 0.0

    @staticmethod
    def _inv_date(date):
        # Digit-wise 9's complement of a fixed-width YYYY-MM-DD string, so that
        # ascending lexicographic order corresponds to DESCENDING date order
        # (separators are left untouched and stay in the same positions).
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

        # Each entry: (device, sort_token) -> [real_marker, sum, count]
        ov = self.partial.setdefault((device, self.OVERALL), ["*", 0.0, 0])
        ov[1] += latency
        ov[2] += 1

        token = self.DAILY_PREFIX + self._inv_date(date)
        day = self.partial.setdefault((device, token), [date, 0.0, 0])
        day[1] += latency
        day[2] += 1

    def mapper_final(self):
        for (device, token), (real, s, c) in self.partial.items():
            # Key = device (used for partition + group). The value carries the
            # sort token first so SORT_VALUES puts overall-before-daily and
            # orders the days descending.
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

    # ---- reduce side: streaming, no full buffering -------------------------

    def reducer_init(self):
        tau_str = jobconf_from_env("myjob.settings.tau")
        self.tau = float(tau_str) if tau_str is not None else 0.0

    def reducer(self, device, values):
        overall_sum = 0.0
        overall_count = 0
        overall_avg = None

        cur_date = None
        cur_sum = 0.0
        cur_count = 0

        for token, real, s, c in values:
            if token == self.OVERALL:
                # Order inversion: every overall partial arrives first.
                overall_sum += s
                overall_count += c
                continue

            # First daily record => the overall stats are now complete.
            if overall_avg is None:
                if overall_count == 0:
                    return
                overall_avg = overall_sum / overall_count

            # Secondary sort: daily partials arrive grouped by date, descending.
            # Merge the (possibly several) partials for the current date, then
            # emit as soon as the date changes -- only one day is held at a time.
            if real != cur_date:
                if cur_date is not None:
                    increase = (cur_sum / cur_count) - overall_avg
                    if increase > self.tau:
                        yield device, "{}:{}".format(cur_date, increase)
                cur_date = real
                cur_sum = 0.0
                cur_count = 0
            cur_sum += s
            cur_count += c

        # Flush the final date.
        if cur_date is not None:
            increase = (cur_sum / cur_count) - overall_avg
            if increase > self.tau:
                yield device, "{}:{}".format(cur_date, increase)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       mapper_final=self.mapper_final,
                       combiner=self.combiner,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer)]


if __name__ == '__main__':
    proj1.run()
