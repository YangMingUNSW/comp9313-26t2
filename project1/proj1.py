from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class proj1(MRJob):
    """
    Single-step MRJob solution for latency stability analysis.

    Design:
      - In-mapper combining (mapper_init / mapper_final) plus a combiner shrink
        the amount of intermediate data emitted to the shuffle.
      - Order inversion: a special key marker '*' carries each device's OVERALL
        sum/count so the reducer can separate it from the per-day partials.
      - The reducer keys only by device, so every record for one device lands
        in the same reducer; the per-day output is sorted by date DESCENDING.

    Correctness does not depend on value arrival order: the reducer fully
    accumulates the overall partials before computing the overall average.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partial = {}
        self.tau = 0.0

    # ---- map side: in-mapper combining -------------------------------------

    def mapper_init(self):
        # (device, marker) -> [sum, count]; marker is '*' (overall) or a date.
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

        overall = self.partial.setdefault((device, "*"), [0.0, 0])
        overall[0] += latency
        overall[1] += 1

        daily = self.partial.setdefault((device, date), [0.0, 0])
        daily[0] += latency
        daily[1] += 1

    def mapper_final(self):
        for (device, marker), (s, c) in self.partial.items():
            # Key only by device so all of a device's records share one reducer.
            yield device, (marker, s, c)

    # ---- combiner ----------------------------------------------------------

    def combiner(self, device, values):
        agg = {}
        for marker, s, c in values:
            row = agg.setdefault(marker, [0.0, 0])
            row[0] += s
            row[1] += c
        for marker, (s, c) in agg.items():
            yield device, (marker, s, c)

    # ---- reduce side -------------------------------------------------------

    def reducer_init(self):
        tau_str = jobconf_from_env("myjob.settings.tau")
        self.tau = float(tau_str) if tau_str is not None else 0.0

    def reducer(self, device, values):
        total_sum = 0.0
        total_count = 0
        daily = {}

        for marker, s, c in values:
            if marker == "*":
                total_sum += s
                total_count += c
            else:
                row = daily.setdefault(marker, [0.0, 0])
                row[0] += s
                row[1] += c

        if total_count == 0:
            return

        overall_avg = total_sum / total_count

        # Output sorted by date DESCENDING within each device.
        for date in sorted(daily.keys(), reverse=True):
            day_sum, day_count = daily[date]
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
