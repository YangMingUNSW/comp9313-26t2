from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):
    """
    One-step MRJob solution for latency stability analysis.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partial = {}
        self.tau = 0.0

    def mapper_init(self):
        # In-mapper combining to reduce intermediate emissions.
        # Keyed by (device_id, marker), where marker is '*' (overall) or date.
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

        # Special key '*' is used for order inversion style overall stats.
        overall_key = (device, "*")
        daily_key = (device, date)

        if overall_key not in self.partial:
            self.partial[overall_key] = [0.0, 0]
        self.partial[overall_key][0] += latency
        self.partial[overall_key][1] += 1

        if daily_key not in self.partial:
            self.partial[daily_key] = [0.0, 0]
        self.partial[daily_key][0] += latency
        self.partial[daily_key][1] += 1

    def mapper_final(self):
        for (device, marker), (s, c) in self.partial.items():
            # Key only by device so all records of one device go to one reducer.
            # Value keeps marker ('*' or date) for order inversion processing.
            yield device, (marker, s, c)

    def combiner(self, device, values):
        agg = {}
        for marker, s, c in values:
            if marker not in agg:
                agg[marker] = [0.0, 0]
            agg[marker][0] += s
            agg[marker][1] += c

        for marker, (s, c) in agg.items():
            yield device, (marker, s, c)

    def reducer_init(self):
        tau_str = jobconf_from_env("myjob.settings.tau")
        if tau_str is None:
            tau_str = "0"
        self.tau = float(tau_str)

    def reducer(self, device, values):
        total_sum = 0.0
        total_count = 0
        daily = {}

        for marker, s, c in values:
            if marker == "*":
                total_sum += s
                total_count += c
            else:
                if marker not in daily:
                    daily[marker] = [0.0, 0]
                daily[marker][0] += s
                daily[marker][1] += c

        if total_count == 0:
            return

        overall_avg = total_sum / total_count

        # Secondary sort: date descending within each device.
        for date in sorted(daily.keys(), reverse=True):
            day_sum, day_count = daily[date]
            daily_avg = day_sum / day_count
            increase = daily_avg - overall_avg
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
