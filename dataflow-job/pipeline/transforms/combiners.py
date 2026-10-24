import apache_beam as beam

from pipeline.checks import MAX_SAMPLES


class SampleAndCountFn(beam.CombineFn):
    """Counts rejected rows, keeps up to MAX_SAMPLES of them and tallies their error rules.

    Input elements are dicts with 'line' and optionally 'errors' (list of 'column.RULE').
    Output: {'count': int, 'samples': [...], 'rule_counts': {rule: rows}}.
    """

    def create_accumulator(self):
        return {"count": 0, "samples": [], "rule_counts": {}}

    def add_input(self, acc, element):
        acc["count"] += 1
        if len(acc["samples"]) < MAX_SAMPLES:
            acc["samples"].append(element)
        for rule in set(element.get("errors", [])):
            acc["rule_counts"][rule] = acc["rule_counts"].get(rule, 0) + 1
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["samples"].extend(acc["samples"][: MAX_SAMPLES - len(merged["samples"])])
            for rule, n in acc["rule_counts"].items():
                merged["rule_counts"][rule] = merged["rule_counts"].get(rule, 0) + n
        return merged

    def extract_output(self, acc):
        return acc


class AllChecksPassedFn(beam.CombineFn):
    """True when no ValidationResult is blocking (an ERROR-severity failure)."""

    def create_accumulator(self):
        return True

    def add_input(self, acc, result):
        return acc and not result.is_blocking

    def merge_accumulators(self, accumulators):
        return all(accumulators)

    def extract_output(self, acc):
        return acc
