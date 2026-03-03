"""
Benchmark: Hidden Column Throughput Impact

Question: Does generating N hidden columns (rand()-based, omit=True) that are
mostly unused per row materially impact sustained throughput?

Setup:
- Single DataGenerator with a weighted event_type discriminator (3 types)
- 3 conditional output fields, each routed via CASE WHEN from hidden columns
- Variable: number of ADDITIONAL hidden columns (simulating spec dedup waste)
  These extra hidden columns use rand()-based generation but are never referenced
  in any output column -- they represent the "waste" when every row materializes
  hidden columns for event types it doesn't belong to.

We measure batch generation throughput (rows/sec) at:
- 0 extra hidden cols   (baseline: only the 3 that are actually used)
- 50 extra hidden cols
- 200 extra hidden cols
- 500 extra hidden cols

Each test generates 500K rows and measures wall-clock time.
"""

import time
import dbldatagen as dg
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ROW_COUNT = 500_000
TRIALS = 3
HIDDEN_COL_COUNTS = [0, 50, 200, 500]

EVENT_TYPES = ["page_view", "click", "purchase"]
EVENT_WEIGHTS = [6, 3, 1]


def build_generator(extra_hidden_cols: int) -> dg.DataGenerator:
    spec = (
        dg.DataGenerator(spark, rows=ROW_COUNT, partitions=8, randomSeedMethod="hash_fieldname")
        .withColumn("id", "long", uniqueValues=ROW_COUNT)
        .withColumn(
            "__dsg_event_type_id", "string",
            values=EVENT_TYPES, weights=EVENT_WEIGHTS, random=True, omit=True
        )
    )

    # 3 "real" hidden columns that ARE used via CASE WHEN routing
    spec = spec.withColumn("__base_price", "float", minValue=1.0, maxValue=500.0, random=True, omit=True)
    spec = spec.withColumn("__base_quantity", "int", minValue=1, maxValue=100, random=True, omit=True)
    spec = spec.withColumn("__base_score", "double", minValue=0.0, maxValue=1.0, random=True, omit=True)

    # Output columns that reference hidden columns via CASE WHEN
    spec = spec.withColumn("price", "float",
        expr="CASE WHEN __dsg_event_type_id = 'purchase' THEN __base_price ELSE NULL END",
        baseColumn=["__dsg_event_type_id", "__base_price"])
    spec = spec.withColumn("quantity", "int",
        expr="CASE WHEN __dsg_event_type_id = 'click' THEN __base_quantity ELSE NULL END",
        baseColumn=["__dsg_event_type_id", "__base_quantity"])
    spec = spec.withColumn("score", "double",
        expr="CASE WHEN __dsg_event_type_id = 'page_view' THEN __base_score ELSE NULL END",
        baseColumn=["__dsg_event_type_id", "__base_score"])

    # Extra hidden columns: rand()-based, omit=True, NEVER referenced by output
    # This simulates the waste when all hidden columns are materialized per row
    for i in range(extra_hidden_cols):
        col_type = ["float", "int", "double"][i % 3]
        if col_type == "float":
            spec = spec.withColumn(f"__waste_{i}", col_type, minValue=0.0, maxValue=1000.0, random=True, omit=True)
        elif col_type == "int":
            spec = spec.withColumn(f"__waste_{i}", col_type, minValue=0, maxValue=10000, random=True, omit=True)
        else:
            spec = spec.withColumn(f"__waste_{i}", col_type, minValue=0.0, maxValue=1.0, random=True, omit=True)

    # A common output field (always present, no CASE WHEN)
    spec = spec.withColumn("event_name", "string",
        values=EVENT_TYPES, weights=EVENT_WEIGHTS, random=True)

    return spec


def run_trial(extra_hidden_cols: int, trial: int) -> dict:
    spec = build_generator(extra_hidden_cols)

    # Measure build time (plan compilation)
    t0 = time.time()
    df = spec.build()
    build_time = time.time() - t0

    # Measure materialization time (actual row generation)
    t1 = time.time()
    count = df.count()
    materialize_time = time.time() - t1

    # Measure full write (forces all column evaluation including hidden)
    t2 = time.time()
    df.write.format("noop").mode("overwrite").save()
    write_time = time.time() - t2

    rows_per_sec_count = count / materialize_time if materialize_time > 0 else 0
    rows_per_sec_write = count / write_time if write_time > 0 else 0

    return {
        "extra_hidden_cols": extra_hidden_cols,
        "total_hidden_cols": extra_hidden_cols + 3,
        "trial": trial,
        "row_count": count,
        "build_time_s": round(build_time, 2),
        "count_time_s": round(materialize_time, 2),
        "write_time_s": round(write_time, 2),
        "rows_per_sec_count": int(rows_per_sec_count),
        "rows_per_sec_write": int(rows_per_sec_write),
    }


print("=" * 80)
print("BENCHMARK: Hidden Column Throughput Impact")
print(f"Rows per test: {ROW_COUNT:,}  |  Trials per config: {TRIALS}")
print("=" * 80)

all_results = []
for n in HIDDEN_COL_COUNTS:
    print(f"\n--- Testing with {n} extra hidden columns ({n + 3} total) ---")
    for t in range(TRIALS):
        result = run_trial(n, t + 1)
        all_results.append(result)
        print(f"  Trial {t+1}: build={result['build_time_s']}s  "
              f"count={result['count_time_s']}s ({result['rows_per_sec_count']:,}/s)  "
              f"write={result['write_time_s']}s ({result['rows_per_sec_write']:,}/s)")

    # Average across trials
    trials = [r for r in all_results if r["extra_hidden_cols"] == n]
    avg_count = sum(r["rows_per_sec_count"] for r in trials) / len(trials)
    avg_write = sum(r["rows_per_sec_write"] for r in trials) / len(trials)
    avg_build = sum(r["build_time_s"] for r in trials) / len(trials)
    print(f"  AVG: build={avg_build:.2f}s  count={avg_count:,.0f}/s  write={avg_write:,.0f}/s")

# Summary table
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"{'Extra Hidden':>14} {'Total Hidden':>14} {'Avg Build(s)':>14} "
      f"{'Avg Count/s':>14} {'Avg Write/s':>14} {'Δ vs Baseline':>14}")
print("-" * 86)

baseline_write = None
for n in HIDDEN_COL_COUNTS:
    trials = [r for r in all_results if r["extra_hidden_cols"] == n]
    avg_build = sum(r["build_time_s"] for r in trials) / len(trials)
    avg_count = sum(r["rows_per_sec_count"] for r in trials) / len(trials)
    avg_write = sum(r["rows_per_sec_write"] for r in trials) / len(trials)
    if baseline_write is None:
        baseline_write = avg_write
        delta = "baseline"
    else:
        pct = ((avg_write - baseline_write) / baseline_write) * 100
        delta = f"{pct:+.1f}%"
    print(f"{n:>14} {n + 3:>14} {avg_build:>14.2f} "
          f"{avg_count:>14,.0f} {avg_write:>14,.0f} {delta:>14}")

print("\nConclusion: If write throughput drops >15% at 200 hidden cols,")
print("implement guarded hidden columns (lazy generation) in Phase 1.")
