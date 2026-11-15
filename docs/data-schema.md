# Shared synthetic sales data and Spark session settings

This project is part 1 of a five-part PySpark series. Every part is runnable on its own
(code is copied, not shared), but they agree on the data and session conventions below.

## Columns of the generated CSV (`data/sales_csv/`)

| Column | Type | Meaning |
|---|---|---|
| `order_id` | long | Row id `0..rows-1` (rows removed by the closed-store rule leave gaps) |
| `ts` | timestamp (UTC) | Sale time in 2025, written as `yyyy-MM-dd HH:mm:ss` |
| `store` | string | `S1`..`S5`, size weights 30 / 25 / 20 / 15 / 10 percent |
| `product` | string | `P01`..`P12` |
| `category` | string | `Food` (P01-P03), `Drinks` (P04-P06), `Home` (P07-P09), `Toys` (P10-P12) |
| `qty` | int | Base 1..5 times the month multiplier, rounded, at least 1 |
| `unit_price` | int | Fixed per product (3,200 .. 21,000) |

Columns added by `prepare()` at read time: `amount = qty * unit_price` (long), `month`
(1-12), `day` (date), `day_no` (days since 2025-01-01).

## Patterns planted on purpose

* **Seasonality**: month multipliers Jan 1.0, Feb 0.8, Mar-Apr 1.0, May-Jun 1.1, Jul-Aug 1.0,
  Sep 1.1, Oct 1.2, Nov 1.4, Dec 1.7.
* **Closed store**: `S5` has no rows on Mondays (so `rowsBetween` and `rangeBetween` differ).
* **Store sizes**: S1 is the biggest, S5 the smallest.

## Generation rules

* `spark.range(rows)` plus column expressions; no Python UDF, no download.
* Every key uses its own hash: `pmod(xxhash64(id, seed, "<column name>"), n)`. Never derive
  several keys from `id % n`: they become correlated (store 0 would sell only every 5th day).
* The same `rows` and `seed` always give the same data. `--rows` is the requested count;
  the real count is a little lower because of the closed-store rule.

## Session settings every part uses

| Setting | Value | Why |
|---|---|---|
| `spark.sql.session.timeZone` | `UTC` | month and day boundaries must not depend on your computer |
| `spark.sql.shuffle.partitions` | `8` | the default 200 creates mostly empty tasks on a laptop |
| `java.io.tmpdir` (via `JAVA_TOOL_OPTIONS`) | `<workdir>/.spark-tmp` | JVM ignores `$TMPDIR`; locked-down machines fail with `JAVA_GATEWAY_EXITED` otherwise |
| `SPARK_LOCAL_DIRS` | `<workdir>/.spark-tmp` | Spark scratch files stay inside the project |
| `spark.ui.enabled` | `false` (true with `--ui`) | no port 4040 needed |
| `spark.driver.host` / `bindAddress` | `127.0.0.1` | local only |

Values you set yourself (`JAVA_TOOL_OPTIONS` with `java.io.tmpdir`, `SPARK_LOCAL_DIRS`) are respected.
