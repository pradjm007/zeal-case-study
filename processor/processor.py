from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, LongType
)
from pyspark.sql.streaming import StreamingQueryListener
from datetime import datetime
from clickhouse_driver import Client

BUFFER_TIMEOUT_MS = 5 * 60 * 1000  # 5 minutes in milliseconds

# ─── ClickHouse setup ────────────────────────────────────────────────────────

client = Client('clickhouse', port=9000, user='default', password='clickhouse')

# ─── Spark setup ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("TelemetryProcessor-ClickHouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("timestamp",      LongType(),    True),
    StructField("viewer_id",      StringType(),  True),
    StructField("session_id",     StringType(),  True),
    StructField("event_type",     StringType(),  True),
    StructField("video_position", DoubleType(),  True),
    StructField("bitrate",        IntegerType(), True),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telemetry") \
    .option("startingOffsets", "latest") \
    .option("kafka.group.id", "processor-group") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

events_df = parsed_df.withColumn("processed_at", current_timestamp())

# ─── State ───────────────────────────────────────────────────────────────────
# (viewer_id, session_id) → buffer_start_ts (epoch ms)

buffer_state: dict[tuple, int] = {}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)

def _flush_timeouts(epoch_id: int, batch_now_ms: int) -> list[tuple]:
    """
    Scan buffer_state for entries older than BUFFER_TIMEOUT_MS.
    Remove them from state and return rows for buffer_timeouts.
    """
    expired_keys = [
        key
        for key, start_ts in buffer_state.items()
        if (batch_now_ms - start_ts) >= BUFFER_TIMEOUT_MS
    ]

    timeout_rows = []
    for key in expired_keys:
        start_ts = buffer_state.pop(key)
        elapsed_ms = batch_now_ms - start_ts
        viewer_id, session_id = key
        print(
            f"[{epoch_id}] timeout  viewer={viewer_id} session={session_id} "
            f"elapsed={elapsed_ms}ms"
        )
        timeout_rows.append((
            viewer_id,
            session_id,
            "timeout",       # reason
            "BUFFER_START",  # the event that was never closed
            0,               # event_ts — no END event arrived
            start_ts,
            elapsed_ms,
            datetime.utcnow(),
        ))

    return timeout_rows

# ─── Batch writer ─────────────────────────────────────────────────────────────

def write_batch(df, epoch_id):
    batch_now_ms = _now_ms()

    # ── 1. Timeout sweep (runs every batch, before processing new events) ──
    timeout_rows = _flush_timeouts(epoch_id, batch_now_ms)

    if df.isEmpty():
        _insert_timeouts(epoch_id, timeout_rows)
        return

    rows = df.collect()

    # ── 2. Raw events → telemetry_events ──
    raw_rows = [
        (
            row.event_id,
            row.timestamp,
            row.viewer_id,
            row.session_id,
            row.event_type,
            float(row.video_position) if row.video_position is not None else 0.0,
            int(row.bitrate)          if row.bitrate          is not None else 0,
            row.processed_at          if row.processed_at      is not None else datetime.utcnow(),
        )
        for row in rows
    ]
    try:
        client.execute(
            "INSERT INTO telemetry_events "
            "(event_id, timestamp, viewer_id, session_id, event_type, "
            " video_position, bitrate, processed_at) VALUES",
            raw_rows
        )
        print(f"[{epoch_id}] raw events   → {len(raw_rows)} rows")
    except Exception as exc:
        import traceback
        print(f"[{epoch_id}] raw insert failed: {exc}")
        traceback.print_exc()

    # ── 3. State transitions ──
    sorted_rows   = sorted(rows, key=lambda r: r.timestamp or 0)
    duration_rows = []

    for row in sorted_rows:
        key        = (row.viewer_id, row.session_id)
        event_ts   = row.timestamp or 0

        if row.event_type == "BUFFER_START":
            # Overwrite any stale start — the previous one should have timed out
            # already, but guard against rapid restarts within the same session.
            buffer_state[key] = event_ts

        elif row.event_type == "BUFFER_END":
            start_ts = buffer_state.pop(key, None)

            if start_ts is None:
                # Orphan END: no matching start in state (missed, restarted, etc.)
                viewer_id, session_id = key
                print(
                    f"[{epoch_id}] orphan_end  viewer={viewer_id} "
                    f"session={session_id} ts={event_ts}"
                )
                timeout_rows.append((
                    viewer_id,
                    session_id,
                    "orphan_end",    # reason
                    "BUFFER_END",    # the event that arrived without a start
                    event_ts,
                    0,               # start_ts unknown
                    0,               # elapsed unknown
                    datetime.utcnow(),
                ))
                continue

            duration_ms = event_ts - start_ts

            if duration_ms < 0:
                print(
                    f"[{epoch_id}] negative duration ({duration_ms} ms) "
                    f"viewer={row.viewer_id} — skipping"
                )
                continue

            duration_rows.append((
                row.viewer_id,
                row.session_id,
                start_ts,
                event_ts,
                duration_ms,
                datetime.utcnow(),
            ))

    # ── 4. Write durations ──
    if duration_rows:
        try:
            client.execute(
                "INSERT INTO buffer_durations "
                "(viewer_id, session_id, buffer_start_ts, buffer_end_ts, "
                " duration_ms, calculated_at) VALUES",
                duration_rows
            )
            print(f"[{epoch_id}] durations    → {len(duration_rows)} rows")
        except Exception as exc:
            import traceback
            print(f"[{epoch_id}] duration insert failed: {exc}")
            traceback.print_exc()

    # ── 5. Write timeouts (sweep + orphan_ends collected above) ──
    _insert_timeouts(epoch_id, timeout_rows)


def _insert_timeouts(epoch_id: int, rows: list[tuple]) -> None:
    if not rows:
        return
    try:
        client.execute(
            "INSERT INTO buffer_timeouts "
            "(viewer_id, session_id, reason, event_type, event_ts, "
            " start_ts, elapsed_ms, recorded_at) VALUES",
            rows
        )
        print(f"[{epoch_id}] timeouts     → {len(rows)} rows")
    except Exception as exc:
        import traceback
        print(f"[{epoch_id}] timeout insert failed: {exc}")
        traceback.print_exc()

# ─── Listener ────────────────────────────────────────────────────────────────

class TriggerLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started : {event.id}")
    def onQueryProgress(self, event):
        print(f"Batch progress: {event.progress.numInputRows} input rows  "
              f"| state size: {len(buffer_state)}")
    def onQueryTerminated(self, event):
        print(f"Query stopped : {event.id}")
    def onQueryIdle(self, event):
        pass

spark.streams.addListener(TriggerLogger())

# ─── Start streaming ──────────────────────────────────────────────────────────

query = events_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/checkpoints/telemetry_events") \
    .start()

print("Streaming started — writing to: telemetry_events, buffer_durations, buffer_timeouts")
query.awaitTermination()