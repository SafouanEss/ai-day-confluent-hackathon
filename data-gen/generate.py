"""Local truck_telemetry data generator.

Emits Avro records: 24h historical baseline (training), then realtime with degradation.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

ROOT = Path(__file__).parent
TOPIC = "truck_telemetry"
HISTORICAL_WINDOW_MS = 24 * 60 * 60 * 1000

SENSORS = {
    "engine_temp_c":       (88.0,  1.2,  2,  2.5),
    "oil_pressure_psi":    (52.0,  0.8,  2, -1.5),
    "vibration_g":         (0.32,  0.04, 4,  0.045),
    "tire_psi_avg":        (102.0, 0.6,  2, -0.7),
    "brake_pad_mm":        (13.0,  0.05, 2, -0.04),
    "fuel_efficiency_mpg": (7.2,   0.25, 2, -0.18),
}

AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "truck_telemetry_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "reading_id", "type": "string"},
        {"name": "truck_id", "type": "string"},
        {"name": "engine_temp_c", "type": "double"},
        {"name": "oil_pressure_psi", "type": "double"},
        {"name": "vibration_g", "type": "double"},
        {"name": "tire_psi_avg", "type": "double"},
        {"name": "brake_pad_mm", "type": "double"},
        {"name": "fuel_efficiency_mpg", "type": "double"},
        {"name": "rpm", "type": "int"},
        {"name": "speed_mph", "type": "double"},
        {"name": "ambient_temp_c", "type": "double"},
        {"name": "reading_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"Error: required env var {name} is not set.", file=sys.stderr)
        sys.exit(1)
    return v


def _clamped_normal(mean: float, sd: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, random.gauss(mean, sd)))


def _build_reading(truck_id: str, ts_ms: int, sim_start_ms: int, degrading: bool) -> dict:
    elapsed_minutes = max(0.0, (ts_ms - sim_start_ms) / 60_000.0)
    rec: dict = {
        "reading_id": f"TLM-{200_000_000 + ((ts_ms % 100_000) * 1000) + random.randint(0, 9999)}",
        "truck_id": truck_id,
        "rpm": int(_clamped_normal(1600, 200, 800, 2400)),
        "speed_mph": round(_clamped_normal(58, 8, 0, 75), 1),
        "ambient_temp_c": round(_clamped_normal(24, 5, -5, 42), 1),
        "reading_ts": ts_ms,
    }
    for name, (baseline, noise_sd, decimals, drift_rate) in SENSORS.items():
        drift = drift_rate if degrading else 0.0
        value = baseline + drift * elapsed_minutes + random.gauss(0, noise_sd)
        rec[name] = round(value, decimals)
    return rec


def _delivery_cb(err, _msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def _produce_with_backpressure(
    producer: Producer, topic: str, key: bytes, value: bytes
) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=_delivery_cb)
            return
        except BufferError:
            producer.poll(0.2)


def _truck_loop(
    producer: Producer,
    key_ser: StringSerializer,
    val_ser: AvroSerializer,
    truck_id: str,
    degrading: bool,
    sim_start_ms: int,
    stop_at: float,
    counters: dict,
) -> None:
    # Per-truck steady-state throttle mean — matches steadyStateMean uniform[1500,3000]
    # × steadyStateThrottle normal(mean, sd=400, clamp[800,5000]) per emission.
    steady_state_mean = random.uniform(1500, 3000)

    def _throttle_ms(mean: float, sd: float) -> float:
        return _clamped_normal(mean, sd, 800, 5000)

    ts = sim_start_ms - HISTORICAL_WINDOW_MS
    key_ctx = SerializationContext(TOPIC, MessageField.KEY)
    val_ctx = SerializationContext(TOPIC, MessageField.VALUE)

    # Phase 1: historical replay (no real-time sleep, just advance virtual clock).
    while ts < sim_start_ms:
        rec = _build_reading(truck_id, ts, sim_start_ms, degrading=degrading)
        _produce_with_backpressure(
            producer, TOPIC,
            key=key_ser(truck_id, key_ctx),
            value=val_ser(rec, val_ctx),
        )
        counters["historical"] += 1
        ts += int(_throttle_ms(steady_state_mean, 400))

    # Phase 2: real-time emission, throttled per ShadowTraffic schedule.
    while time.time() < stop_at:
        now_ms = int(time.time() * 1000)
        rec = _build_reading(truck_id, now_ms, sim_start_ms, degrading=degrading)
        _produce_with_backpressure(
            producer, TOPIC,
            key=key_ser(truck_id, key_ctx),
            value=val_ser(rec, val_ctx),
        )
        counters["realtime"] += 1
        throttle = _throttle_ms(1500, 200) if degrading else _throttle_ms(steady_state_mean, 400)
        time.sleep(throttle / 1000.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local truck_telemetry data generator.")
    parser.add_argument(
        "--max-ms", type=int, default=600_000,
        help="Total wall-clock runtime in ms (default 600000 = 10 min).",
    )
    args = parser.parse_args()

    healthy = json.loads((ROOT / "trucks/healthy-trucks.json").read_text())
    degrading = json.loads((ROOT / "trucks/degrading-trucks.json").read_text())

    bootstrap = _require_env("KAFKA_BOOTSTRAP_SERVERS")
    sr = SchemaRegistryClient({
        "url": _require_env("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": f"{_require_env('SCHEMA_REGISTRY_API_KEY')}:{_require_env('SCHEMA_REGISTRY_API_SECRET')}",
    })
    val_ser = AvroSerializer(sr, AVRO_SCHEMA)
    key_ser = StringSerializer("utf_8")
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": _require_env("KAFKA_API_KEY"),
        "sasl.password": _require_env("KAFKA_API_SECRET"),
        "linger.ms": 50,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
    })

    sim_start_ms = int(time.time() * 1000)
    stop_at = time.time() + args.max_ms / 1000.0
    counters = {"historical": 0, "realtime": 0}

    threads: list[threading.Thread] = []
    for truck_id in healthy:
        threads.append(threading.Thread(
            target=_truck_loop,
            args=(producer, key_ser, val_ser, truck_id, False, sim_start_ms, stop_at, counters),
            daemon=True,
        ))
    for truck_id in degrading:
        threads.append(threading.Thread(
            target=_truck_loop,
            args=(producer, key_ser, val_ser, truck_id, True, sim_start_ms, stop_at, counters),
            daemon=True,
        ))

    print(
        f"Generating into {TOPIC} for {args.max_ms / 1000:.0f}s "
        f"({len(healthy)} healthy + {len(degrading)} degrading trucks)",
        flush=True,
    )
    for t in threads:
        t.start()

    last_report = time.time()
    try:
        while any(t.is_alive() for t in threads):
            producer.poll(0.5)
            if time.time() - last_report >= 5:
                print(
                    f"  historical={counters['historical']}  realtime={counters['realtime']}",
                    flush=True,
                )
                last_report = time.time()
    except KeyboardInterrupt:
        print("\nInterrupted, flushing...", flush=True)

    producer.flush(30)
    print(
        f"Done. Sent {counters['historical']} historical + {counters['realtime']} realtime records.",
        flush=True,
    )


if __name__ == "__main__":
    main()
