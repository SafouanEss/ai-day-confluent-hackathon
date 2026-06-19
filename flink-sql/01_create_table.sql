-- Truck telemetry source table.
-- The Kafka topic `truck_telemetry` is produced by the shadowtraffic data generator
-- (see ../data-gen). Each reading is a per-truck IoT snapshot of engine, oil,
-- vibration, tire, brake, and fuel-efficiency signals.

CREATE OR REPLACE TABLE `truck_telemetry` (
  `reading_id`          STRING NOT NULL,
  `truck_id`            STRING NOT NULL,
  `engine_temp_c`       DOUBLE NOT NULL,
  `oil_pressure_psi`    DOUBLE NOT NULL,
  `vibration_g`         DOUBLE NOT NULL,
  `tire_psi_avg`        DOUBLE NOT NULL,
  `brake_pad_mm`        DOUBLE NOT NULL,
  `fuel_efficiency_mpg` DOUBLE NOT NULL,
  `rpm`                 INT NOT NULL,
  `speed_mph`           DOUBLE NOT NULL,
  `ambient_temp_c`      DOUBLE NOT NULL,
  `reading_ts`          TIMESTAMP(3) NOT NULL,
  WATERMARK FOR `reading_ts` AS `reading_ts` - INTERVAL '5' SECOND
);
