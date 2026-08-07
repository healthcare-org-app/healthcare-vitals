"""Kafka consumers for vitals-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("vitals-service.consumers")

TABLE = "vitals"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("encounter.started")
    def _on_encounter_started(envelope: dict) -> None:
        log.info("vitals-service: received encounter.started id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.encounter.started", actor="system:vitals-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("device.reading")
    def _on_device_reading(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    metric = data.get("metric")
                    # Only certain metrics count as vitals for the chart.
                    if metric not in ("heart_rate", "spo2", "temperature", "blood_pressure"): return
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": data.get("patient_id"),
                                      "metric": metric,
                                      "value":  data.get("value"),
                                      "recorded_at": envelope.get("occurred_at")}),))
        except Exception as e:
            log.exception("vitals-service/device.reading handler failed: %s", e)
        emit_audit(bus, action="consume.device.reading", actor="system:vitals-service",
                   target=None, details={"envelope_id": envelope.get("id")})

