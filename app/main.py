"""
Smart Parking G3 - Backend/API (FastAPI + Prisma + MQTT)

ESP32 --MQTT--> este servico --Prisma--> BD --REST--> dashboard (HTML)

Topicos:
  <BASE>/status  (ESP32 -> servidor)  estado completo a cada 10 s
  <BASE>/event   (ESP32 -> servidor)  eventos pontuais
  <BASE>/cmd     (servidor -> ESP32)  AUTO | OPEN | CLOSE
"""
import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prisma import Prisma

MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
BASE = os.getenv("MQTT_BASE", "ujac/dsio/g3/parking")
LOT_ID = os.getenv("LOT_ID", "G3-LOT-01")
ONLINE_WINDOW_S = 30          # sem mensagens ha mais de 30 s => OFFLINE
SNAPSHOT_MIN_GAP_S = 60       # no maximo 1 snapshot/min se nada mudou

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

db = Prisma()
mqtt_client: mqtt.Client | None = None
_last_snapshot: dict[str, tuple] = {}   # lotId -> (free, occupied, gate, mode, datetime)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


# --------------------------------------------------------------------------
# Tratamento das mensagens MQTT
# --------------------------------------------------------------------------
async def ensure_lot(lot_id: str, total: int | None = None):
    total = total or 4
    return await db.parkinglot.upsert(
        where={"id": lot_id},
        data={
            "create": {"id": lot_id, "name": f"Parque {lot_id}", "totalSpots": total},
            "update": {"totalSpots": total},
        },
    )


async def handle_status(data: dict):
    lot_id = data.get("lotId") or LOT_ID
    await ensure_lot(lot_id, data.get("total"))

    await db.parkinglot.update(
        where={"id": lot_id},
        data={
            "gate": data.get("gate", "CLOSED"),
            "mode": data.get("mode", "AUTO"),
            "lastSeenAt": utcnow(),
        },
    )

    for s in data.get("spots", []):
        number = int(s["id"])
        cm = s.get("cm")
        cm = cm if isinstance(cm, (int, float)) and cm >= 0 else None
        await db.parkingspot.upsert(
            where={"lotId_number": {"lotId": lot_id, "number": number}},
            data={
                "create": {"lotId": lot_id, "number": number,
                           "state": s.get("state", "FREE"), "lastDistanceCm": cm},
                "update": {"state": s.get("state", "FREE"), "lastDistanceCm": cm},
            },
        )

    # Snapshot para o grafico: so grava se mudou ou passou o intervalo minimo
    key = (data.get("free"), data.get("occupied"), data.get("gate"), data.get("mode"))
    prev = _last_snapshot.get(lot_id)
    now = utcnow()
    if prev is None or prev[:4] != key or (now - prev[4]).total_seconds() >= SNAPSHOT_MIN_GAP_S:
        await db.statussnapshot.create(data={
            "lotId": lot_id,
            "total": int(data.get("total", 0)),
            "free": int(data.get("free", 0)),
            "occupied": int(data.get("occupied", 0)),
            "gate": data.get("gate", "CLOSED"),
            "mode": data.get("mode", "AUTO"),
        })
        _last_snapshot[lot_id] = (*key, now)


async def handle_event(data: dict):
    lot_id = data.get("lotId") or LOT_ID
    await ensure_lot(lot_id)

    ts = data.get("ts") or 0
    device_ts = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
    spot = data.get("spotId")

    await db.parkingevent.create(data={
        "lotId": lot_id,
        "spotNumber": int(spot) if spot else None,
        "type": str(data.get("type", "UNKNOWN")),
        "detail": data.get("detail"),
        "deviceTs": device_ts,
    })
    await db.parkinglot.update(where={"id": lot_id}, data={"lastSeenAt": utcnow()})

    # Actualiza o lugar de imediato (o proximo status confirma)
    etype = data.get("type")
    if spot and etype in ("VEHICLE_ENTER", "VEHICLE_EXIT", "SENSOR_FAULT", "SENSOR_RECOVERED"):
        state = {"VEHICLE_ENTER": "OCCUPIED", "VEHICLE_EXIT": "FREE",
                 "SENSOR_FAULT": "FAULT", "SENSOR_RECOVERED": "FREE"}[etype]
        await db.parkingspot.upsert(
            where={"lotId_number": {"lotId": lot_id, "number": int(spot)}},
            data={"create": {"lotId": lot_id, "number": int(spot), "state": state},
                  "update": {"state": state}},
        )


async def dispatch(topic: str, payload: bytes):
    try:
        data = json.loads(payload.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("payload nao e um objecto JSON")
        if topic.endswith("/status"):
            await handle_status(data)
        elif topic.endswith("/event"):
            await handle_event(data)
    except Exception as exc:  # mensagem invalida nunca derruba o servico
        print(f"[MQTT] mensagem ignorada em {topic}: {exc!r}")


def start_mqtt(loop: asyncio.AbstractEventLoop):
    global mqtt_client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"parking-backend-{uuid.uuid4().hex[:8]}")

    def on_connect(c, userdata, flags, reason_code, properties):
        print(f"[MQTT] ligado a {MQTT_HOST}:{MQTT_PORT} ({reason_code})")
        c.subscribe([(f"{BASE}/status", 0), (f"{BASE}/event", 1)])

    def on_message(c, userdata, msg):
        asyncio.run_coroutine_threadsafe(dispatch(msg.topic, msg.payload), loop)

    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)   # reconexao automatica
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    mqtt_client = client


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await ensure_lot(LOT_ID, 4)
    start_mqtt(asyncio.get_running_loop())
    yield
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    await db.disconnect()


app = FastAPI(title="Smart Parking G3", lifespan=lifespan)


# --------------------------------------------------------------------------
# API REST
# --------------------------------------------------------------------------
@app.get("/api/overview")
async def overview(lotId: str = LOT_ID):
    lot = await db.parkinglot.find_unique(
        where={"id": lotId}, include={"spots": {"order_by": {"number": "asc"}}}
    )
    if not lot:
        raise HTTPException(404, "Parque nao encontrado")

    spots = lot.spots or []
    occupied = sum(1 for s in spots if s.state == "OCCUPIED")
    fault = sum(1 for s in spots if s.state == "FAULT")
    free = sum(1 for s in spots if s.state == "FREE")
    online = bool(lot.lastSeenAt and (utcnow() - lot.lastSeenAt).total_seconds() <= ONLINE_WINDOW_S)

    return {
        "lotId": lot.id,
        "name": lot.name,
        "total": lot.totalSpots,
        "occupied": occupied,
        "free": free,
        "fault": fault,
        "gate": lot.gate,
        "mode": lot.mode,
        "online": online,
        "lastSeenAt": iso(lot.lastSeenAt),
        "spots": [{"number": s.number, "state": s.state,
                   "distanceCm": s.lastDistanceCm, "updatedAt": iso(s.updatedAt)}
                  for s in spots],
    }


@app.get("/api/events")
async def events(lotId: str = LOT_ID, limit: int = Query(30, ge=1, le=200), type: str | None = None):
    where = {"lotId": lotId}
    if type:
        where["type"] = type
    rows = await db.parkingevent.find_many(where=where, order={"createdAt": "desc"}, take=limit)
    return [{"id": e.id, "type": e.type, "spotNumber": e.spotNumber, "detail": e.detail,
             "at": iso(e.deviceTs or e.createdAt), "receivedAt": iso(e.createdAt)} for e in rows]


@app.get("/api/history")
async def history(lotId: str = LOT_ID, hours: int = Query(6, ge=1, le=168)):
    since = utcnow() - timedelta(hours=hours)
    rows = await db.statussnapshot.find_many(
        where={"lotId": lotId, "createdAt": {"gte": since}}, order={"createdAt": "asc"}, take=2000)
    return [{"t": iso(r.createdAt), "occupied": r.occupied, "free": r.free, "total": r.total}
            for r in rows]


@app.get("/api/stats")
async def stats(lotId: str = LOT_ID, hours: int = Query(24, ge=1, le=168)):
    """Utilizacao: entradas por lugar/hora e permanencia media (pares ENTER/EXIT)."""
    since = utcnow() - timedelta(hours=hours)
    rows = await db.parkingevent.find_many(
        where={"lotId": lotId, "createdAt": {"gte": since},
               "type": {"in": ["VEHICLE_ENTER", "VEHICLE_EXIT"]}},
        order={"createdAt": "asc"}, take=5000)

    entries_per_spot: dict[int, int] = {}
    entries_per_hour: dict[int, int] = {}
    open_stay: dict[int, datetime] = {}
    stays: list[float] = []
    for e in rows:
        when = e.createdAt
        n = e.spotNumber or 0
        if e.type == "VEHICLE_ENTER":
            entries_per_spot[n] = entries_per_spot.get(n, 0) + 1
            entries_per_hour[when.hour] = entries_per_hour.get(when.hour, 0) + 1
            open_stay[n] = when
        elif n in open_stay:
            stays.append((when - open_stay.pop(n)).total_seconds() / 60)

    return {
        "hours": hours,
        "totalEntries": sum(entries_per_spot.values()),
        "entriesPerSpot": [{"spot": k, "entries": v} for k, v in sorted(entries_per_spot.items())],
        "entriesPerHourUtc": [{"hour": k, "entries": v} for k, v in sorted(entries_per_hour.items())],
        "avgStayMinutes": round(sum(stays) / len(stays), 1) if stays else None,
    }


class GateCommand(BaseModel):
    command: str   # AUTO | OPEN | CLOSE


@app.post("/api/gate")
async def gate_command(body: GateCommand, lotId: str = LOT_ID):
    cmd = body.command.upper()
    if cmd not in ("AUTO", "OPEN", "CLOSE"):
        raise HTTPException(400, "Comando invalido (AUTO, OPEN ou CLOSE)")
    if not mqtt_client or not mqtt_client.is_connected():
        raise HTTPException(503, "Sem ligacao ao broker MQTT")
    mqtt_client.publish(f"{BASE}/cmd", cmd, qos=1)
    await ensure_lot(lotId)
    await db.parkingevent.create(data={
        "lotId": lotId, "type": "COMMAND", "detail": f"dashboard -> {cmd}"})
    return {"sent": cmd}


# --------------------------------------------------------------------------
# Dashboard estatico
# --------------------------------------------------------------------------
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")