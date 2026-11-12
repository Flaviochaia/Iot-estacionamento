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

class DriverCreate(BaseModel):
    fullName: str
    biNumber: str | None = None
    phone: str | None = None


class DriverStatusUpdate(BaseModel):
    status: str
    

class VehicleCreate(BaseModel):
    driverId: int
    plate: str
    brand: str | None = None
    model: str | None = None
    color: str | None = None

class CardCreate(BaseModel):
    driverId: int
    uid: str


class CardStatusUpdate(BaseModel):
    status: str    

class RFIDScan(BaseModel):
    uid: str
    plate: str

class VisitorRequestCreate(BaseModel):
    visitorName: str | None = None
    biNumber: str | None = None
    phone: str | None = None
    plate: str | None = None
    vehicleBrand: str | None = None
    vehicleModel: str | None = None
    notes: str | None = None


class VisitorDecision(BaseModel):
    decidedBy: str = "Recepcao"
    notes: str | None = None
    visitorName: str | None = None
    biNumber: str | None = None
    phone: str | None = None
    plate: str | None = None
    vehicleBrand: str | None = None
    vehicleModel: str | None = None
    notes: str | None = None

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
# VALIDACAO RFID
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# GESTAO DE VISITANTES
# --------------------------------------------------------------------------

@app.post("/api/visitors", status_code=201)
async def create_visitor_request(body: VisitorRequestCreate):

    await ensure_lot(LOT_ID, 4)

    request = await db.visitorrequest.create(
        data={
            "lotId": LOT_ID,
            "visitorName": body.visitorName.strip() if body.visitorName else None,
            "biNumber": body.biNumber.strip() if body.biNumber else None,
            "phone": body.phone.strip() if body.phone else None,
            "plate": body.plate.strip().upper() if body.plate else None,
            "vehicleBrand": body.vehicleBrand,
            "vehicleModel": body.vehicleModel,
            "notes": body.notes,
            "status": "PENDING"
        }
    )

    return {
        "message": "Pedido de visita criado com sucesso",
        "request": {
            "id": request.id,
            "visitorName": request.visitorName,
            "plate": request.plate,
            "status": request.status,
            "requestedAt": iso(request.requestedAt)
        }
    }


@app.get("/api/visitors")
async def list_visitor_requests(status: str | None = None):

    where = {"lotId": LOT_ID}

    if status:
        status = status.upper()

        if status not in (
            "PENDING", "APPROVED", "REJECTED",
            "ENTERED", "EXITED", "CANCELLED"
        ):
            raise HTTPException(400, "Estado de pedido invalido")

        where["status"] = status

    requests = await db.visitorrequest.find_many(
        where=where,
        order={"requestedAt": "desc"}
    )

    return [
        {
            "id": r.id,
            "visitorName": r.visitorName,
            "biNumber": r.biNumber,
            "phone": r.phone,
            "plate": r.plate,
            "vehicleBrand": r.vehicleBrand,
            "vehicleModel": r.vehicleModel,
            "status": r.status,
            "requestedAt": iso(r.requestedAt),
            "decidedAt": iso(r.decidedAt),
            "decidedBy": r.decidedBy,
            "notes": r.notes
        }
        for r in requests
    ]
@app.patch("/api/visitors/{request_id}/approve")
async def approve_visitor(
    request_id: int,
    body: VisitorDecision
):
    # 1. Procurar pedido
    request = await db.visitorrequest.find_unique(
        where={"id": request_id}
    )

    if not request:
        raise HTTPException(404, "Pedido de visita nao encontrado")

    if request.status != "PENDING":
        raise HTTPException(400, "Este pedido ja foi decidido")

    # 2. Validar dados obrigatorios
    if not request.visitorName or not request.plate:
        raise HTTPException(
            400,
            "O nome do visitante e a matricula sao obrigatorios"
        )

    # 3. Verificar ligacao MQTT
    if not mqtt_client or not mqtt_client.is_connected():
        raise HTTPException(503, "Sem ligacao ao ESP32/MQTT")

    # 4. Verificar vagas livres
    free_spots = await db.parkingspot.find_many(
        where={
            "lotId": request.lotId,
            "state": "FREE"
        },
        take=1
    )

    if not free_spots:
        raise HTTPException(
            403,
            "Parque cheio ou sem vagas disponiveis"
        )

    # 5. Procurar visitante pelo BI, se existir
    driver = None

    if request.biNumber:
        driver = await db.driver.find_unique(
            where={"biNumber": request.biNumber}
        )

        if driver and driver.role != "VISITOR":
            raise HTTPException(
                409,
                "Este BI ja esta associado a um proprietario"
            )

    # 6. Criar visitante se ainda nao estiver registado
    if not driver:
        driver = await db.driver.create(
            data={
                "fullName": request.visitorName.strip(),
                "biNumber": request.biNumber,
                "phone": request.phone,
                "role": "VISITOR",
                "status": "ACTIVE"
            }
        )

    if driver.status != "ACTIVE":
        raise HTTPException(403, "Visitante bloqueado")

    # 7. Verificar matricula
    plate = request.plate.strip().upper()

    vehicle = await db.vehicle.find_unique(
        where={"plate": plate}
    )

    if vehicle:
        if vehicle.driverId != driver.id:
            raise HTTPException(
                409,
                "Esta matricula ja pertence a outro condutor"
            )
    else:
        vehicle = await db.vehicle.create(
            data={
                "driverId": driver.id,
                "plate": plate,
                "brand": request.vehicleBrand,
                "model": request.vehicleModel
            }
        )

    # 8. Atualizar pedido
    updated = await db.visitorrequest.update(
        where={"id": request_id},
        data={
            "driverId": driver.id,
            "status": "APPROVED",
            "decidedAt": utcnow(),
            "decidedBy": body.decidedBy,
            "notes": body.notes or request.notes
        }
    )

    # 9. Registar evento
    await db.parkingevent.create(
        data={
            "lotId": request.lotId,
            "type": "VISITOR_APPROVED",
            "detail": f"Visita aprovada: {plate}",
            "driverId": driver.id,
            "vehicleId": vehicle.id,
            "accessMethod": "VISITOR_APPROVAL"
        }
    )

    # 10. Abrir cancela
    mqtt_client.publish(
        f"{BASE}/cmd",
        "OPEN",
        qos=1
    )

    return {
        "authorized": True,
        "message": "Visita aprovada com sucesso",
        "requestId": updated.id,
        "visitor": driver.fullName,
        "plate": vehicle.plate,
        "status": updated.status,
        "gate": "OPEN"
    }
@app.patch("/api/visitors/{request_id}/reject")
async def reject_visitor(
    request_id: int,
    body: VisitorDecision
):
    request = await db.visitorrequest.find_unique(
        where={"id": request_id}
    )

    if not request:
        raise HTTPException(404, "Pedido de visita nao encontrado")

    if request.status != "PENDING":
        raise HTTPException(400, "Este pedido ja foi decidido")

    updated = await db.visitorrequest.update(
        where={"id": request_id},
        data={
            "status": "REJECTED",
            "decidedAt": utcnow(),
            "decidedBy": body.decidedBy,
            "notes": body.notes or request.notes
        }
    )

    await db.parkingevent.create(
        data={
            "lotId": request.lotId,
            "type": "VISITOR_REJECTED",
            "detail": f"Visita rejeitada: {request.plate or request.visitorName}",
            "accessMethod": "VISITOR_APPROVAL"
        }
    )

    return {
        "message": "Pedido de visita rejeitado",
        "requestId": updated.id,
        "status": updated.status
    }
# --------------------------------------------------------------------------
# GESTAO DE PROPRIETARIOS
# --------------------------------------------------------------------------

@app.post("/api/drivers", status_code=201)
async def create_driver(body: DriverCreate):

    name = body.fullName.strip()

    if not name:
        raise HTTPException(400, "O nome e obrigatorio")

    bi = body.biNumber.strip() if body.biNumber else None
    phone = body.phone.strip() if body.phone else None

    # Verificar BI duplicado
    if bi:
        existing = await db.driver.find_unique(
            where={"biNumber": bi}
        )

        if existing:
            raise HTTPException(409, "Este BI ja esta registado")

    driver = await db.driver.create(
        data={
            "fullName": name,
            "biNumber": bi,
            "phone": phone,
            "role": "OWNER",
            "status": "ACTIVE"
        }
    )

    return {
        "message": "Proprietario registado com sucesso",
        "driver": {
            "id": driver.id,
            "fullName": driver.fullName,
            "biNumber": driver.biNumber,
            "phone": driver.phone,
            "role": driver.role,
            "status": driver.status
        }
    }


@app.get("/api/drivers")
async def list_drivers():

    drivers = await db.driver.find_many(
        where={"role": "OWNER"},
        order={"fullName": "asc"}
    )

    return [
        {
            "id": d.id,
            "fullName": d.fullName,
            "biNumber": d.biNumber,
            "phone": d.phone,
            "role": d.role,
            "status": d.status,
            "createdAt": iso(d.createdAt)
        }
        for d in drivers
    ]


@app.patch("/api/drivers/{driver_id}/status")
async def update_driver_status(
    driver_id: int,
    body: DriverStatusUpdate
):

    status = body.status.upper()

    if status not in ("ACTIVE", "BLOCKED"):
        raise HTTPException(
            400,
            "Estado invalido. Utilize ACTIVE ou BLOCKED"
        )

    driver = await db.driver.find_unique(
        where={"id": driver_id}
    )

    if not driver:
        raise HTTPException(404, "Proprietario nao encontrado")

    updated = await db.driver.update(
        where={"id": driver_id},
        data={"status": status}
    )

    return {
        "message": "Estado atualizado com sucesso",
        "id": updated.id,
        "status": updated.status
    }
    
@app.post("/api/vehicles", status_code=201)
async def create_vehicle(body: VehicleCreate):

    plate = body.plate.strip().upper()

    if not plate:
        raise HTTPException(400, "A matricula e obrigatoria")

    driver = await db.driver.find_unique(
        where={"id": body.driverId}
    )

    if not driver:
        raise HTTPException(404, "Proprietario nao encontrado")

    if driver.role != "OWNER":
        raise HTTPException(400, "O condutor nao e proprietario")

    if driver.status != "ACTIVE":
        raise HTTPException(400, "Proprietario bloqueado")

    existing = await db.vehicle.find_unique(
        where={"plate": plate}
    )

    if existing:
        raise HTTPException(409, "Esta matricula ja esta registada")

    vehicle = await db.vehicle.create(
        data={
            "driverId": body.driverId,
            "plate": plate,
            "brand": body.brand,
            "model": body.model,
            "color": body.color
        }
    )

    return {
        "message": "Veiculo registado com sucesso",
        "vehicle": {
            "id": vehicle.id,
            "driverId": vehicle.driverId,
            "plate": vehicle.plate,
            "brand": vehicle.brand,
            "model": vehicle.model,
            "color": vehicle.color
        }
    }
#consulta de veiculos 
@app.get("/api/vehicles")
async def list_vehicles():
    vehicles = await db.vehicle.find_many(
        include={"driver": True},
        order={"plate": "asc"}
    )

    return [
        {
            "id": v.id,
            "plate": v.plate,
            "brand": v.brand,
            "model": v.model,
            "color": v.color,
            "driver": {
                "id": v.driver.id,
                "fullName": v.driver.fullName,
                "biNumber": v.driver.biNumber
            }
        }
        for v in vehicles
    ]        

@app.post("/api/cards", status_code=201)
async def create_card(body: CardCreate):

    uid = body.uid.strip().upper()

    if not uid:
        raise HTTPException(400, "UID do cartao obrigatorio")

    driver = await db.driver.find_unique(
        where={"id": body.driverId}
    )

    if not driver:
        raise HTTPException(404, "Proprietario nao encontrado")

    if driver.role != "OWNER":
        raise HTTPException(400, "O condutor nao e proprietario")

    if driver.status != "ACTIVE":
        raise HTTPException(400, "Proprietario bloqueado")

    existing = await db.accesscard.find_unique(
        where={"uid": uid}
    )

    if existing:
        raise HTTPException(409, "Este cartao ja esta registado")

    card = await db.accesscard.create(
        data={
            "driverId": body.driverId,
            "uid": uid,
            "status": "ACTIVE"
        }
    )

    return {
        "message": "Cartao RFID registado com sucesso",
        "card": {
            "id": card.id,
            "driverId": card.driverId,
            "uid": card.uid,
            "status": card.status
        }
    }


@app.get("/api/cards")
async def list_cards():

    cards = await db.accesscard.find_many(
        include={"driver": True},
        order={"createdAt": "desc"}
    )

    return [
        {
            "id": c.id,
            "uid": c.uid,
            "status": c.status,
            "createdAt": iso(c.createdAt),
            "driver": {
                "id": c.driver.id,
                "fullName": c.driver.fullName,
                "biNumber": c.driver.biNumber,
                "phone": c.driver.phone,
                "role": c.driver.role
            }
        }
        for c in cards
    ]

@app.patch("/api/cards/{card_id}/status")
async def update_card_status(
    card_id: int,
    body: CardStatusUpdate
):

    status = body.status.upper()

    if status not in ("ACTIVE", "BLOCKED"):
        raise HTTPException(
            400,
            "Estado invalido. Utilize ACTIVE ou BLOCKED"
        )

    card = await db.accesscard.find_unique(
        where={"id": card_id}
    )

    if not card:
        raise HTTPException(404, "Cartao nao encontrado")

    updated = await db.accesscard.update(
        where={"id": card_id},
        data={"status": status}
    )

    return {
        "message": "Estado do cartao atualizado",
        "id": updated.id,
        "status": updated.status
    }


# --------------------------------------------------------------------------
# VALIDACAO RFID
# --------------------------------------------------------------------------

@app.post("/api/rfid/scan")
async def rfid_scan(body: RFIDScan):

    uid = body.uid.strip().upper().replace(":", "").replace(" ", "")
    plate = body.plate.strip().upper()

    if not uid or not plate:
        raise HTTPException(
            400,
            "UID e matricula sao obrigatorios"
        )

    # 1. Procurar o cartao
    card = await db.accesscard.find_unique(
        where={"uid": uid},
        include={"driver": True}
    )

    if not card:
        raise HTTPException(403, "Cartao RFID desconhecido")

    # 2. Verificar estado do cartao
    if card.status != "ACTIVE":
        raise HTTPException(403, "Cartao RFID bloqueado")

    driver = card.driver

    # 3. Verificar proprietario
    if driver.status != "ACTIVE":
        raise HTTPException(403, "Proprietario bloqueado")

    if driver.role != "OWNER":
        raise HTTPException(403, "Acesso nao autorizado")

    # 4. Procurar veiculo
    vehicle = await db.vehicle.find_unique(
        where={"plate": plate}
    )

    if not vehicle:
        raise HTTPException(404, "Veiculo nao registado")

    if vehicle.driverId != driver.id:
        raise HTTPException(
            403,
            "Este veiculo nao pertence ao proprietario do cartao"
        )

    # 5. Verificar ligacao MQTT
    if not mqtt_client or not mqtt_client.is_connected():
        raise HTTPException(
            503,
            "Sem ligacao ao ESP32/MQTT"
        )

    # 6. Verificar se o veiculo ja esta dentro
    sessions = await db.parkingsession.find_many(
        where={
            "lotId": LOT_ID,
            "driverId": driver.id,
            "vehicleId": vehicle.id,
            "status": "ACTIVE"
        },
        take=1
    )

    # ------------------------------------------------
    # SAIDA DO VEICULO
    # ------------------------------------------------

    if sessions:

        session = sessions[0]

        await db.parkingsession.update(
            where={"id": session.id},
            data={
                "exitAt": utcnow(),
                "status": "CLOSED"
            }
        )

        mqtt_client.publish(
            f"{BASE}/cmd",
            "OPEN",
            qos=1
        )

        await db.parkingevent.create(
            data={
                "lotId": LOT_ID,
                "type": "RFID_EXIT",
                "detail": f"Saida autorizada: {plate}",
                "driverId": driver.id,
                "vehicleId": vehicle.id,
                "accessMethod": "RFID"
            }
        )

        return {
            "authorized": True,
            "action": "EXIT",
            "message": "Saida autorizada",
            "driver": driver.fullName,
            "plate": vehicle.plate,
            "gate": "OPEN"
        }

    # ------------------------------------------------
    # ENTRADA DO VEICULO
    # ------------------------------------------------

    # Verificar vagas livres
    free_spots = await db.parkingspot.find_many(
        where={
            "lotId": LOT_ID,
            "state": "FREE"
        },
        take=1
    )

    if not free_spots:
        raise HTTPException(
            403,
            "Parque cheio ou estado das vagas indisponivel"
        )

    # Criar sessao de estacionamento
    session = await db.parkingsession.create(
        data={
            "lotId": LOT_ID,
            "driverId": driver.id,
            "vehicleId": vehicle.id,
            "accessMethod": "RFID",
            "entryAt": utcnow(),
            "status": "ACTIVE"
        }
    )

    # Abrir cancela
    mqtt_client.publish(
        f"{BASE}/cmd",
        "OPEN",
        qos=1
    )

    await db.parkingevent.create(
        data={
            "lotId": LOT_ID,
            "type": "RFID_ENTRY",
            "detail": f"Entrada autorizada: {plate}",
            "driverId": driver.id,
            "vehicleId": vehicle.id,
            "accessMethod": "RFID"
        }
    )

    return {
        "authorized": True,
        "action": "ENTRY",
        "message": "Entrada autorizada",
        "driver": driver.fullName,
        "plate": vehicle.plate,
        "gate": "OPEN"
    }
# --------------------------------------------------------------------------
# Dashboard estatico
# --------------------------------------------------------------------------
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")




app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
