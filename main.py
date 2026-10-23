"""
main.py - Backend do Smart Parking (DSIo, Grupo 3)

Fluxo: ESP32 (Wokwi) -> MQTT -> este servidor -> Prisma/SQLite -> API REST -> dashboard

Mensagem esperada do ESP32:
  topico : <PREFIX>/parking/spot/<codigo>      (ex.: ujac-dsio-g3/parking/spot/A1)
  payload: {"distance_cm": 12.5, "occupied": true}   ("occupied" e opcional)

Correr com:  python -m uvicorn main:app --reload
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from prisma import Prisma

# ---------------------------------------------------------------- configuracao
BROKER = "broker.hivemq.com"      # broker publico (acessivel a partir do Wokwi)
PORT = 1883
PREFIX = "ujac-dsio-g3"           # MUDA para algo unico, o broker e publico!
TOPIC_SPOTS = f"{PREFIX}/parking/spot/+"
TOPIC_GATE = f"{PREFIX}/parking/gate"

SPOT_CODES = ["A1", "A2", "A3", "A4"]   # lugares da maquete
DIST_MIN, DIST_MAX = 2.0, 400.0         # intervalo valido do HC-SR04 (cm)
OCCUPIED_BELOW_CM = 15.0                # abaixo disto = lugar ocupado

db = Prisma()
loop = None  # loop asyncio principal (preenchido no arranque)
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)


# ------------------------------------------------------------------------ MQTT
def on_connect(client, userdata, flags, reason_code, properties):
    """Chamado ao (re)ligar ao broker: subscreve os lugares."""
    print(f"[MQTT] ligado ({reason_code})")
    client.subscribe(TOPIC_SPOTS)


def on_message(client, userdata, msg):
    """Corre na thread do paho; passa o trabalho para o loop asyncio."""
    code = msg.topic.split("/")[-1]
    asyncio.run_coroutine_threadsafe(
        process_message(code, msg.payload.decode(errors="ignore")), loop
    )


async def process_message(code: str, raw: str):
    """Valida a leitura, guarda historico e actualiza o estado do lugar."""
    # 1) payload malformado -> ignorar (tratamento de falhas)
    try:
        data = json.loads(raw)
        distance = float(data["distance_cm"])
    except (ValueError, KeyError, TypeError):
        print(f"[WARN] payload invalido em {code}: {raw!r}")
        return

    spot = await db.spot.find_unique(where={"code": code})
    if spot is None:
        print(f"[WARN] lugar desconhecido: {code}")
        return

    # 2) guardar sempre a leitura (mesmo invalida) no historico
    valid = DIST_MIN <= distance <= DIST_MAX
    await db.reading.create(
        data={
            "distanceCm": distance,
            "valid": valid,
            "spot": {"connect": {"id": spot.id}},
        }
    )
    if not valid:
        print(f"[WARN] leitura invalida em {code}: {distance} cm")
        return  # nao altera o estado do lugar

    # 3) estado: usa o que o ESP32 enviou, ou calcula pela distancia
    if "occupied" in data:
        occupied = bool(data["occupied"])
    else:
        occupied = distance < OCCUPIED_BELOW_CM
    if occupied == spot.occupied:
        return  # sem mudanca

    # 4) mudanca de estado -> actualiza lugar e regista evento
    await db.spot.update(where={"id": spot.id}, data={"occupied": occupied})
    await db.event.create(
        data={
            "type": "ENTRY" if occupied else "EXIT",
            "spot": {"connect": {"id": spot.id}},
        }
    )
    print(f"[EVENTO] {code} -> {'ENTRY' if occupied else 'EXIT'}")
    await apply_rules()


async def apply_rules():
    """Regra automatica: SE nao ha lugares livres ENTAO fecha cancela + alerta."""
    free = await db.spot.count(where={"occupied": False})
    if free == 0:
        await db.event.create(data={"type": "FULL_ALERT"})
        mqtt_client.publish(TOPIC_GATE, "CLOSE", retain=True)
        print("[REGRA] parque cheio -> cancela CLOSE")
    else:
        mqtt_client.publish(TOPIC_GATE, "OPEN", retain=True)


# ------------------------------------------------------------------------- API
@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_running_loop()
    await db.connect()
    # garante que os lugares existem na BD
    for code in SPOT_CODES:
        await db.spot.upsert(
            where={"code": code}, data={"create": {"code": code}, "update": {}}
        )
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect_async(BROKER, PORT)  # nao falha se nao houver rede
    mqtt_client.loop_start()                 # reconecta automaticamente
    yield
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    await db.disconnect()


app = FastAPI(title="Smart Parking API", lifespan=lifespan)


@app.get("/api/status")
async def status():
    """Total, ocupados, livres e estado de cada lugar."""
    spots = await db.spot.find_many(order={"code": "asc"})
    occupied = sum(1 for s in spots if s.occupied)
    return {
        "total": len(spots),
        "occupied": occupied,
        "free": len(spots) - occupied,
        "spots": spots,
    }


@app.get("/api/events")
async def events(limit: int = 50):
    """Historico de entradas, saidas e alertas (mais recentes primeiro)."""
    return await db.event.find_many(
        take=limit, order={"createdAt": "desc"}, include={"spot": True}
    )


@app.get("/api/readings")
async def readings(limit: int = 100):
    """Ultimas leituras brutas dos sensores."""
    return await db.reading.find_many(
        take=limit, order={"createdAt": "desc"}, include={"spot": True}
    )


# dashboard (pasta ./dashboard/index.html) servido na raiz
if os.path.isdir("dashboard"):
    app.mount("/", StaticFiles(directory="dashboard", html=True), name="dashboard")