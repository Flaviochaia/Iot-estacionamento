"""
Simulador do ESP32 para testar o backend/dashboard sem o Wokwi.
Publica 'status' e 'event' no mesmo formato do sketch.ino.

Uso:  python scripts/simulate.py
"""
import json
import os
import random
import time

import paho.mqtt.client as mqtt

HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
BASE = os.getenv("MQTT_BASE", "ujac/dsio/g3/parking")
LOT = os.getenv("LOT_ID", "G3-LOT-01")
N = 4

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{random.randint(1000, 9999)}")
client.connect(HOST, 1883, 30)
client.loop_start()

spots = ["FREE"] * N
gate = "OPEN"


def event(etype, spot=None, detail=""):
    msg = {"lotId": LOT, "type": etype, "detail": detail, "ts": int(time.time())}
    if spot:
        msg["spotId"] = spot
    client.publish(f"{BASE}/event", json.dumps(msg), qos=1)
    print("EVENT", msg)


def status():
    occ = spots.count("OCCUPIED")
    msg = {"lotId": LOT, "total": N, "free": spots.count("FREE"), "occupied": occ,
           "gate": gate, "mode": "AUTO", "ts": int(time.time()),
           "spots": [{"id": i + 1, "state": s,
                      "cm": random.uniform(5, 12) if s == "OCCUPIED" else random.uniform(40, 150)}
                     for i, s in enumerate(spots)]}
    client.publish(f"{BASE}/status", json.dumps(msg))
    print("STATUS free=", msg["free"], "occupied=", occ)


event("ONLINE", detail="simulador ligado")
status()
try:
    while True:
        time.sleep(4)
        i = random.randrange(N)
        if spots[i] == "FREE":
            spots[i] = "OCCUPIED"
            event("VEHICLE_ENTER", i + 1, "lugar ocupado")
        else:
            spots[i] = "FREE"
            event("VEHICLE_EXIT", i + 1, "lugar libertado")
        new_gate = "CLOSED" if spots.count("FREE") == 0 else "OPEN"
        if new_gate != gate:
            gate = new_gate
            event("GATE_OPEN" if gate == "OPEN" else "GATE_CLOSE", detail="regra automatica")
            if gate == "CLOSED":
                event("LOT_FULL", detail="sem lugares livres")
        status()
except KeyboardInterrupt:
    client.loop_stop()