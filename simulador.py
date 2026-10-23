"""
simulador.py - Finge ser o ESP32 para testar o backend antes do Wokwi.
Publica leituras aleatorias dos lugares A1..A4 a cada 2 segundos.
Correr com:  python simulador.py   (Ctrl+C para parar)
"""
import json
import random
import time

import paho.mqtt.client as mqtt

BROKER, PORT = "broker.hivemq.com", 1883
PREFIX = "ujac-dsio-g3"  # tem de ser igual ao do main.py
SPOTS = ["A1", "A2", "A3", "A4"]

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT)
client.loop_start()

# estado inicial: todos livres (distancia grande)
state = {code: False for code in SPOTS}

try:
    while True:
        code = random.choice(SPOTS)
        roll = random.random()
        if roll < 0.1:
            distance = 999.0                 # leitura invalida (testa o tratamento)
        else:
            if roll < 0.4:
                state[code] = not state[code]  # carro entra/sai
            distance = random.uniform(5, 12) if state[code] else random.uniform(60, 150)
        payload = {"distance_cm": round(distance, 1), "occupied": state[code]}
        client.publish(f"{PREFIX}/parking/spot/{code}", json.dumps(payload))
        print(code, payload)
        time.sleep(2)
except KeyboardInterrupt:
    client.loop_stop()