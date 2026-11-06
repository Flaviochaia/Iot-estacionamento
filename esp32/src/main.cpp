#include <Arduino.h>

/*
 * DSIo 2026 - Grupo 3: Estacionamento Inteligente (Smart Parking)
 * ESP32 + 4x HC-SR04 + LEDs + LCD I2C + Servo (cancela) + Buzzer
 *
 * Fluxo: Sensores -> regras -> actuadores (LEDs/LCD/cancela/buzzer)
 *        -> Wi-Fi -> MQTT -> Backend -> BD -> Dashboard
 *
 * Regras automáticas:
 *   R1: SE lugar ocupado ENTÃO LED vermelho ligado, verde desligado.
 *   R2: SE lugares livres == 0 ENTÃO cancela fecha e buzzer apita.
 *   R3: SE lugares livres > 0 (modo AUTO) ENTÃO cancela abre.
 *
 * Tratamento de falhas:
 *   - Leitura inválida (timeout do sensor): ignorada; após 5 falhas seguidas
 *     o lugar passa a estado FALHA (LEDs a piscar, reportado ao servidor).
 *   - Filtro: mediana de 3 leituras + histerese + confirmação de 3 ciclos.
 *   - Perda de Wi-Fi/MQTT: reconexão não bloqueante; eventos ficam numa fila
 *     (até 20) e são enviados quando a ligação volta.
 *
 * Tópicos MQTT:
 *   ujac/dsio/g3/parking/status  (publica, a cada 10 s)
 *   ujac/dsio/g3/parking/event   (publica, a cada mudança)
 *   ujac/dsio/g3/parking/cmd     (subscreve: AUTO | OPEN | CLOSE)
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ESP32Servo.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <time.h>

// ---------------- Configuração ----------------
const char* WIFI_SSID = "Wokwi-GUEST";   // rede virtual do Wokwi
const char* WIFI_PASS = "";
const char* MQTT_HOST = "broker.hivemq.com";
const uint16_t MQTT_PORT = 1883;
const char* LOT_ID = "G3-LOT-01";

const char* T_STATUS = "ujac/dsio/g3/parking/status";
const char* T_EVENT  = "ujac/dsio/g3/parking/event";
const char* T_CMD    = "ujac/dsio/g3/parking/cmd";

const int NUM_SPOTS = 4;
const int TRIG_PIN[NUM_SPOTS]  = {13, 14, 27, 26};
const int ECHO_PIN[NUM_SPOTS]  = {34, 35, 32, 33};
const int GREEN_PIN[NUM_SPOTS] = {4, 16, 17, 5};
const int RED_PIN[NUM_SPOTS]   = {18, 19, 23, 25};
const int SERVO_PIN  = 15;
const int BUZZER_PIN = 2;

const float OCCUPIED_BELOW_CM = 15.0;  // < 15 cm  => ocupado
const float FREE_ABOVE_CM     = 20.0;  // > 20 cm  => livre (histerese)
const uint8_t CONFIRM_CYCLES  = 3;
const uint8_t FAULT_AFTER     = 5;

const unsigned long READ_EVERY_MS   = 500;
const unsigned long STATUS_EVERY_MS = 10000;
const unsigned long RECONNECT_MS    = 5000;

// ---------------- Estado ----------------
struct Spot {
  bool occupied = false;
  bool fault = false;
  uint8_t confirm = 0;      // ciclos consecutivos a favor da mudança
  uint8_t badReads = 0;     // leituras inválidas consecutivas
  float lastCm = -1;
};
Spot spots[NUM_SPOTS];

enum GateMode { GATE_AUTO, GATE_OPEN, GATE_CLOSED };
GateMode gateMode = GATE_AUTO;
bool gateOpen = false;

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
Servo gate;
LiquidCrystal_I2C lcd(0x27, 16, 2);

const int QUEUE_SIZE = 20;
String queueBuf[QUEUE_SIZE];
int qHead = 0, qCount = 0;

unsigned long tRead = 0, tStatus = 0, tReconnect = 0;
bool lastFull = false;

// ---------------- Utilitários ----------------
long nowEpoch() {
  time_t t = time(nullptr);
  return (t > 1700000000) ? (long)t : 0;  // 0 = hora ainda não sincronizada
}

// Guarda mensagem na fila circular (descarta a mais antiga se cheia)
void enqueue(const String& payload) {
  if (qCount == QUEUE_SIZE) {
    qHead = (qHead + 1) % QUEUE_SIZE;
    qCount--;
  }
  queueBuf[(qHead + qCount) % QUEUE_SIZE] = payload;
  qCount++;
}

// Envia o que estiver pendente quando há ligação
void flushQueue() {
  while (qCount > 0 && mqtt.connected()) {
    if (!mqtt.publish(T_EVENT, queueBuf[qHead].c_str())) break;
    qHead = (qHead + 1) % QUEUE_SIZE;
    qCount--;
  }
}

void publishEvent(const char* type, int spotIdx, const char* detail) {
  StaticJsonDocument<256> doc;
  doc["lotId"] = LOT_ID;
  doc["type"] = type;
  if (spotIdx >= 0) doc["spotId"] = spotIdx + 1;
  doc["detail"] = detail;
  doc["ts"] = nowEpoch();
  doc["uptimeMs"] = millis();
  String out;
  serializeJson(doc, out);
  Serial.println("[EVENT] " + out);
  enqueue(out);
  flushQueue();
}

int freeCount() {
  int f = 0;
  for (int i = 0; i < NUM_SPOTS; i++)
    if (!spots[i].occupied && !spots[i].fault) f++;
  return f;
}
int occupiedCount() {
  int o = 0;
  for (int i = 0; i < NUM_SPOTS; i++)
    if (spots[i].occupied) o++;
  return o;
}

// ---------------- Sensores ----------------
float readOnceCm(int i) {
  digitalWrite(TRIG_PIN[i], LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN[i], HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN[i], LOW);
  unsigned long dur = pulseIn(ECHO_PIN[i], HIGH, 30000UL);  // timeout 30 ms
  if (dur == 0) return -1;                                  // sem eco = inválido
  float cm = dur / 58.0;
  if (cm < 2.0 || cm > 400.0) return -1;                    // fora do alcance
  return cm;
}

// Mediana de 3 leituras válidas; -1 se todas inválidas
float readDistanceCm(int i) {
  float v[3];
  int n = 0;
  for (int k = 0; k < 3; k++) {
    float d = readOnceCm(i);
    if (d > 0) v[n++] = d;
    delay(5);
  }
  if (n == 0) return -1;
  for (int a = 0; a < n - 1; a++)
    for (int b = a + 1; b < n; b++)
      if (v[b] < v[a]) { float t = v[a]; v[a] = v[b]; v[b] = t; }
  return v[n / 2];
}

void updateSpot(int i) {
  Spot& s = spots[i];
  float d = readDistanceCm(i);

  if (d < 0) {                                  // leitura inválida
    if (s.badReads < 255) s.badReads++;
    if (s.badReads >= FAULT_AFTER && !s.fault) {
      s.fault = true;
      publishEvent("SENSOR_FAULT", i, "5 leituras invalidas seguidas");
    }
    return;
  }

  if (s.fault) {                                // recuperou
    s.fault = false;
    publishEvent("SENSOR_RECOVERED", i, "leitura valida");
  }
  s.badReads = 0;
  s.lastCm = d;

  bool wantOccupied = s.occupied;
  if (d < OCCUPIED_BELOW_CM) wantOccupied = true;
  else if (d > FREE_ABOVE_CM) wantOccupied = false;

  if (wantOccupied != s.occupied) {
    if (++s.confirm >= CONFIRM_CYCLES) {        // confirma antes de mudar
      s.occupied = wantOccupied;
      s.confirm = 0;
      publishEvent(s.occupied ? "VEHICLE_ENTER" : "VEHICLE_EXIT", i,
                   s.occupied ? "lugar ocupado" : "lugar libertado");
    }
  } else {
    s.confirm = 0;
  }
}

// ---------------- Actuadores ----------------
void updateLeds() {
  bool blink = (millis() / 400) % 2;
  for (int i = 0; i < NUM_SPOTS; i++) {
    if (spots[i].fault) {                       // falha: ambos a piscar
      digitalWrite(GREEN_PIN[i], blink);
      digitalWrite(RED_PIN[i], blink);
    } else {                                    // R1
      digitalWrite(GREEN_PIN[i], !spots[i].occupied);
      digitalWrite(RED_PIN[i], spots[i].occupied);
    }
  }
}

void setGate(bool open, const char* reason) {
  if (open == gateOpen) return;
  gateOpen = open;
  gate.write(open ? 90 : 0);
  publishEvent(open ? "GATE_OPEN" : "GATE_CLOSE", -1, reason);
}

void applyGateRules() {
  int f = freeCount();
  bool full = (f == 0);

  if (gateMode == GATE_AUTO) {
    if (full) setGate(false, "R2: parque lotado");       // R2
    else      setGate(true, "R3: ha lugares livres");    // R3
  } else if (gateMode == GATE_OPEN) {
    setGate(true, "comando manual");
  } else {
    setGate(false, "comando manual");
  }

  if (full && !lastFull) {                               // buzzer ao lotar
    tone(BUZZER_PIN, 1000, 300);
    publishEvent("LOT_FULL", -1, "sem lugares livres");
  } else if (!full && lastFull) {
    publishEvent("LOT_AVAILABLE", -1, "lugar libertado");
  }
  lastFull = full;
}

void updateLcd() {
  char l1[17], l2[17];
  snprintf(l1, sizeof(l1), "Livres:%d Ocup:%d ", freeCount(), occupiedCount());
  String s = "";
  for (int i = 0; i < NUM_SPOTS; i++) {
    s += String(i + 1) + ":" + (spots[i].fault ? "F" : (spots[i].occupied ? "O" : "L")) + " ";
  }
  snprintf(l2, sizeof(l2), "%-15s%c", s.c_str(), (WiFi.status() == WL_CONNECTED && mqtt.connected()) ? 'W' : '!');
  lcd.setCursor(0, 0); lcd.print(l1);
  lcd.setCursor(0, 1); lcd.print(l2);
}

// ---------------- Rede ----------------
void onMessage(char* topic, byte* payload, unsigned int len) {
  String msg;
  for (unsigned int i = 0; i < len; i++) msg += (char)payload[i];
  msg.trim(); msg.toUpperCase();
  Serial.println("[CMD] " + msg);
  if (msg == "AUTO") gateMode = GATE_AUTO;
  else if (msg == "OPEN") gateMode = GATE_OPEN;
  else if (msg == "CLOSE") gateMode = GATE_CLOSED;
  applyGateRules();
}

void networkLoop() {
  if (WiFi.status() != WL_CONNECTED) {
    if (millis() - tReconnect > RECONNECT_MS) {
      tReconnect = millis();
      Serial.println("[WIFI] a reconectar...");
      WiFi.disconnect();
      WiFi.begin(WIFI_SSID, WIFI_PASS);
    }
    return;
  }
  if (!mqtt.connected()) {
    if (millis() - tReconnect > RECONNECT_MS) {
      tReconnect = millis();
      String cid = "esp32-g3-" + String((uint32_t)ESP.getEfuseMac(), HEX);
      Serial.println("[MQTT] a ligar...");
      if (mqtt.connect(cid.c_str())) {
        mqtt.subscribe(T_CMD);
        publishEvent("ONLINE", -1, "dispositivo ligado");
        flushQueue();
      }
    }
    return;
  }
  mqtt.loop();
}

void publishStatus() {
  StaticJsonDocument<512> doc;
  doc["lotId"] = LOT_ID;
  doc["total"] = NUM_SPOTS;
  doc["free"] = freeCount();
  doc["occupied"] = occupiedCount();
  doc["gate"] = gateOpen ? "OPEN" : "CLOSED";
  doc["mode"] = gateMode == GATE_AUTO ? "AUTO" : "MANUAL";
  doc["ts"] = nowEpoch();
  JsonArray arr = doc.createNestedArray("spots");
  for (int i = 0; i < NUM_SPOTS; i++) {
    JsonObject o = arr.createNestedObject();
    o["id"] = i + 1;
    o["state"] = spots[i].fault ? "FAULT" : (spots[i].occupied ? "OCCUPIED" : "FREE");
    o["cm"] = spots[i].lastCm;
  }
  String out;
  serializeJson(doc, out);
  Serial.println("[STATUS] " + out);
  if (mqtt.connected()) mqtt.publish(T_STATUS, out.c_str());
}

// ---------------- Setup / Loop ----------------
void setup() {
  Serial.begin(115200);
  for (int i = 0; i < NUM_SPOTS; i++) {
    pinMode(TRIG_PIN[i], OUTPUT);
    pinMode(ECHO_PIN[i], INPUT);
    pinMode(GREEN_PIN[i], OUTPUT);
    pinMode(RED_PIN[i], OUTPUT);
  }
  pinMode(BUZZER_PIN, OUTPUT);

  gate.attach(SERVO_PIN);
  gate.write(0);

  Wire.begin(21, 22);
  lcd.init();
  lcd.backlight();
  lcd.setCursor(0, 0);
  lcd.print("Smart Parking G3");
  lcd.setCursor(0, 1);
  lcd.print("A iniciar...");

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  configTime(0, 0, "pool.ntp.org");   // UTC; o dashboard converte para Africa/Maputo
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMessage);
  mqtt.setBufferSize(768);
  delay(800);
}

void loop() {
  networkLoop();

  if (millis() - tRead >= READ_EVERY_MS) {
    tRead = millis();
    for (int i = 0; i < NUM_SPOTS; i++) updateSpot(i);
    applyGateRules();
    updateLcd();
  }
  updateLeds();

  if (millis() - tStatus >= STATUS_EVERY_MS) {
    tStatus = millis();
    publishStatus();
  }
  if (mqtt.connected() && qCount > 0) flushQueue();
}