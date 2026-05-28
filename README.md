# mqtt-lab
MQTT C-lab

Prompt:
Das ist mein Code /*
 * test_publisher.c
 * Connects to the public test broker and publishes 5 simulated sensor readings.
 * Usage: ./test_publisher
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <mosquitto.h>

#define BROKER      "localhost"
#define PORT        1883
#define TOPIC       "mqtt-lab/test/sensor"
#define MSG_COUNT   5
#define INTERVAL_S  2

struct SensorData {
    char Zeitstempel[20];
    char Stationsname[3];
    float Temperatur;
    float Luftfeuchtigkeit;
    char Sequenznummer[100];
};
void serialize_to_json(struct SensorData *data, char *buf, size_t len) {
    /* Temperatur prüfen */
    int temp_valid = 1;

    if (data->Temperatur < -273.15f || data->Temperatur > 100.0f) {
        fprintf(stderr,
                "[Fehler] Ungültige Temperatur: %.2f°C -> wird ignoriert!\n",
                data->Temperatur);

        temp_valid = 0;
    }

    if (temp_valid) {
        snprintf(buf, len,
            "{\"Sequenznummer\":\"%s\","
            "\"Stationsname\":\"%s\","
            "\"Zeitstempel\":\"%s\","
            "\"Temperatur\":%.1f,"
            "\"Luftfeuchtigkeit\":%.1f}",
            data->Sequenznummer,
            data->Stationsname,
            data->Zeitstempel,
            data->Temperatur,
            data->Luftfeuchtigkeit);
    } else {
        snprintf(buf, len,
            "{\"Sequenznummer\":\"%s\","
            "\"Stationsname\":\"%s\","
            "\"Zeitstempel\":\"%s\","
            "\"Temperatur\":null,"
            "\"Luftfeuchtigkeit\":%.1f}",
            data->Sequenznummer,
            data->Stationsname,
            data->Zeitstempel,
            data->Luftfeuchtigkeit);
    }
}

/* Generate a simple JSON payload with dummy sensor data */
static void build_payload(char *buf, size_t len, int seq) {
    time_t now = time(NULL);
    struct tm *t = gmtime(&now);
    char ts[30];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", t);

    /* Simple simulated values */
    float temp     = 18.0f + (float)(rand() % 100) / 20.0f;   /* 18.0 – 23.0 */
    float humidity = 50.0f + (float)(rand() % 300) / 10.0f;   /* 50.0 – 80.0 */

    snprintf(buf, len,
        "{\"seq\":%d,\"station_id\":\"S1\","
        "\"timestamp\":\"%s\","
        "\"temperature_c\":%.1f,"
        "\"humidity_pct\":%.1f}",
        seq, ts, temp, humidity);
}

/* Callback: called when connection is established */
static void on_connect(struct mosquitto *mosq, void *userdata, int rc) {
    if (rc == 0) {
        printf("[publisher] Connected to %s:%d\n", BROKER, PORT);
    } else {
        fprintf(stderr, "[publisher] Connection failed: %s\n",
                mosquitto_connack_string(rc));
        mosquitto_disconnect(mosq);
    }
}

/* Callback: called after each message is published */
static void on_publish(struct mosquitto *mosq, void *userdata, int mid) {
    printf("[publisher] Message %d delivered to broker\n", mid);
}

int main(void) {
    struct SensorData data;

strcpy(data.Sequenznummer, "1");
strcpy(data.Stationsname, "S1");
strcpy(data.Zeitstempel, "2026-05-24T12:00:00Z");

data.Temperatur = 250.0f;   // absichtlich ungültig
data.Luftfeuchtigkeit = 55.5f;

char payload[256];

serialize_to_json(&data, payload, sizeof(payload));

printf("JSON: %s\n", payload);
    srand((unsigned int)time(NULL));

    mosquitto_lib_init();

    struct mosquitto *mosq = mosquitto_new("mqtt-lab-publisher", true, NULL);
    if (!mosq) {
        fprintf(stderr, "[publisher] Failed to create mosquitto instance\n");
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_publish_callback_set(mosq, on_publish);

    int rc = mosquitto_connect(mosq, BROKER, PORT, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[publisher] Could not connect: %s\n",
                mosquitto_strerror(rc));
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
        return 1;
    }

    /* Start the network loop in background thread */
    mosquitto_loop_start(mosq);

    
    printf("[publisher] Sending %d messages to topic: %s\n\n", MSG_COUNT, TOPIC);
for (int i = 1; i <= MSG_COUNT; i++) {

    struct SensorData data;

    snprintf(data.Sequenznummer,
             sizeof(data.Sequenznummer),
             "%d",
             i);

    strcpy(data.Stationsname, "S1");
    strcpy(data.Zeitstempel, "2026-05-24T12:00:00Z");

    /* absichtlich ungültig */
    data.Temperatur = 250.0f;

    data.Luftfeuchtigkeit = 55.5f;

    serialize_to_json(&data, payload, sizeof(payload));

    printf("[publisher] Publishing: %s\n", payload);

    rc = mosquitto_publish(
            mosq,
            NULL,
            TOPIC,
            (int)strlen(payload),
            payload,
            1,
            false);

    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr,
                "[publisher] Publish error: %s\n",
                mosquitto_strerror(rc));
    }

    sleep(INTERVAL_S);
}

    printf("\n[publisher] Done. Disconnecting.\n");
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
 decke mir für die Funktion serialize_to_json die restlichen randfälle ab. Diese habe ich folgendermaßen fetgelegt und das verhalten dafür
Zeitstempel string 20 zeichen  Fehlermeldung
->Zeitstempel mit 0 gefüllt(Datenblock ignorieren)Stationsname 
Temperatur float  -273,15°C bis 100°C Fehlermeldung
->Setze Temp auf Null (Ignorieren)
Luftfeuchtigkeit float  0% bis 100% Fehlermeldung
->Setze auf Null (Ignorieren)
Stationsname String 3 Zeichen Fehlermeldung
-> Name mit 0 füllen(Datenblock ignorieren)
Sequenznummer String keine Umlaute

->Alle Fehler sollen in Auswertung trotzdem angezeigt werden


zeitstempel sollen 20 zeichen sein nicht 19 und stationsname 3 und nicht 2