/*
 * test_publisher.c
 * MQTT Test Publisher
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
    char Zeitstempel[22];      // 21 Zeichen + '\0'
    char Stationsname[4];      // z.B. "S1"
    float Temperatur;
    float Luftfeuchtigkeit;
    char Sequenznummer[100];
};

/* ---------------- Datengenerator ---------------- */

static struct SensorData dataGenerator(int seq)
{
    struct SensorData data;

    time_t now = time(NULL);
    struct tm *t = gmtime(&now);

    strftime(
        data.Zeitstempel,
        sizeof(data.Zeitstempel),
        "%Y-%m-%dT%H:%M:%SZ",
        t
    );

    snprintf(
        data.Stationsname,
        sizeof(data.Stationsname),
        "S%d",
        ((seq - 1) % 3) + 1
    );

    data.Temperatur =
        18.0f + (float)(rand() % 100) / 20.0f;

    data.Luftfeuchtigkeit =
        50.0f + (float)(rand() % 300) / 10.0f;

    snprintf(
        data.Sequenznummer,
        sizeof(data.Sequenznummer),
        "%d",
        seq
    );

    return data;
}

/* ---------------- JSON Serializer ---------------- */

void serialize_to_json(
    struct SensorData *data,
    char *buf,
    size_t len
) {
    int temp_valid = 1;
    int humidity_valid = 1;

    /* Temperatur prüfen */
    if (data->Temperatur < -273.15f ||
        data->Temperatur > 100.0f)
    {
        fprintf(stderr,
            "[Fehler] Ungültige Temperatur: %.2f°C\n",
            data->Temperatur);

        temp_valid = 0;
    }

    /* Luftfeuchtigkeit prüfen */
    if (data->Luftfeuchtigkeit < 0.0f ||
        data->Luftfeuchtigkeit > 100.0f)
    {
        fprintf(stderr,
            "[Fehler] Ungültige Luftfeuchtigkeit: %.2f%%\n",
            data->Luftfeuchtigkeit);

        humidity_valid = 0;
    }

    /* Stationsname prüfen */
    size_t st_len = strlen(data->Stationsname);

    if (st_len < 2 || st_len > 3) {
        fprintf(stderr,
            "[Fehler] Stationsname ungültig\n");

        strcpy(data->Stationsname, "IN");
    }

    /* Zeitstempel prüfen */
    if (strlen(data->Zeitstempel) != 20) {
        fprintf(stderr,
            "[Fehler] Zeitstempel ungültig\n");

        strcpy(
            data->Zeitstempel,
            "0000-00-00T00:00:00Z"
        );
    }

    /* ASCII Check */
    for (size_t i = 0;
         data->Sequenznummer[i] != '\0';
         i++)
    {
        unsigned char c =
            (unsigned char)data->Sequenznummer[i];

        if (c > 127) {
            fprintf(stderr,
                "[Fehler] Sequenznummer enthält Sonderzeichen\n");

            strcpy(data->Sequenznummer, "INVALID");
            break;
        }
    }

    char temp_str[32];
    char humidity_str[32];

    if (temp_valid)
        snprintf(
            temp_str,
            sizeof(temp_str),
            "%.1f",
            data->Temperatur
        );
    else
        strcpy(temp_str, "null");

    if (humidity_valid)
        snprintf(
            humidity_str,
            sizeof(humidity_str),
            "%.1f",
            data->Luftfeuchtigkeit
        );
    else
        strcpy(humidity_str, "null");

    /* JSON bauen */
    snprintf(buf, len,
        "{"
        "\"Sequenznummer\":\"%s\","
        "\"Stationsname\":\"%s\","
        "\"Zeitstempel\":\"%s\","
        "\"Temperatur\":%s,"
        "\"Luftfeuchtigkeit\":%s"
        "}",
        data->Sequenznummer,
        data->Stationsname,
        data->Zeitstempel,
        temp_str,
        humidity_str
    );
}

/* ---------------- Payload Builder ---------------- */

static void build_payload(
    char *buf,
    size_t len,
    int seq
) {
    struct SensorData data =
        dataGenerator(seq);

    serialize_to_json(&data, buf, len);
}

/* ---------------- MQTT Callbacks ---------------- */

static void on_connect(
    struct mosquitto *mosq,
    void *userdata,
    int rc
) {
    (void)mosq;
    (void)userdata;

    if (rc == 0) {
        printf(
            "[publisher] Connected to %s:%d\n",
            BROKER,
            PORT
        );
    } else {
        fprintf(stderr,
            "[publisher] Connection failed: %s\n",
            mosquitto_connack_string(rc));

        mosquitto_disconnect(mosq);
    }
}

static void on_publish(
    struct mosquitto *mosq,
    void *userdata,
    int mid
) {
    (void)mosq;
    (void)userdata;

    printf(
        "[publisher] Message %d delivered\n",
        mid
    );
}

int mqtt_connect_and_publish()
{
    struct mosquitto *mosq;
    int rc;

    mosquitto_lib_init();

    mosq = mosquitto_new(
        "mqtt-lab-publisher",
        true,
        NULL
    );

    if (!mosq) {
        fprintf(stderr,
            "[publisher] Failed to create client\n");

        mosquitto_lib_cleanup();
        return 1;
    }

    /* Callbacks setzen */
    mosquitto_connect_callback_set(
        mosq,
        on_connect
    );

    mosquitto_publish_callback_set(
        mosq,
        on_publish
    );

    /* ---------------- Retry-Logik ---------------- */

    int retries = 3;
    int connected = 0;

    for (int attempt = 1;
         attempt <= retries;
         attempt++)
    {
        printf(
            "[publisher] Connection attempt %d/%d...\n",
            attempt,
            retries
        );

        rc = mosquitto_connect(
            mosq,
            BROKER,
            PORT,
            60
        );

        if (rc == MOSQ_ERR_SUCCESS) {
            connected = 1;

            printf(
                "[publisher] Successfully connected.\n"
            );

            break;
        }

        fprintf(stderr,
            "[publisher] Connection failed: %s\n",
            mosquitto_strerror(rc));

        if (attempt < retries) {
            printf(
                "[publisher] Retrying in 2 seconds...\n"
            );

            sleep(2);
        }
    }

    /* Nach allen Versuchen fehlgeschlagen */
    if (!connected) {

        fprintf(stderr,
            "[publisher] Broker unreachable after %d attempts\n",
            retries);

        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();

        return 1;
    }

    /* Netzwerkloop starten */
    mosquitto_loop_start(mosq);

    char payload[256];

    printf(
        "[publisher] Sending %d messages\n\n",
        MSG_COUNT
    );

    /* Nachrichten senden */
    for (int i = 1; i <= MSG_COUNT; i++) {

        build_payload(
            payload,
            sizeof(payload),
            i
        );

        printf(
            "[publisher] Publishing: %s\n",
            payload
        );

        rc = mosquitto_publish(
            mosq,
            NULL,
            TOPIC,
            (int)strlen(payload),
            payload,
            1,
            false
        );

        if (rc != MOSQ_ERR_SUCCESS) {

            fprintf(stderr,
                "[publisher] Publish error: %s\n",
                mosquitto_strerror(rc));
        }

        sleep(INTERVAL_S);
    }

    printf("\n[publisher] Done.\n");

    /* Cleanup */
    mosquitto_disconnect(mosq);

    mosquitto_loop_stop(
        mosq,
        true
    );

    mosquitto_destroy(mosq);

    mosquitto_lib_cleanup();

    return 0;
}

int main(void)
{
    srand((unsigned int)time(NULL));

    int result =
        mqtt_connect_and_publish();

    if (result != 0) {

        fprintf(stderr,
            "[main] Publisher terminated with error\n");

        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}