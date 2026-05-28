/*
 * test_subscriber.c
 * Connects to the public test broker and prints every message on the lab topic.
 * Usage: ./test_subscriber   (keep running while publisher sends)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <mosquitto.h>

#define BROKER  "localhost"
#define PORT    1883
#define TOPIC   "mqtt-lab/test/sensor"

static int msg_count = 0;

static int extract_json_string(
    const char *payload,
    const char *key,
    char *out,
    size_t out_len
) {
    const char *pos = strstr(payload, key);
    if (!pos) {
        return -1;
    }

    pos += strlen(key);
    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r') {
        pos++;
    }

    if (*pos != '"') {
        return -3;
    }

    pos++;
    const char *end = strchr(pos, '"');
    if (!end) {
        return -3;
    }

    size_t len = (size_t)(end - pos);
    if (len >= out_len) {
        len = out_len - 1;
    }

    memcpy(out, pos, len);
    out[len] = '\0';
    return 0;
}

static int extract_json_number(
    const char *payload,
    const char *key,
    float *out
) {
    const char *pos = strstr(payload, key);
    if (!pos) {
        return -1;
    }

    pos += strlen(key);
    while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r') {
        pos++;
    }

    if (*pos == 'n' && strncmp(pos, "null", 4) == 0) {
        return -3;
    }

    char *end = NULL;
    *out = strtof(pos, &end);
    if (end == pos) {
        return -3;
    }

    while (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r') {
        end++;
    }

    if (*end != ',' && *end != '}' && *end != '\0') {
        return -3;
    }

    return 0;
}

static int validate_timestamp(const char *timestamp) {
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    char trailing = '\0';

    if (strlen(timestamp) != 20) {
        return -3;
    }

    if (sscanf(timestamp, "%4d-%2d-%2dT%2d:%2d:%2dZ%c",
               &year, &month, &day, &hour, &minute, &second,
               &trailing) != 6) {
        return -3;
    }

    if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31 ||
        hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59) {
        return -2;
    }

    return 0;
}

int validate_message(const char *payload, size_t payload_len) {
    if (payload_len == 0 || payload == NULL) {
        return -1;
    }

    char *message = malloc(payload_len + 1);
    if (!message) {
        return -3;
    }

    memcpy(message, payload, payload_len);
    message[payload_len] = '\0';

    char sequenznummer[128];
    char stationsname[32];
    char zeitstempel[32];
    float temperatur = 0.0f;
    float luftfeuchtigkeit = 0.0f;

    int rc = extract_json_string(message, "\"Sequenznummer\":", sequenznummer, sizeof(sequenznummer));
    if (rc != 0) {
        free(message);
        return rc;
    }

    rc = extract_json_string(message, "\"Stationsname\":", stationsname, sizeof(stationsname));
    if (rc != 0) {
        free(message);
        return rc;
    }

    rc = extract_json_string(message, "\"Zeitstempel\":", zeitstempel, sizeof(zeitstempel));
    if (rc != 0) {
        free(message);
        return rc;
    }

    rc = extract_json_number(message, "\"Temperatur\":", &temperatur);
    if (rc != 0) {
        free(message);
        return rc;
    }

    rc = extract_json_number(message, "\"Luftfeuchtigkeit\":", &luftfeuchtigkeit);
    if (rc != 0) {
        free(message);
        return rc;
    }

    for (size_t i = 0; sequenznummer[i] != '\0'; i++) {
        unsigned char c = (unsigned char)sequenznummer[i];
        if (!isalnum(c)) {
            free(message);
            return -3;
        }
    }

    if (stationsname[0] != 'S' || stationsname[1] == '\0') {
        free(message);
        return -3;
    }

    for (size_t i = 1; stationsname[i] != '\0'; i++) {
        unsigned char c = (unsigned char)stationsname[i];
        if (!isdigit(c)) {
            free(message);
            return -3;
        }
    }

    rc = validate_timestamp(zeitstempel);
    if (rc != 0) {
        free(message);
        return rc;
    }

    if (temperatur < -273.15f || temperatur > 100.0f) {
        free(message);
        return -2;
    }

    if (luftfeuchtigkeit < 0.0f || luftfeuchtigkeit > 100.0f) {
        free(message);
        return -2;
    }

    free(message);
    return 0;
}

/* Callback: called when connection is established */
static void on_connect(struct mosquitto *mosq, void *userdata, int rc) {
    if (rc == 0) {
        printf("[subscriber] Connected to %s:%d\n", BROKER, PORT);
        printf("[subscriber] Subscribing to: %s\n\n", TOPIC);

        int sub_rc = mosquitto_subscribe(mosq, NULL, TOPIC, 1);
        if (sub_rc != MOSQ_ERR_SUCCESS) {
            fprintf(stderr, "[subscriber] Subscribe failed: %s\n",
                    mosquitto_strerror(sub_rc));
        }
    } else {
        fprintf(stderr, "[subscriber] Connection failed: %s\n",
                mosquitto_connack_string(rc));
    }
}

/* Callback: called when a message arrives */
static void on_message(struct mosquitto *mosq, void *userdata,
                       const struct mosquitto_message *msg) {
    msg_count++;
    printf("[subscriber] Message #%d received:\n", msg_count);
    printf("  Topic:   %s\n", msg->topic);
    printf("  Payload: %.*s\n\n", msg->payloadlen, (char *)msg->payload);

    int validation_rc = validate_message((const char *)msg->payload, (size_t)msg->payloadlen);
    printf("  Validation: %d\n\n", validation_rc);
}

/* Callback: called on disconnect */
static void on_disconnect(struct mosquitto *mosq, void *userdata, int rc) {
    if (rc != 0) {
        printf("[subscriber] Unexpected disconnect (rc=%d) – will reconnect\n", rc);
    } else {
        printf("[subscriber] Disconnected cleanly. Received %d messages total.\n",
               msg_count);
    }
}

int main(void) {
    mosquitto_lib_init();

    struct mosquitto *mosq = mosquitto_new("mqtt-lab-subscriber", true, NULL);
    if (!mosq) {
        fprintf(stderr, "[subscriber] Failed to create mosquitto instance\n");
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);

    int rc = mosquitto_connect(mosq, BROKER, PORT, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[subscriber] Could not connect: %s\n",
                mosquitto_strerror(rc));
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
        return 1;
    }

    printf("[subscriber] Waiting for messages. Press Ctrl+C to stop.\n\n");

    /* Loop forever – handles reconnects automatically */
    mosquitto_loop_forever(mosq, -1, 1);

    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
