/*
 * test_subscriber.c
 * Connects to the public test broker and prints every message on the lab topic.
 * Usage: ./test_subscriber   (keep running while publisher sends)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mosquitto.h>

#define BROKER  "localhost"
#define PORT    1883
#define TOPIC   "mqtt-lab/test/sensor"

static int msg_count = 0;

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
