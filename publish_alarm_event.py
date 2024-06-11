from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Configuración
SERVICE_BUS_CONNECTION_STRING = "Endpoint=sb://gestionoym.servicebus.windows.net/;SharedAccessKeyName=accessGestionOYM;SharedAccessKey=A+yT6j/XMUa0J2Vf4BtdvrQcGO0Fp+mHM+ASbIQfGhU"
QUEUE_NAME = "incidents"

def publish_alarm_event(event):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=SERVICE_BUS_CONNECTION_STRING, logging_enable=True)

    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            message = ServiceBusMessage(event)
            sender.send_messages(message)
            print(f"Sent: {event}")

if __name__ == "__main__":
    # Evento de alarma de ejemplo
    alarm_event = {
        "event_id": "12345",
        "type": "CPU_OVERLOAD",
        "severity": "high",
        "description": "CPU usage exceeded 90%"
    }

    # Publicar evento de alarma
    publish_alarm_event(str(alarm_event))

