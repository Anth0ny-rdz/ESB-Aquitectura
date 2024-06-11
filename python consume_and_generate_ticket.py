from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import uuid

# Configuración
SERVICE_BUS_CONNECTION_STRING = "Endpoint=sb://gestionoym.servicebus.windows.net/;SharedAccessKeyName=accessGestionOYM;SharedAccessKey=A+yT6j/XMUa0J2Vf4BtdvrQcGO0Fp+mHM+ASbIQfGhU"
INCIDENTS_QUEUE_NAME = "incidents"
TICKETS_QUEUE_NAME = "tickets"

def generate_ticket(alarm_event):
    ticket_id = str(uuid.uuid4())
    ticket = {
        "ticket_id": ticket_id,
        "event_id": alarm_event["event_id"],
        "description": alarm_event["description"],
        "status": "open"
    }
    return ticket

def on_message(message):
    alarm_event = json.loads(str(message))
    print(f"Received: {alarm_event}")

    # Generar el ticket
    ticket = generate_ticket(alarm_event)
    print(f"Generated ticket: {ticket}")

    # Publicar el ticket en la cola 'tickets'
    with servicebus_client.get_queue_sender(queue_name=TICKETS_QUEUE_NAME) as sender:
        ticket_message = ServiceBusMessage(json.dumps(ticket))
        sender.send_messages(ticket_message)
        print(f"Sent TicketId: {ticket['ticket_id']}")

    message.complete()

if __name__ == "__main__":
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=SERVICE_BUS_CONNECTION_STRING, logging_enable=True)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=INCIDENTS_QUEUE_NAME)
        with receiver:
            for msg in receiver:
                on_message(msg)
