import amqpstorm

def on_request(message):
    print(f"[x] Recibido: {message.body}")

    response = f"Procesado: {message.body}"

    # 🔥 RESPUESTA MANUAL CORRECTA
    message.channel.basic.publish(
        body=response,
        routing_key=message.reply_to,
        properties={
            'correlation_id': message.correlation_id
        }
    )

    message.ack()


# Conexión
connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')
channel = connection.channel()

# Cola RPC
channel.queue.declare(
    queue='rpc_queue',
    durable=True
)

channel.basic.qos(prefetch_count=1)

channel.basic.consume(on_request, queue='rpc_queue')

print("[x] Esperando solicitudes RPC...")
channel.start_consuming()