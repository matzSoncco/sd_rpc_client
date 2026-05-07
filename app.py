import os
import threading
from time import sleep
from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient(object):
    """Cliente RPC Asíncrono adaptado para conexiones en la nube."""

    def __init__(self, host, username, password, virtual_host, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        """Establece la conexión con CloudAMQP y configura las colas necesarias."""
        self.connection = amqpstorm.Connection(
            hostname=self.host,
            username=self.username,
            password=self.password,
            virtual_host=self.virtual_host,
            port=5672
        )
        self.channel = self.connection.channel()
        
        self.channel.queue.declare(
            queue=self.rpc_queue, 
            durable=False, 
            auto_delete=False
        )
        
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        
        self.channel.basic.consume(
            self._on_response, 
            no_ack=True,
            queue=self.callback_queue
        )
        self._create_process_thread()

    def _create_process_thread(self):
        """Crea y ejecuta un hilo en segundo plano para el consumo de eventos."""
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True 
        thread.start()

    def _process_data_events(self):
        """Inicia el consumo de respuestas del broker en el canal actual."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """Almacena el cuerpo de la respuesta usando su ID de correlación."""
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        """Publica una solicitud RPC y retorna el ID único de la transacción."""
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id


RPC_CLIENT = RpcClient(
    host='horse.lmq.cloudamqp.com',
    username='jrynbgfw',
    password='fW12mOLo5JzmtTd_gJx83y04HCCxl3hS',
    virtual_host='jrynbgfw',
    rpc_queue='vilef_rpc_queue'
)

@app.route('/')
def home():
    """Ruta base para verificar el estado del cliente web."""
    return "Cliente RPC - Activo y listo"

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Endpoint principal que expone la funcionalidad RPC vía HTTP."""
    corr_id = RPC_CLIENT.send_request(payload)

    timeout = 0
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)
        timeout += 1
        if timeout > 200:
            return "Error: Timeout - El servicio backend no responde."

    respuesta = RPC_CLIENT.queue[corr_id]
    del RPC_CLIENT.queue[corr_id] 
    
    return respuesta

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)