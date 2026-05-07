import os
import threading
from time import sleep
from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

AMQP_CONFIG = {
    "hostname": "horse.lmq.cloudamqp.com",
    "username": "jrynbgfw",
    "password": "fW12mOLo5JzmtTd_gJx83y04HCCxl3hS",
    "virtual_host": "jrynbgfw",
    "port": 5671,
    "ssl": True,
    "ssl_options": {"server_hostname": "horse.lmq.cloudamqp.com"},
    "heartbeat": 60
}

class RpcClient(object):

    def __init__(self, rpc_queue):
        self.queue = {}
        self.rpc_queue = rpc_queue
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.open()

    def open(self):
        self.connection = amqpstorm.Connection(**AMQP_CONFIG)
        self.channel = self.connection.channel()

        self.channel.queue.declare(
            queue=self.rpc_queue,
            durable=True,
            auto_delete=False
        )

        result = self.channel.queue.declare(exclusive=True, auto_delete=True)
        self.callback_queue = result['queue']

        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )
        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events, daemon=True)
        thread.start()

    def _process_data_events(self):
        try:
            self.channel.start_consuming(to_tuple=False)
        except Exception as e:
            print(f"[!] Hilo consumidor caído: {e}")

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        if not self.connection.is_open:
            print("[*] Reconectando cliente...")
            self.open()

        print(f"[DEBUG] Enviando payload: {payload}")
        print(f"[DEBUG] Cola destino: {self.rpc_queue}")
        print(f"[DEBUG] Cola callback: {self.callback_queue}")
        
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        
        print(f"[DEBUG] Mensaje publicado con corr_id: {message.correlation_id}")
        return message.correlation_id


RPC_CLIENT = RpcClient(rpc_queue='new_rpc_queue_cloud_v2')

@app.route('/')
def home():
    return "Cliente RPC - Activo y listo"

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    print(f"[DEBUG] Iniciando rpc_call con payload: {payload}")
    corr_id = RPC_CLIENT.send_request(payload)
    print(f"[DEBUG] Esperando respuesta para corr_id: {corr_id}")

    timeout = 0
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)
        timeout += 1
        if timeout > 200:
            del RPC_CLIENT.queue[corr_id]
            print(f"[DEBUG] TIMEOUT para corr_id: {corr_id}")
            return "Error: Timeout - El servicio backend no responde."

    respuesta = RPC_CLIENT.queue[corr_id]
    del RPC_CLIENT.queue[corr_id]
    return respuesta

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)