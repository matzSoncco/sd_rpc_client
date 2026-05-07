import threading
from threading import Event
from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)


class RpcClient(object):
    def __init__(self, host, username, password, rpc_queue):
        self.responses = {}
        self.events = {}

        self.host = host
        self.username = username
        self.password = password

        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue

        self.open()

    def open(self):
        self.connection = amqpstorm.Connection(
            self.host,
            self.username,
            self.password
        )
        self.channel = self.connection.channel()

        # ✅ Cola corregida
        self.channel.queue.declare(
            queue=self.rpc_queue,
            durable=True
        )

        # Cola exclusiva para respuestas
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )

        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        corr_id = message.correlation_id
        self.responses[corr_id] = message.body

        if corr_id in self.events:
            self.events[corr_id].set()

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue

        event = Event()
        self.events[message.correlation_id] = event

        message.publish(routing_key=self.rpc_queue)

        return message.correlation_id, event


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id, event = RPC_CLIENT.send_request(payload)

    event.wait(timeout=5)

    return RPC_CLIENT.responses.get(corr_id, "Timeout")


if __name__ == '__main__':
    RPC_CLIENT = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')
    app.run()