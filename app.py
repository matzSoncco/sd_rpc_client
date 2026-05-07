from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Servidor Flask publicado correctamente"

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    return f"Procesado: {payload}"

if __name__ == "__main__":
    app.run()