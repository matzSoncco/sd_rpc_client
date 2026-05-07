import threading
import requests

def call_api(data):
    r = requests.get(f"http://127.0.0.1:5000/rpc_call/{data}")
    print(data, "->", r.text)

for i in range(3):
    threading.Thread(target=call_api, args=(f"msg{i}",)).start()