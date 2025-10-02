# dashboard.py
from flask import Flask, render_template
from kafka import KafkaConsumer
import json
import threading
from collections import deque
import pytz

app = Flask(__name__)

# Indian timezone
ist = pytz.timezone('Asia/Kolkata')

# Store data for 8 hours (market hours)
price_data = deque(maxlen=500)  # Increased buffer size

def consume_kafka():
    consumer = KafkaConsumer(
        "indian_avg",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    for message in consumer:
        data = message.value
        # Preserve original Spark timestamps
        price_data.append(data)
        print("📥 Dashboard received:", data)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/data')
def data():
    return {"prices": list(price_data)}

if __name__ == '__main__':
    threading.Thread(target=consume_kafka, daemon=True).start()
    app.run(port=5000, debug=True)
