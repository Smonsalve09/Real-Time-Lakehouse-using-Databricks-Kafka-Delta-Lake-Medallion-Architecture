!pip install confluent-kafka
import json

from confluent_kafka import Producer
from websocket import WebSocketApp

# =========================================================
# CONFLUENT CLOUD CONFIG
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
KAFKA_TOPIC = "crypto-transaction"

KAFKA_API_KEY = "E7FIZIBKNYJXFO4K"
KAFKA_API_SECRET = "cflt1GuGMjLlEF431MS+O0gIcFHAxOgMA9p+KRV5j3IARQQ8cFFNPAtm9+yXkjbQ"

# =========================================================
# KAFKA PRODUCER CONFIG
# =========================================================

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_API_KEY,
    "sasl.password": KAFKA_API_SECRET,

    # reliability
    "acks": "all",
    "client.id": "coinbase-producer",

    # debug opcional
    # "debug": "broker,security"
}

producer = Producer(producer_config)

# =========================================================
# DELIVERY REPORT
# =========================================================

def delivery_report(err, msg):
    if err is not None:
        print("\n❌ DELIVERY FAILED")
        print(err)
    else:
        print("\n✅ MESSAGE DELIVERED")
        print(f"Topic      : {msg.topic()}")
        print(f"Partition  : {msg.partition()}")
        print(f"Offset     : {msg.offset()}")

# =========================================================
# WEBSOCKET CALLBACKS
# =========================================================

def on_open(ws):
    print("\n✅ Connected to Coinbase WebSocket")

    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {
                "name": "matches",
                "product_ids": ["BTC-USD"]
            }
        ]
    }

    ws.send(json.dumps(subscribe_message))

    print("✅ Subscription message sent")


def on_message(ws, message):
    try:
        data = json.loads(message)

        # imprimir raw para debug
        print("\n📩 RAW MESSAGE:")
        print(data)

        # ignorar mensajes que no son trades
        if data.get("type") != "match":
            return

        event = {
            "symbol": data["product_id"],
            "price": float(data["price"]),
            "quantity": float(data["size"]),
            "side": data["side"],
            "trade_id": data["trade_id"],
            "timestamp": data["time"]
        }

        print("\n🚀 SENDING TO KAFKA:")
        print(event)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event["symbol"],
            value=json.dumps(event),
            callback=delivery_report
        )

        # fuerza envío inmediato
        producer.flush()

    except Exception as e:
        print("\n❌ ERROR PROCESSING MESSAGE")
        print(str(e))


def on_error(ws, error):
    print("\n❌ WEBSOCKET ERROR")
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("\n❌ CONNECTION CLOSED")
    print(close_status_code)
    print(close_msg)

# =========================================================
# START WEBSOCKET
# =========================================================

ws = WebSocketApp(
    "wss://ws-feed.exchange.coinbase.com",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print("\n=================================================")
print("STARTING COINBASE → CONFLUENT KAFKA STREAM")
print("=================================================\n")

ws.run_forever()