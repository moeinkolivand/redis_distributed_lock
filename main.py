from faststream.kafka import KafkaBroker
from faststream import FastStream

kafka_broker = KafkaBroker(["localhost:9092", "localhost:9094", "localhost:9096"])
app = FastStream(kafka_broker)

@kafka_broker.subscriber("test_topic")
async def on_input_data(msg):
    print("Received message:", msg)
