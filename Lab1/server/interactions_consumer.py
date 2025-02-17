from confluent_kafka import Consumer
import random 
import time
import json
import pandas as pd

class InteractionsConsumer:
    def __init__(self, config: dict, topic_name: str):
        self.topic = topic_name
        self._consumer = Consumer(config)
        self._consumer.subscribe([topic_name])
        

    def consume(self):
        data = []
        message = self._consumer.consume(num_messages=100, timeout=1)
        if message is not None:
            for msg in message:
                interaction = json.loads(msg.value().decode('utf-8'))
                data.append(interaction)
        return pd.DataFrame(data)

            