from confluent_kafka import Producer
import random 
import time
import json
from rs_datasets import MovieLens

class InteractionsProducer:
    def __init__(self, config:str, topic_name: str):
        self.topic = topic_name
        self._producer = Producer(config)

    def _prepare_interactions(self):
        movielens = MovieLens("1m")
        interactions = movielens.ratings
        interactions["timestamp"] = interactions["timestamp"].astype("int64")
        interactions = interactions.sort_values(by="timestamp")
        interactions["timestamp"] = interactions.groupby("user_id").cumcount()
        return interactions

    def produce_interactions(self):
        interactions = self._prepare_interactions().sample(frac = 1)

        for _, interaction in interactions.iterrows():
            self._producer.produce(self.topic, key="1", value=json.dumps(interaction.to_dict()))
            self._producer.flush()
            print("broker get OK")
            time.sleep(random.random())

if __name__=="__main__":
    config = {"bootstrap.servers": "localhost:9095"}

    producer = InteractionsProducer(config=config, topic_name="stream_interactions")
    producer.produce_interactions()