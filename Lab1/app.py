import streamlit as st
import json
from server.interactions_consumer import InteractionsConsumer
from ml.sasrec_inferencer import SasRecInferencer
import time
from collections import Counter
import pandas as pd

st.set_page_config(page_title="Click Stream Status", layout="wide")

if "stream" not in st.session_state:
    st.session_state["stream"] = []
    st.session_state["stream_unique_users"] = []
    st.session_state["rec_genres"] = {}
    st.session_state["rec_number"] = []

config = {"bootstrap.servers": "localhost:9095", "group.id": "my_consumers"}
consumer = InteractionsConsumer(config=config, topic_name="stream_interactions")

model = SasRecInferencer()
unique_users = set()

st.title("Click Stream Status")
stream_chart_holder = st.empty()

st.title("Unique Users")
users_chart_holder = st.empty()

st.title("Recommendation number")
recom_chart_holder = st.empty()

st.title("Recommended Genres Disctribution")
genres_chart_holder = st.empty()
while True:
    interactions = consumer.consume()
    st.session_state["stream"].append(len(interactions))
    if len(interactions) > 0:
        unique_users.update(interactions["user_id"].unique().tolist())
        st.session_state["stream_unique_users"].append(len(unique_users))
    stream_chart_holder.line_chart(st.session_state["stream"])
    users_chart_holder.line_chart(st.session_state["stream_unique_users"])

    if len(interactions) > 0:
        recommendations = model.predict(interactions)
        st.session_state["rec_number"].append(len(recommendations))
        for genre in recommendations["genres"].apply(lambda x: x.split("|")[0]).values.tolist():
            st.session_state["rec_genres"][genre] = [st.session_state["rec_genres"].get(genre, [0])[0] + 1]

    recom_chart_holder.line_chart(st.session_state["rec_number"])
    genres_chart_holder.bar_chart(pd.DataFrame(st.session_state["rec_genres"]), horizontal=True)

    time.sleep(1)