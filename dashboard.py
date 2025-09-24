import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
from kafka import KafkaConsumer
import json
import re
from textblob import TextBlob

st.title("📊 Mastodon Real-Time Sentiment Dashboard")

# Kafka consumer
consumer = KafkaConsumer(
    "mastodon-raw",
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

sentiments = []

placeholder = st.empty()

for msg in consumer:
    post = msg.value

    # Safely get content (fallback if missing)
    raw_content = post.get("content", "")
    clean = re.sub(r"<[^<]+?>", "", raw_content) if raw_content else "[No text]"

    # Sentiment analysis
    score = TextBlob(clean).sentiment.polarity
    if score > 0:
        label = "positive"
    elif score < 0:
        label = "negative"
    else:
        label = "neutral"

    sentiments.append(label)

    # Update live chart
    df = pd.Series(sentiments).value_counts()
    with placeholder.container():
        st.bar_chart(df)
        st.write(f"**Latest post:** {clean[:200]}...")  # preview latest message
