import os, sys, json, time
from kafka import KafkaConsumer

# --- Sentiment libraries ---
try:
    from textblob import TextBlob
    USE_TEXTBLOB = True
except ImportError:
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    import nltk
    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()
    USE_TEXTBLOB = False

# --- Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mastodon-raw")

# --- Consumer ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mastodon-consumer",
)

def analyze_sentiment(text: str):
    if not text:
        return {"polarity": 0.0, "subjectivity": 0.0, "label": "neutral"}
    if USE_TEXTBLOB:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subj = blob.sentiment.subjectivity
    else:
        score = sia.polarity_scores(text)
        polarity = score["compound"]
        subj = score["neu"]  # not true subjectivity, just neutral score
    # Label
    if polarity > 0.1:
        label = "positive"
    elif polarity < -0.1:
        label = "negative"
    else:
        label = "neutral"
    return {"polarity": polarity, "subjectivity": subj, "label": label}

print(f"📡 Listening to Kafka topic '{TOPIC}' on {KAFKA_BOOTSTRAP} ...")

try:
    for msg in consumer:
        rec = msg.value
        sentiment = analyze_sentiment(rec.get("content_text", ""))
        print(
            f"👤 {rec.get('account_acct')} | "
            f"📝 {rec.get('content_text')[:60]}... | "
            f"📊 Sentiment: {sentiment['label']} ({sentiment['polarity']:.2f})"
        )
except KeyboardInterrupt:
    print("👋 Shutting down consumer...")
    consumer.close()
    sys.exit(0)
