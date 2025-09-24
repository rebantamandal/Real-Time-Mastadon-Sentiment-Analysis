import json, os, signal, sys, time
from bs4 import BeautifulSoup
from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer

# --- Config ---
MASTODON_BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.social")
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN", "i2rC1YQsJbOpnAL-o-Tzr_k6Bfgj8J3SGlgKudX2iQY")  # required
HASHTAG = os.getenv("MASTODON_HASHTAG", "Science")          # no '#'
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mastodon-raw")

if not MASTODON_ACCESS_TOKEN:
    print("❌ ERROR: set MASTODON_ACCESS_TOKEN env var", file=sys.stderr)
    sys.exit(1)

# --- Clients ---
# mastodon = Mastodon(
#     api_base_url=MASTODON_BASE_URL,
#     access_token=MASTODON_ACCESS_TOKEN,
#     ratelimit_method="pace",
# )

mastodon = Mastodon(
    client_id='qpHVy7dW7A_2lkNsseHiUFetEqHIfEuKIc8e6zirvNM',
    client_secret='Yupj-XTy-Dy99c7RFn8DCPHS_vgHYysRKVUjM1M0Frk',
    access_token='i2rC1YQsJbOpnAL-o-Tzr_k6Bfgj8J3SGlgKudX2iQY',
    api_base_url='https://mastodon.social'
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks="all",
    enable_idempotence=True,
    compression_type="gzip",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
)

# --- Helpers ---
def clean_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    return soup.get_text(" ", strip=True)

def to_record(status):
    return {
        "id": str(status["id"]),
        "created_at": str(status.get("created_at")),
        "lang": (status.get("language") or "").lower(),
        "account_acct": status.get("account", {}).get("acct"),
        "account_display_name": status.get("account", {}).get("display_name"),
        "reblog": bool(status.get("reblog")),
        "sensitive": bool(status.get("sensitive")),
        "tags": [t.get("name") for t in status.get("tags", [])],
        "content_text": clean_html(status.get("content")),
        "hashtag_tracked": HASHTAG.lower(),
    }

# --- Stream listener ---
class HashtagListener(StreamListener):
    def on_update(self, status):
        try:
            rec = to_record(status)
            print(f"📥 {rec['account_acct']}: {rec['content_text'][:80]}...")
            producer.send(TOPIC, key=rec["id"], value=rec)
        except Exception as e:
            print("⚠️ Producer error:", e, file=sys.stderr)

    def on_delete(self, status_id):
        print(f"🗑️ Deleted toot: {status_id}", file=sys.stderr)

    def on_connection_error(self, err):
        print("⚠️ Connection error:", err, file=sys.stderr)
        time.sleep(5)

    def on_abort(self, err):
        print("⚠️ Stream aborted:", err, file=sys.stderr)
        time.sleep(5)

# --- Shutdown ---
def shutdown(*_):
    print("👋 Shutting down...")
    try:
        producer.flush(5)
        producer.close(5)
    finally:
        sys.exit(0)

# --- Main ---
if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    print(f"🚀 Streaming #{HASHTAG} from {MASTODON_BASE_URL} → Kafka topic {TOPIC}")
    while True:  # resilient loop
        try:
            mastodon.stream_hashtag(
                HASHTAG,
                listener=HashtagListener(),
                timeout=60,
                reconnect_async=False,
            )
        except Exception as e:
            print("⚠️ Fatal stream error:", e, file=sys.stderr)
            time.sleep(10)
