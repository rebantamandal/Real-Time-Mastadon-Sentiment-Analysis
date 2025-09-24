from mastodon import Mastodon

# --- This is where you put the keys you just generated ---
mastodon = Mastodon(
    client_id='qpHVy7dW7A_2lkNsseHiUFetEqHIfEuKIc8e6zirvNM',
    client_secret='Yupj-XTy-Dy99c7RFn8DCPHS_vgHYysRKVUjM1M0Frk',
    access_token='i2rC1YQsJbOpnAL-o-Tzr_k6Bfgj8J3SGlgKudX2iQY',
    api_base_url='https://mastodon.social'  
)

print("Successfully connected to Mastodon!")


hashtag_to_track = 'Science' # <-- CHANGE THIS to your chosen hashtag

print(f"Starting to track the hashtag: #{hashtag_to_track}")

# 2. COLLECT POSTS
try:
    # Fetch the 40 most recent toots with this hashtag
    hashtag_posts = mastodon.timeline_hashtag(hashtag_to_track, limit=40)

    # 3. PROCESS AND PRINT
    if hashtag_posts:
        for toot in hashtag_posts:
            # You will do your cleaning and sentiment analysis here later
            print(toot['content'])
            print("-" * 20)
    else:
        print(f"Found no recent posts for #{hashtag_to_track}. Try a more popular hashtag!")

except Exception as e:
    print(f"An error occurred: {e}")