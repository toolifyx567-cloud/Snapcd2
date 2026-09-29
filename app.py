import threading
import time
import requests
import random
import string
import re
import sqlite3
import concurrent.futures
from dotenv import load_dotenv
import os
import yt_dlp

load_dotenv()

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ===== SESSION SETUP =====
session = requests.Session()
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

retry = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=1000, pool_maxsize=1000)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ===== DATABASE =====
conn = sqlite3.connect("stats.db", check_same_thread=False)
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER)''')
for key in ["requests", "downloads", "cache_hits", "videos_served"]:
    c.execute("INSERT OR IGNORE INTO stats (key,value) VALUES (?,?)", (key, 0))

c.execute('''CREATE TABLE IF NOT EXISTS unique_ips (ip TEXT PRIMARY KEY)''')
c.execute('''CREATE TABLE IF NOT EXISTS video_cache (url TEXT PRIMARY KEY, video_url TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS download_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, url TEXT, timestamp INTEGER)''')
conn.commit()

cache = {}

# ===== HELPERS =====
def clean_filename(text):
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = re.sub(r'\s+', " ", text).strip()
    return text[:120]

def random_string(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def expand_url(url):
    try:
        if "snapchat.com/l/" in url or "sc-cdn.net" in url:
            r = session.head(url, allow_redirects=True, timeout=5)
            return r.url
    except:
        pass
    return url

# ============================================================
#  1. APIFY — SNAPCHAT HASHTAG SCRAPER (unwatermarked priority)
# ============================================================
def fetch_with_apify_hashtag(url):
    """
    Uses the Snapchat Hashtag Scraper actor which returns BOTH
    watermarked and unwatermarked URLs. We pick the unwatermarked one.
    """
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return None
    try:
        actor_id = "crawlerbros~snapchat-hashtag-scraper"
        run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={apify_token}"
        payload = {
            "hashtags": [url],
            "resultsPerHashtag": 1,
            "includeVideoUrls": True
        }
        response = session.post(run_url, json=payload, timeout=20)
        if response.status_code not in (200, 201):
            print(f"[Apify Hashtag] Start failed: {response.status_code}")
            return None
        run_id = response.json().get("data", {}).get("id")
        if not run_id:
            return None

        for _ in range(30):
            time.sleep(2)
            status_res = session.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}?token={apify_token}",
                timeout=10
            )
            status_data = status_res.json().get("data", {})
            if status_data.get("status") == "SUCCEEDED":
                dataset_id = status_data.get("defaultDatasetId")
                items_res = session.get(
                    f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={apify_token}",
                    timeout=10
                )
                items = items_res.json()
                if items and len(items) > 0:
                    item = items[0]
                    # PRIORITIZE unwatermarked URL
                    video_url = item.get("video_url_unwatermarked") or item.get("video_url")
                    if video_url:
                        print(f"[Apify Hashtag] Success (unwatermarked={bool(item.get('video_url_unwatermarked'))})")
                        return {
                            "video_url": video_url,
                            "title": item.get("title", "Snapchat Video"),
                            "author": item.get("creator_username", ""),
                            "thumbnail": item.get("thumbnail_url", "")
                        }
            elif status_data.get("status") in ("FAILED", "ABORTED", "TIMED-OUT"):
                print(f"[Apify Hashtag] Run failed: {status_data.get('status')}")
                break
    except Exception as e:
        print(f"[Apify Hashtag] error: {e}")
    return None

# ============================================================
#  2. PAGE SCRAPER — tries to find unwatermarked in page source
# ============================================================
def fetch_with_scraper(url):
    try:
        response = session.get(url, timeout=6)
        if response.status_code == 200:
            for pattern in [
                r'"videoUrlUnwatermarked"\s*:\s*"([^"]+)"',
                r'"video_url_unwatermarked"\s*:\s*"([^"]+)"',
                r'"mediaUrl"\s*:\s*"([^"]+)"',
                r'"videoUrl"\s*:\s*"([^"]+)"',
            ]:
                match = re.search(pattern, response.text)
                if match:
                    return {
                        "video_url": match.group(1),
                        "title": "Snapchat Video",
                        "author": "",
                        "thumbnail": ""
                    }
    except Exception as e:
        print(f"[Scraper] error: {e}")
    return None

# ============================================================
#  3. APIFY — ORIGINAL BYTEPULSE ACTOR (fallback)
# ============================================================
def fetch_with_apify_original(url):
    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        return None
    try:
        actor_id = "bytepulselabs~snapchat-video-downloader"
        run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={apify_token}"
        payload = {"urls": [{"url": url}], "quality": "480"}
        response = session.post(run_url, json=payload, timeout=20)
        if response.status_code not in (200, 201):
            print(f"[Apify Original] Start failed: {response.status_code}")
            return None
        run_data = response.json().get("data", {})
        run_id = run_data.get("id")
        if not run_id:
            return None

        for _ in range(30):
            time.sleep(2)
            status_res = session.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}?token={apify_token}",
                timeout=10
            )
            status_data = status_res.json().get("data", {})
            if status_data.get("status") == "SUCCEEDED":
                dataset_id = status_data.get("defaultDatasetId")
                items_res = session.get(
                    f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={apify_token}",
                    timeout=10
                )
                items = items_res.json()
                if items and len(items) > 0:
                    item = items[0]
                    video_url = item.get("videoUrl") or item.get("downloadUrl")
                    if video_url:
                        print("[Apify Original] Success")
                        return {
                            "video_url": video_url,
                            "title": item.get("title", "Snapchat Video"),
                            "author": item.get("author", ""),
                            "thumbnail": item.get("thumbnail", "")
                        }
            elif status_data.get("status") in ("FAILED", "ABORTED", "TIMED-OUT"):
                print(f"[Apify Original] Run failed: {status_data.get('status')}")
                break
    except Exception as e:
        print(f"[Apify Original] error: {e}")
    return None

# ============================================================
#  4. YT-DLP — final fallback (may have watermark)
# ============================================================
def fetch_with_ytdlp(url):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'format': 'best[ext=mp4]/best',
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            video_url = info.get('url')
            if not video_url and 'formats' in info and info['formats']:
                mp4_formats = [f for f in info['formats'] if f.get('ext') == 'mp4']
                video_url = mp4_formats[-1]['url'] if mp4_formats else info['formats'][-1]['url']
            if video_url:
                print("[yt-dlp] Success")
                return {
                    "video_url": video_url,
                    "title": info.get('title') or 'Snapchat Video',
                    "author": info.get('uploader') or '',
                    "thumbnail": info.get('thumbnail') or ''
                }
    except Exception as e:
        print(f"[yt-dlp] error: {e}")
    return None

# ============================================================
#  MAIN FETCH — PRIORITIZES UNWATERMARKED SOURCES
# ============================================================
def fetch_snapchat_video(url):
    url = expand_url(url)

    # Priority order: unwatermarked sources first
    fetchers = [
        fetch_with_apify_hashtag,   # Best: returns unwatermarked explicitly
        fetch_with_scraper,          # Tries to find unwatermarked in page
        fetch_with_apify_original,   # Your original Apify actor
        fetch_with_ytdlp,            # yt-dlp fallback
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(fetchers)) as executor:
        futures = [executor.submit(f, url) for f in fetchers]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    result["original_url"] = url
                    return result
            except Exception as e:
                print(f"Fetcher exception: {e}")
    return None

# ===== SAVE CACHE =====
def save_cache_db(url, video_url):
    try:
        conn2 = sqlite3.connect("stats.db")
        c2 = conn2.cursor()
        c2.execute("INSERT OR REPLACE INTO video_cache (url,video_url) VALUES (?,?)", (url, video_url))
        conn2.commit()
        conn2.close()
    except Exception as e:
        print("DB thread error:", e)

# ===== ROUTES =====
@app.route("/download", methods=["POST"])
def download_video():
    try:
        data = request.get_json()
        url = data.get("url")
        ip = request.remote_addr
        if not url:
            return jsonify({"success": False, "message": "No URL"}), 400

        try:
            c.execute("UPDATE stats SET value=value+1 WHERE key='requests'")
            c.execute("INSERT OR IGNORE INTO unique_ips (ip) VALUES (?)", (ip,))
            conn.commit()
        except:
            pass

        result = fetch_snapchat_video(url)
        print("FETCH RESULT:", result)

        if not result:
            return jsonify({"success": False, "message": "Failed to fetch video – link may be private or unsupported"}), 500

        video_url = result["video_url"]
        title = result.get("title", "")
        author = result.get("author", "")
        thumbnail = result.get("thumbnail", "")

        cache[url] = video_url
        threading.Thread(target=save_cache_db, args=(url, video_url), daemon=True).start()

        try:
            c.execute("UPDATE stats SET value=value+1 WHERE key='downloads'")
            c.execute("UPDATE stats SET value=value+1 WHERE key='videos_served'")
            c.execute("INSERT INTO download_logs (ip,url,timestamp) VALUES (?,?,?)", (ip, url, int(time.time())))
            conn.commit()
        except:
            pass

        filename = clean_filename(title or "Snapchat") + "_" + random_string() + ".mp4"

        return jsonify({
            "success": True,
            "url": video_url,
            "filename": filename,
            "title": title,
            "author": author,
            "thumbnail": thumbnail,
            "videoId": url
        })

    except Exception as e:
        print("CRASH PREVENTED:", e)
        return jsonify({"success": False, "message": "Server error, please retry"}), 500

@app.route("/file")
def serve_file():
    video_url = request.args.get("url")
    video_id = request.args.get("videoId")
    mode = request.args.get("mode", "preview")

    if video_id and not video_url:
        result = fetch_snapchat_video(video_id)
        if result:
            video_url = result["video_url"]
        else:
            return jsonify({"success": False, "message": "Could not re-fetch video"}), 500

    if not video_url:
        return jsonify({"success": False, "message": "No video URL"}), 400

    try:
        range_header = request.headers.get("Range")
        source_headers = {}
        if range_header:
            source_headers["Range"] = range_header

        r = session.get(video_url, stream=True, timeout=15, headers=source_headers)

        filename = f"Snapchat_{random_string()}.mp4"
        status_code = 206 if r.status_code == 206 else 200
        headers = {
            "Content-Type": r.headers.get("Content-Type", "video/mp4"),
            "Accept-Ranges": "bytes",
        }
        if "Content-Range" in r.headers:
            headers["Content-Range"] = r.headers["Content-Range"]
        if "Content-Length" in r.headers:
            headers["Content-Length"] = r.headers["Content-Length"]

        disposition = f'attachment; filename="{filename}"' if mode == "download" else f'inline; filename="{filename}"'
        headers["Content-Disposition"] = disposition

        def generate():
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    yield chunk

        return Response(generate(), status=status_code, headers=headers)

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/stats", methods=["GET"])
def get_stats():
    c.execute("SELECT key,value FROM stats")
    stats_data = dict(c.fetchall())
    c.execute("SELECT COUNT(*) FROM unique_ips")
    unique_ips = c.fetchone()[0]
    c.execute("SELECT ip,url,timestamp FROM download_logs")
    logs = [{"ip": ip, "url": url, "timestamp": ts} for ip, url, ts in c.fetchall()]
    return jsonify({**stats_data, "unique_ips": unique_ips, "download_logs": logs})

@app.route("/wake", methods=["GET"])
def wake():
    return jsonify({"success": True, "message": "Server is awake"})

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

@app.route("/admin/reset", methods=["POST"])
def reset_stats():
    data = request.get_json()
    password = data.get("password")
    if password != ADMIN_PASSWORD:
        return jsonify({"success": False, "message": "Wrong password"}), 401
    for key in ["requests", "downloads", "cache_hits", "videos_served"]:
        c.execute("UPDATE stats SET value=0 WHERE key=?", (key,))
    c.execute("DELETE FROM unique_ips")
    c.execute("DELETE FROM download_logs")
    conn.commit()
    return jsonify({"success": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
