import os
import json
import time
import datetime
from typing import List, Dict

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from pytz import timezone

from airflow import DAG
from airflow.decorators import dag, task
from datetime import timedelta


DEFAULT_SUBREDDITS: List[str] = [
    'MusicIndia'
]


def get_db_conn(conn_str: str):
    return psycopg2.connect(conn_str, cursor_factory=RealDictCursor)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reddit_posts (
                id SERIAL PRIMARY KEY,
                post_id VARCHAR(32) UNIQUE NOT NULL,
                subreddit VARCHAR(100) NOT NULL,
                title TEXT,
                description TEXT,
                upvotes INT,
                downvotes INT,
                score INT,
                url TEXT,
                author VARCHAR(255),
                author_fullname VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE,
                created_utc BIGINT,
                num_comments INT,
                over_18 BOOLEAN,
                pinned BOOLEAN,
                distinguished VARCHAR(64),
                locked BOOLEAN,
                spoiler BOOLEAN,
                stickied BOOLEAN,
                flair_text TEXT,
                flair_css TEXT,
                thumbnail TEXT,
                thumbnail_width INT,
                thumbnail_height INT,
                is_video BOOLEAN,
                media_urls TEXT,
                post_hint VARCHAR(128),
                domain VARCHAR(255),
                subreddit_subscribers INT,
                upvote_ratio REAL,
                scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            """
        )
    conn.commit()


@dag(
    dag_id="reddit_trending_pipeline",
    description="Scrape Reddit posts from last 24 hours and save to PostgreSQL",
    start_date=datetime.datetime(2025, 10, 1),
    schedule="0 6 * * *",  # every day at 6 AM
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["reddit", "scraping", "taskflow"],
)
def reddit_trending_pipeline():
    @task
    def get_config() -> Dict:
        india_tz = timezone('Asia/Kolkata')
        now = datetime.datetime.now(india_tz)
        cutoff = now - datetime.timedelta(hours=24)
        db_conn = os.environ.get("REDDIT_DB_CONN", "postgresql://airflow:airflow@postgres:5432/airflow")
        delay_s = int(os.environ.get("REDDIT_REQUEST_DELAY_S", "2"))
        return {
            "now_iso": now.isoformat(),
            "cutoff_ts": int(cutoff.timestamp()),
            "db_conn": db_conn,
            "delay_s": delay_s,
        }

    @task
    def get_subreddits() -> List[str]:
        """
        Reads subreddits from the environment variable REDDIT_SUBREDDITS.
        Example:
        REDDIT_SUBREDDITS='["MusicIndia", "IndiaTech", "worldnews"]'
        """
        subreddits_env = os.environ.get("REDDIT_SUBREDDITS")
        try:
            if subreddits_env:
                subs = json.loads(subreddits_env)
                if isinstance(subs, list) and all(isinstance(s, str) for s in subs):
                    return subs
        except Exception as e:
            print(f"Invalid REDDIT_SUBREDDITS env var: {e}")
        return DEFAULT_SUBREDDITS

    @task
    def fetch_subreddit(args: Dict, subreddit: str) -> List[Dict]:
        headers = {'User-Agent': 'IndiaTrendingScraper/0.1'}
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
        results: List[Dict] = []
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            india_tz = timezone('Asia/Kolkata')
            for post in data.get('data', {}).get('children', []):
                p = post.get('data', {})
                created_utc = int(p.get('created_utc', 0))
                # Only last 24 hours
                if created_utc <= args["cutoff_ts"]:
                    continue
                if int(p.get('ups', 0)) < 100:
                    continue

                media_urls: List[str] = []
                if p.get('post_hint') == 'image' and p.get('url_overridden_by_dest'):
                    media_urls.append(p['url_overridden_by_dest'])
                if p.get('is_video') and p.get('media'):
                    rv = p['media'].get('reddit_video', {})
                    if rv.get('fallback_url'):
                        media_urls.append(rv['fallback_url'])
                if p.get('gallery_data') and p.get('media_metadata'):
                    for item in p['gallery_data']['items']:
                        mid = item['media_id']
                        info = p['media_metadata'].get(mid, {})
                        if info.get('status') == 'valid':
                            s = info.get('s', {})
                            u = s.get('u')
                            if u:
                                media_urls.append(u.replace('&amp;', '&'))

                created_dt = datetime.datetime.fromtimestamp(created_utc, tz=india_tz)
                results.append({
                    'Title': p.get('title'),
                    'Description': p.get('selftext'),
                    'Upvotes': p.get('ups'),
                    'Downvotes': p.get('downs'),
                    'Score': p.get('score'),
                    'Subreddit': subreddit,
                    'URL': f"https://reddit.com{p.get('permalink')}",
                    'Post_ID': p.get('id'),
                    'Author': p.get('author'),
                    'Author_Fullname': p.get('author_fullname'),
                    'Created': created_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'Created_UTC': created_utc,
                    'Edited': p.get('edited'),
                    'Num_Comments': p.get('num_comments'),
                    'Over_18': p.get('over_18'),
                    'Pinned': p.get('pinned'),
                    'Distinguished': p.get('distinguished'),
                    'Locked': p.get('locked'),
                    'Spoiler': p.get('spoiler'),
                    'Stickied': p.get('stickied'),
                    'Flair_Text': p.get('link_flair_text'),
                    'Flair_CSS': p.get('link_flair_css_class'),
                    'Thumbnail': p.get('thumbnail'),
                    'Thumbnail_Width': p.get('thumbnail_width'),
                    'Thumbnail_Height': p.get('thumbnail_height'),
                    'Is_Video': p.get('is_video'),
                    'Media_URLs': ', '.join(media_urls),
                    'Post_Hint': p.get('post_hint'),
                    'Domain': p.get('domain'),
                    'Subreddit_Subscribers': p.get('subreddit_subscribers'),
                    'Upvote_Ratio': p.get('upvote_ratio'),
                })
        except Exception as e:
            print(f"Error fetching r/{subreddit}: {e}")
        time.sleep(args["delay_s"])  # rate-limit
        return results

    @task
    def combine_results(results: List[List[Dict]]) -> List[Dict]:
        combined: List[Dict] = []
        for part in results:
            if part:
                combined.extend(part)
        return combined

    @task
    def persist_and_output(args: Dict, rows: List[Dict]) -> Dict:
        if not rows:
            return {"rows": 0}

        conn = get_db_conn(args["db_conn"])
        ensure_tables(conn)
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO reddit_posts (
                        post_id, subreddit, title, description, upvotes, downvotes, score, url,
                        author, author_fullname, created_at, created_utc, num_comments, over_18,
                        pinned, distinguished, locked, spoiler, stickied, flair_text, flair_css,
                        thumbnail, thumbnail_width, thumbnail_height, is_video, media_urls, post_hint,
                        domain, subreddit_subscribers, upvote_ratio
                    ) VALUES (
                        %(Post_ID)s, %(Subreddit)s, %(Title)s, %(Description)s, %(Upvotes)s, %(Downvotes)s, %(Score)s, %(URL)s,
                        %(Author)s, %(Author_Fullname)s, %(Created)s, %(Created_UTC)s, %(Num_Comments)s, %(Over_18)s,
                        %(Pinned)s, %(Distinguished)s, %(Locked)s, %(Spoiler)s, %(Stickied)s, %(Flair_Text)s, %(Flair_CSS)s,
                        %(Thumbnail)s, %(Thumbnail_Width)s, %(Thumbnail_Height)s, %(Is_Video)s, %(Media_URLs)s, %(Post_Hint)s,
                        %(Domain)s, %(Subreddit_Subscribers)s, %(Upvote_Ratio)s
                    )
                    ON CONFLICT (post_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        upvotes = EXCLUDED.upvotes,
                        downvotes = EXCLUDED.downvotes,
                        score = EXCLUDED.score,
                        url = EXCLUDED.url,
                        author = EXCLUDED.author,
                        author_fullname = EXCLUDED.author_fullname,
                        created_at = EXCLUDED.created_at,
                        created_utc = EXCLUDED.created_utc,
                        num_comments = EXCLUDED.num_comments,
                        over_18 = EXCLUDED.over_18,
                        pinned = EXCLUDED.pinned,
                        distinguished = EXCLUDED.distinguished,
                        locked = EXCLUDED.locked,
                        spoiler = EXCLUDED.spoiler,
                        stickied = EXCLUDED.stickied,
                        flair_text = EXCLUDED.flair_text,
                        flair_css = EXCLUDED.flair_css,
                        thumbnail = EXCLUDED.thumbnail,
                        thumbnail_width = EXCLUDED.thumbnail_width,
                        thumbnail_height = EXCLUDED.thumbnail_height,
                        is_video = EXCLUDED.is_video,
                        media_urls = EXCLUDED.media_urls,
                        post_hint = EXCLUDED.post_hint,
                        domain = EXCLUDED.domain,
                        subreddit_subscribers = EXCLUDED.subreddit_subscribers,
                        upvote_ratio = EXCLUDED.upvote_ratio,
                        scraped_at = NOW()
                    """,
                    r,
                )
        conn.commit()
        conn.close()
        return {"rows": len(rows)}

    cfg = get_config()
    subs = get_subreddits()
    fetched = fetch_subreddit.partial(args=cfg).expand(subreddit=subs)
    combined = combine_results(fetched)

    @task
    def to_json(rows: List[Dict]) -> str:
        return json.dumps(rows, ensure_ascii=False, indent=2)

    persist_and_output(cfg, combined)
    to_json(combined)


dag = reddit_trending_pipeline()
