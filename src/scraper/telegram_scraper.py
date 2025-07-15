import os
import asyncio
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import json 

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env'))

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')

db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

MAX_MEDIA_FILE_SIZE_BYTES = 20 * 1024 * 1024 # 20 MB

if not api_id or not api_hash:
    print("Error: TELEGRAM_API_ID or TELEGRAM_API_HASH not set in .env")
    exit(1)

client = TelegramClient('telegram_scraper_session', api_id, api_hash)

async def connect_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

async def create_table_if_not_exists(conn):
    """Creates the telegram_messages table if it doesn't exist."""
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS telegram_messages (
        id BIGINT PRIMARY KEY,
        chat_id BIGINT,
        sender_id BIGINT,
        message_date TIMESTAMP WITH TIME ZONE,
        message_text TEXT,
        views INTEGER,
        forwards INTEGER,
        replies_count INTEGER,
        media_type TEXT,
        media_path TEXT,
        _extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        _file TEXT,
        raw_message_json JSONB -- To store the full raw message for flexibility
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'telegram_messages' ensured to exist.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

async def insert_message(conn, message_data):
    """Inserts a single message into the telegram_messages table."""
    cursor = conn.cursor()
    insert_query = sql.SQL("""
        INSERT INTO telegram_messages (
            id, chat_id, sender_id, message_date, message_text,
            views, forwards, replies_count, media_type, media_path, _file, raw_message_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (id) DO UPDATE SET
            chat_id = EXCLUDED.chat_id,
            sender_id = EXCLUDED.sender_id,
            message_date = EXCLUDED.message_date,
            message_text = EXCLUDED.message_text,
            views = EXCLUDED.views,
            forwards = EXCLUDED.forwards,
            replies_count = EXCLUDED.replies_count,
            media_type = EXCLUDED.media_type,
            media_path = EXCLUDED.media_path,
            _extracted_at = NOW(),
            _file = EXCLUDED._file,
            raw_message_json = EXCLUDED.raw_message_json;
    """)
    try:
        cursor.execute(insert_query, list(message_data.values()))
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error inserting message {message_data.get('id')}: {e}")
    finally:
        cursor.close()

async def scrape_channel(channel_username, limit=5):
    """Scrapes messages from a Telegram channel and saves them to the database."""
    # Ensure client is connected
    if not client.is_connected():
        await client.start()

    print(f"\n--- Scraping messages from {channel_username} (limit: {limit}) ---")

    conn = await connect_db()
    if not conn:
        print("Failed to connect to database, skipping scraping for this channel.")
        return

    await create_table_if_not_exists(conn)

    # Create a directory to save media
    media_save_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw', 'telegram_media', channel_username)
    os.makedirs(media_save_dir, exist_ok=True)

    messages_scraped = 0
    async for message in client.iter_messages(channel_username, limit=limit):
        media_type = None
        media_path = None
        download_skipped_reason = None

        if message.media:
            # Check file size before attempting download
            file_size = 0
            if hasattr(message.media, 'document') and hasattr(message.media.document, 'size'):
                file_size = message.media.document.size
            elif hasattr(message.media, 'photo') and hasattr(message.media.photo, 'sizes') and message.media.photo.sizes:

                largest_photo_size = max([s.size for s in message.media.photo.sizes if hasattr(s, 'size')], default=0)
                file_size = largest_photo_size

            if file_size > MAX_MEDIA_FILE_SIZE_BYTES:
                media_type = 'skipped_too_large'
                download_skipped_reason = f"File too large ({file_size / (1024 * 1024):.2f}MB > {MAX_MEDIA_FILE_SIZE_BYTES / (1024 * 1024):.2f}MB)"
                print(f"Skipping download for message {message.id}: {download_skipped_reason}")
            else:
                if isinstance(message.media, MessageMediaPhoto):
                    media_type = 'photo'
                elif isinstance(message.media, MessageMediaDocument):
                    media_type = 'document'
                else:
                    media_type = 'other_media' 

                # Download media
                try:
                    file_path = await message.download_media(file=media_save_dir)
                    if file_path:
                        media_path = os.path.relpath(file_path, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
                except Exception as e:
                    print(f"Error downloading media for message {message.id}: {e}")
                    media_path = "Download_Failed"


        if download_skipped_reason:
            media_path = download_skipped_reason


        message_data = {
            'id': message.id,
            'chat_id': message.chat_id,
            'sender_id': message.sender_id,
            'message_date': message.date,
            'message_text': message.message,
            'views': message.views if hasattr(message, 'views') else 0,
            'forwards': message.forwards if hasattr(message, 'forwards') else 0,
            'replies_count': message.replies.replies if message.replies else 0,
            'media_type': media_type,
            'media_path': media_path,
            '_file': f"telegram_channel_{channel_username}.json", # Simple identifier
            'raw_message_json': json.dumps(message.to_dict(), default=str) # Store full message as JSON
        }
        await insert_message(conn, message_data)
        messages_scraped += 1

    if conn:
        conn.close()
    print(f"--- Finished scraping {channel_username}. Scraped {messages_scraped} messages. ---")

async def main_scraper_task():
    """Main function to run scraping for the specified channels."""


    channels_to_scrape = [
        'lobelia4cosmetics', 
        'tikvahpharma',      
        'who_news',          
    ]

    messages_limit_per_channel = 100

    print(f"Attempting to scrape {len(channels_to_scrape)} channels.")
    for channel_username in channels_to_scrape:
        try:
            await scrape_channel(channel_username, limit=messages_limit_per_channel)
        except Exception as e:
            print(f"An error occurred while scraping {channel_username}: {e}")

    if client.is_connected():
        await client.disconnect()
    print("\nAll scraping tasks completed.")


if __name__ == "__main__":

    asyncio.run(main_scraper_task())

    print("\nScraping script finished. Now you can try running 'dbt run' again to process new data!")