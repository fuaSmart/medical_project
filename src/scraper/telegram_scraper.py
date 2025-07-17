import os
import asyncio
from telethon.sync import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument 
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import time

load_dotenv()

# --- Database Configuration ---
DB_HOST = os.getenv('DB_HOST', 'db') 
# DB_HOST = 'localhost' 
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT', '5432')

print(f"DEBUG: Scraper attempting to connect to DB_HOST: {DB_HOST}")

# --- Telegram API Configuration ---
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')

TARGET_CHANNELS = [
    'chemed_tele_channel',
    'lobelia4cosmetics',
    'tikvahpharma',
]

# --- Media Download Path ---

MEDIA_DOWNLOAD_BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/raw/telegram_media')
os.makedirs(MEDIA_DOWNLOAD_BASE_PATH, exist_ok=True) 

# --- Telegram Client Initialization ---
client = TelegramClient('anon', API_ID, API_HASH)

# --- Database Functions ---

def get_db_connection(retries=5, delay=3):
    """
    Establishes and returns a database connection with retries.
    """
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        raise ValueError("Missing one or more database environment variables. Check your .env file.")

    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            print(f"Database connection successful after {i+1} attempt(s).")
            return conn
        except psycopg2.OperationalError as e:
            if "could not translate host name" in str(e) or "Is the server running on host" in str(e):
                print(f"Attempt {i+1}/{retries}: Database not ready yet ({e}). Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise e
        except Exception as e:
            raise e

    raise Exception(f"Failed to connect to database after {retries} attempts.")


async def ensure_raw_messages_table_exists():
    """
    Ensures the raw_telegram_messages table exists in the database.
    This table will store raw scraped data.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT UNIQUE NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_username TEXT,
            message_text TEXT,
            message_date TIMESTAMP WITH TIME ZONE,
            sender_id BIGINT,
            sender_username TEXT,
            views_count BIGINT,
            forwards_count BIGINT,
            replies_count JSONB,
            reactions_count JSONB,
            link TEXT,
            media_data JSONB,
            local_media_path TEXT, -- New column to store local path to saved media
            scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Ensured 'raw_telegram_messages' table exists.")
    except Exception as e:
        print(f"Error ensuring table exists: {e}")
        if conn:
            conn.rollback()

        raise
    finally:
        if conn:
            cur.close()
            conn.close()


async def insert_message_to_db(message_data):
    """Inserts a single message's data into the raw_telegram_messages table."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO raw_telegram_messages (
                message_id, channel_id, channel_username, message_text, message_date,
                sender_id, sender_username, views_count, forwards_count,
                replies_count, reactions_count, link, media_data, local_media_path
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (message_id) DO NOTHING;
        """)
        cur.execute(insert_query, (
            message_data['message_id'], message_data['channel_id'], message_data['channel_username'],
            message_data['message_text'], message_data['message_date'], message_data['sender_id'],
            message_data['sender_username'], message_data['views_count'], message_data['forwards_count'],
            message_data['replies_count'], message_data['reactions_count'], message_data['link'],
            message_data['media_data'], message_data['local_media_path'] 
        ))
        conn.commit()

    except Exception as e:
        print(f"Error inserting message {message_data.get('message_id', 'N/A')}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Telegram Scraping Logic ---

@client.on(events.NewMessage(chats=TARGET_CHANNELS))
async def my_event_handler(event):
    message = event.message
    chat = await event.get_chat()

    local_media_path = None 

    if message.media:

        channel_dir_name = chat.username if chat.username else str(chat.id)
        channel_media_path = os.path.join(MEDIA_DOWNLOAD_BASE_PATH, channel_dir_name)
        os.makedirs(channel_media_path, exist_ok=True) 

        try:
            
            print(f"Downloading media for message {message.id} from channel {channel_dir_name}...")

            file_path = await message.download_media(file=channel_media_path)
            local_media_path = str(file_path) 
            print(f"Media saved to: {local_media_path}")
        except Exception as e:
            print(f"Error downloading media for message {message.id}: {e}")
            local_media_path = None 

    # Extract relevant message data
    message_data = {
        'message_id': message.id,
        'channel_id': chat.id,
        'channel_username': chat.username if hasattr(chat, 'username') else None,
        'message_text': message.message,
        'message_date': message.date,
        'sender_id': message.sender_id,
        'sender_username': message.sender.username if message.sender and hasattr(message.sender, 'username') else None,
        'views_count': message.views,
        'forwards_count': message.forwards,
        'replies_count': message.repllies.to_json() if message.replies else None,
        'reactions_count': message.reactions.to_json() if message.reactions else None,
        'link': f"https://t.me/{chat.username}/{message.id}" if chat.username else None,
        'media_data': message.media.to_json() if message.media else None,
        'local_media_path': local_media_path 
    }

    await insert_message_to_db(message_data)

    print(f"Scraped & Inserted: Channel {message_data['channel_username']} - Message {message_data['message_id']}")


async def main():

    await ensure_raw_messages_table_exists()

    # Connect to Telegram
    print("Connecting to Telegram...")
    await client.start(phone=PHONE_NUMBER)
    print("Client connected!")

    
    print("Fetching past messages (this may take a while for large channels)...")
    for channel_entity in TARGET_CHANNELS:
        try:
            entity = await client.get_entity(channel_entity)

            async for message in client.iter_messages(entity, limit=None): 

                await my_event_handler(events.NewMessage.Event(message))
            print(f"Finished fetching past messages for channel: {channel_entity}")
        except Exception as e:
            print(f"Error fetching past messages for {channel_entity}: {e}")


    print("Listening for new messages (Press Ctrl+C to stop)...")
    await client.run_until_disconnected()


if __name__ == '__main__':

    if not all([API_ID, API_HASH, PHONE_NUMBER, DB_NAME, DB_USER, DB_PASSWORD]):
        print("Error: Missing required environment variables. Please check your .env file.")
        print("Required: API_ID, API_HASH, PHONE_NUMBER, DB_NAME, DB_USER, DB_PASSWORD")
    else:
        asyncio.run(main())