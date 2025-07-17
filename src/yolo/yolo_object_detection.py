import os
import asyncio
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from ultralytics import YOLO 
import json 
import time

load_dotenv()

# --- Database Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT', '5432')

print(f"DEBUG (YOLO): Attempting to connect to DB_HOST: {DB_HOST}")

# --- YOLO Model Configuration ---

YOLO_MODEL_PATH = 'yolov8n.pt'
model = YOLO(YOLO_MODEL_PATH)


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


async def ensure_raw_image_detections_table_exists():
    """
    Ensures the raw_image_detections table exists in the database.
    This table will store raw YOLO detection results.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_image_detections (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            image_path TEXT NOT NULL,
            detected_objects JSONB, -- Store list of detected objects as JSON
            detection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            -- Add a unique constraint to prevent re-processing the same image for the same message
            UNIQUE (message_id, image_path)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Ensured 'raw_image_detections' table exists.")
    except Exception as e:
        print(f"Error ensuring 'raw_image_detections' table exists: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


async def get_messages_with_media_paths():
    """Retrieves messages with local media paths that haven't been processed."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT
                rtm.message_id,
                rtm.local_media_path
            FROM
                raw_telegram_messages rtm
            LEFT JOIN
                raw_image_detections rid ON rtm.message_id = rid.message_id AND rtm.local_media_path = rid.image_path
            WHERE
                rtm.local_media_path IS NOT NULL
                AND rid.id IS NULL; -- Only select if no corresponding detection record exists
        """
        cur.execute(query)
        return cur.fetchall()
    except Exception as e:
        print(f"Error retrieving messages with media paths: {e}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


async def insert_detection_results(message_id, image_path, detections):
    """Inserts YOLO detection results into the raw_image_detections table."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO raw_image_detections (
                message_id, image_path, detected_objects
            ) VALUES (
                %s, %s, %s
            ) ON CONFLICT (message_id, image_path) DO UPDATE SET
                detected_objects = EXCLUDED.detected_objects,
                detection_timestamp = NOW();
        """)
        cur.execute(insert_query, (
            message_id,
            image_path,
            json.dumps(detections) 
        ))
        conn.commit()
        print(f"Inserted/Updated YOLO detections for message {message_id} - {image_path}")
    except Exception as e:
        print(f"Error inserting YOLO detections for message {message_id} - {image_path}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

# --- YOLO Processing Logic ---

async def process_images_with_yolo():
    """
    Main function to fetch images, run YOLO, and store results.
    """
    await ensure_raw_image_detections_table_exists()

    messages_to_process = await get_messages_with_media_paths()

    if not messages_to_process:
        print("No new images with media paths found to process for YOLO detection.")
        return

    print(f"Found {len(messages_to_process)} new images to process with YOLO.")

    for message_id, image_path in messages_to_process:
        if not os.path.exists(image_path):
            print(f"Warning: Image file not found at {image_path} for message {message_id}. Skipping.")
            continue

        print(f"Processing image: {image_path} for message ID: {message_id}")
        try:
            # Run YOLO inference
            results = model(image_path) 

            detected_objects_list = []
            for r in results:

                for box in r.boxes:
                    class_id = int(box.cls[0])
                    confidence = float(box.conf[0])
                    bbox = box.xyxy[0].tolist() # [x1, y1, x2, y2]

                    detected_objects_list.append({
                        "class_id": class_id,
                        "class_name": model.names[class_id],
                        "confidence": confidence,
                        "bbox": bbox
                    })

            await insert_detection_results(message_id, image_path, detected_objects_list)

        except Exception as e:
            print(f"Error processing image {image_path} for message {message_id}: {e}")

    print("Finished processing all new images with YOLO.")

if __name__ == '__main__':

    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        print("Error: Missing required database environment variables. Check your .env file.")
        print("Required: DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT")
    else:
        asyncio.run(process_images_with_yolo())