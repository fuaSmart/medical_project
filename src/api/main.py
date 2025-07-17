#from fastapi import FastAPI

#app = FastAPI()

#@app.get("/")
#async def read_root():
  #  return {"message": "Hello from Medical Project API!"}

import os
import asyncio
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends
from typing import List, Dict, Any, Optional
import json 

load_dotenv()

app = FastAPI(
    title="Ethiopian Medical Telegram Data API",
    description="API to access scraped and enriched Telegram data related to Ethiopian medical businesses.",
    version="1.0.0",
)

# --- Database Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT', '5432')

# Global connection pool
db_pool = None

# --- Database Connection Dependency ---
async def get_db_connection():
    """
    Dependency to get a connection from the pool.
    Uses asyncio.to_thread for synchronous psycopg2 operations.
    """
    conn = None
    try:
        conn = await asyncio.to_thread(db_pool.getconn)
        yield conn
    except Exception as e:
        print(f"Error getting DB connection from pool: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")
    finally:
        if conn:
            await asyncio.to_thread(db_pool.putconn, conn)

# --- FastAPI Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    """Initialize the database connection pool on FastAPI startup."""
    global db_pool
    print("Initializing database connection pool...")
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        # Test a connection to ensure it works
        conn = await asyncio.to_thread(db_pool.getconn)
        await asyncio.to_thread(db_pool.putconn, conn)
        print("Database connection pool initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize database connection pool: {e}")
        raise 

@app.on_event("shutdown")
async def shutdown_event():
    """Close the database connection pool on FastAPI shutdown."""
    global db_pool
    if db_pool:
        print("Closing database connection pool...")
        await asyncio.to_thread(db_pool.closeall)
        print("Database connection pool closed.")

# --- API Endpoints ---

@app.get("/", summary="Root endpoint for API status")
async def read_root():
    """Returns a welcome message to confirm the API is running."""
    return {"message": "Welcome to the Ethiopian Medical Data API! 🩺"}

# -----------------------------------------------------------------------------

@app.get("/messages", response_model=List[Dict[str, Any]], summary="Retrieve recent Telegram messages")
async def get_messages(
    limit: int = 10,
    offset: int = 0,
    channel_username: Optional[str] = None, # New optional filter
    min_views: Optional[int] = None,       # New optional filter
    conn: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves recent messages from the `fct_messages` table.
    Allows filtering by `channel_username` and `min_views`.
    """
    if not (1 <= limit <= 100):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 100.")

    cursor = conn.cursor()
    try:
        query = """
            SELECT
                message_id,
                channel_username,
                message_text,
                message_date,
                views_count,
                forwards_count,
                link,
                has_media -- Assuming you added this to fct_messages or can infer from local_media_path in raw
            FROM fct_messages
            WHERE 1=1
        """
        params = []

        if channel_username:
            query += " AND channel_username ILIKE %s"
            params.append(f"%{channel_username}%")
        if min_views is not None:
            query += " AND views_count >= %s"
            params.append(min_views)

        query += " ORDER BY message_date DESC LIMIT %s OFFSET %s;"
        params.extend([limit, offset])

        cursor.execute(query, tuple(params))
        messages = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in messages]

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving messages: {e}")
    finally:
        cursor.close()

# -----------------------------------------------------------------------------

@app.get("/channels", response_model=List[Dict[str, Any]], summary="Retrieve unique channels")
async def get_channels(
    conn: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves a list of unique channels from the `dim_channels` table.
    Ordered by total messages.
    """
    cursor = conn.cursor()
    try:
        query = """
            SELECT
                channel_id,
                channel_username,
                first_message_date,
                last_message_date,
                total_messages
            FROM dim_channels
            ORDER BY total_messages DESC;
        """
        cursor.execute(query)
        channels = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in channels]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving channels: {e}")
    finally:
        cursor.close()

# -----------------------------------------------------------------------------

@app.get("/image_detections", response_model=List[Dict[str, Any]], summary="Retrieve object detection results from images")
async def get_image_detections(
    limit: int = 10,
    offset: int = 0,
    object_class: Optional[str] = None,
    min_confidence: float = 0.0,
    channel_username: Optional[str] = None, # New filter for image detections
    conn: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves object detection results from the `fct_image_detections` table.
    Allows filtering by `object_class`, `min_confidence`, and `channel_username`.
    """
    if not (1 <= limit <= 100):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 100.")

    cursor = conn.cursor()
    try:
        query = """
            SELECT
                fid.image_detection_id,
                fid.message_id,
                fid.detected_message_date,
                fid.channel_username,
                fid.image_path,
                fid.detected_object_class,
                fid.confidence_score,
                fid.box_xmin,
                fid.box_ymin,
                fid.box_xmax,
                fid.box_ymax,
                fid.detection_timestamp
            FROM fct_image_detections fid
            WHERE 1=1
        """
        params = []

        if object_class:
            query += " AND fid.detected_object_class ILIKE %s"
            params.append(f"%{object_class}%")
        if min_confidence > 0:
            query += " AND fid.confidence_score >= %s"
            params.append(min_confidence)
        if channel_username:
            query += " AND fid.channel_username ILIKE %s"
            params.append(f"%{channel_username}%")

        query += " ORDER BY fid.detection_timestamp DESC LIMIT %s OFFSET %s;"
        params.extend([limit, offset])

        cursor.execute(query, tuple(params))
        detections = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in detections]

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving image detections: {e}")
    finally:
        cursor.close()

# -----------------------------------------------------------------------------

@app.get("/image_detections/classes", response_model=List[str], summary="Retrieve unique detected object classes")
async def get_detection_classes(
    conn: psycopg2.extensions.connection = Depends(get_db_connection)
):
    """
    Retrieves a list of all unique detected object classes from the `fct_image_detections` table.
    """
    cursor = conn.cursor()
    try:
        query = """
            SELECT DISTINCT detected_object_class
            FROM fct_image_detections
            ORDER BY detected_object_class;
        """
        cursor.execute(query)
        classes = [row[0] for row in cursor.fetchall()]
        return classes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving detection classes: {e}")
    finally:
        cursor.close()