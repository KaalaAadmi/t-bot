import redis
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class RedisClient:
    """A client for managing Redis connection, streams, and publishing."""
    def __init__(self, host: str, port: int, db: int):
        """Initializes the Redis connection pool."""
        self.host = host # Storing host and port for logging
        self.port = port
        self.db = db
        # Use a reasonable connection timeout
        self._pool = redis.ConnectionPool(
            host=host, 
            port=port, 
            db=db, 
            decode_responses=True,
            socket_timeout=5 # Add a connection timeout
        )
        self.r = redis.Redis(connection_pool=self._pool)
        logger.info(f"Redis connection pool initialized at {host}:{port}/{db}.")
        
    def check_connection(self) -> bool:
        """Pings Redis to check connection health."""
        try:
            self.r.ping()
            logger.info("Successfully connected to Redis and connection is healthy.")
            return True
        except Exception as e:
            # Enhanced error message to show the connection details
            logger.error(f"Failed to connect to Redis at {self.host}:{self.port}/{self.db}. Error: {e}")
            return False

    def create_consumer_group(self, stream_name: str, group_name: str, mkstream: bool = True):
        """Creates a consumer group for a given stream, if it does not already exist."""
        try:
            # Check if the group already exists
            info = self.r.xinfo_groups(stream_name)
            if not any(group['name'] == group_name for group in info):
                # Create the group. '0' means start reading from the beginning.
                self.r.xgroup_create(
                    name=stream_name, 
                    groupname=group_name, 
                    id='0', 
                    mkstream=mkstream
                )
                logger.info(f"Successfully created consumer group '{group_name}' on stream '{stream_name}'.")
            else:
                logger.debug(f"Consumer group '{group_name}' already exists on stream '{stream_name}'.")
        except redis.exceptions.ResponseError as e:
            # This can happen if the stream doesn't exist and mkstream=False
            if "NOGROUP" in str(e) and not mkstream:
                logger.warning(f"Stream {stream_name} does not exist. Cannot create group {group_name}.")
            elif "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{group_name}' already exists.")
            else:
                logger.error(f"Error creating consumer group {group_name} on {stream_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in create_consumer_group for {stream_name}: {e}")


    def initialize_streams(self, stream_names: list) -> None:
        """
        Ensures all necessary Redis Streams are initialized (if they don't exist).
        This is primarily to verify connection and log channel definition.
        """
        for stream_name in stream_names:
            # We don't need to explicitly create the stream as XADD does it, 
            # but we can try an initial XINFO command to confirm existence if needed.
            # For simplicity and robustness, we rely on XADD creation, and just log here.
            logger.info(f"Declared communication stream: {stream_name}")

    def publish_message(self, stream_name: str, data: Dict[str, Any]):
        """
        Publishes a dictionary message to a Redis Stream using XADD.
        Handles serialization of datetime objects.
        """
        message = {}
        for key, value in data.items():
            # Convert datetime objects to ISO format string
            if isinstance(value, datetime):
                message[key] = value.isoformat()
            # Convert numbers/other types to string (Redis stream values are strings/bytes)
            else:
                message[key] = str(value)
                
        try:
            # XADD adds a new item to a stream, * is for auto-generated ID
            # Redis requires all fields to be strings when using this client
            self.r.xadd(stream_name, message)
            # logger.debug(f"Published to {stream_name}: {message}") # Use debug level for high-frequency logs
        except Exception as e:
            logger.error(f"Error publishing to Redis stream {stream_name}: {e}")