import json
from config import db_config
import redis

class redis_db:
    def __init__(self):
        """
        Initializes the Redis connection.

        Args:
            host (str): Redis server host. Defaults to 'localhost'.
            port (int): Redis server port. Defaults to 6379.
            db (int): Redis database number. Defaults to 0.
            password (str, optional): Password for Redis authentication. Defaults to None.
            decode_responses (bool): Whether to decode responses from Redis (e.g., bytes to strings).
                                     Defaults to True for easier handling of string data.
        """
        self.host = db_config.redis_host
        self.port = db_config.redis_port
        self.db = db_config.redis_db
        self.password = db_config.redis_password
        self.decode_responses = db_config.redis_decode_responses
        self.client = None
        self._connect()

    def _connect(self):
        """Establishes the connection to the Redis server."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses,
                socket_timeout=5 # Add a timeout for connection attempts
            )
            # Test the connection
            self.client.ping()
            print(f"Successfully connected to Redis at {self.host}:{self.port}, DB: {self.db}")
        except Exception as e:
            print(f"An unexpected error occurred during connection: {e}")
            self.client = None

    def is_connected(self) -> bool:
        """Checks if the client is connected to Redis."""
        if not self.client:
            return False
        try:
            return self.client.ping()
        except Exception as e:
            return False
        
    def create(self, key: str, value, use_json: bool = False) -> bool:
        """
        Creates a new key-value pair in Redis.
        If the key already exists, its value will be overwritten.

        Args:
            key (str): The key to store the data under.
            value: The value to store. Can be string, bytes, int, float.
                   If use_json is True, it can be a dict or list.
            use_json (bool): If True, serialize 'value' to JSON before storing.
                             Defaults to False.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self.is_connected():
            print("Error: Not connected to Redis.")
            return False
        try:
            data_to_store = json.dumps(value) if use_json else value
            return self.client.set(key, data_to_store)
        except TypeError as e:
            print(f"Error: Could not serialize value for key '{key}'. Ensure it's JSON serializable if use_json=True. Error: {e}")
            return False
        except Exception as e:
            print(f"Error in redis_db.create(): {e}")
            return False
        
    def read(self, key: str, parse_json: bool = False):
        """
        Reads the value associated with a key from Redis.

        Args:
            key (str): The key whose value needs to be retrieved.
            parse_json (bool): If True, attempt to parse the retrieved value as JSON.
                               Defaults to False.

        Returns:
            The value associated with the key, or None if the key doesn't exist
            or an error occurred. If parse_json is True and successful, returns
            the parsed Python object (dict/list).
        """
        if not self.is_connected():
            print("Error: Not connected to Redis.")
            return None
        try:
            value = self.client.get(key)
            if value is None:
                # Key doesn't exist, which is not an error state
                return None

            if parse_json:
                try:
                    # Assumes decode_responses=True was used in init
                    return json.loads(value)
                except json.JSONDecodeError:
                    print(f"Warning: Value for key '{key}' is not valid JSON, returning as string.")
                    return value # Return raw value if JSON parsing fails
            else:
                # Value is already decoded to string if decode_responses=True
                # If decode_responses=False, value would be bytes here
                return value
        except Exception as e:
            print(f"Error: redis_db.read(), Error reading key '{key}': {e}")
            return None
        
    def update(self, key: str, value, use_json: bool = False) -> bool:
        """
        Updates the value for an existing key.
        Note: In Redis, SET performs an overwrite, so this is functionally
              identical to create(). This method is provided for semantic clarity.
              You could add a check to ensure the key exists first if needed.

        Args:
            key (str): The key whose value needs to be updated.
            value: The new value to store.
            use_json (bool): If True, serialize 'value' to JSON. Defaults to False.


        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        # create directly overwrites thekey if it exists, so its fine
        return self.create(key, value, use_json=use_json)
    
    def delete(self, key: str) -> bool:
        """
        Deletes a key-value pair from Redis.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was deleted, False if the key didn't exist or an error occurred.
        """
        if not self.is_connected():
            print("Error: Not connected to Redis.")
            return False
        try:
            # DEL returns the number of keys deleted (0 or 1 in this case)
            deleted_count = self.client.delete(key)
            return deleted_count > 0
        except Exception as e:
            print(f"Error: redis_db.delete(), Error deleting key '{key}': {e}")
            return False
        
    def exists(self, key: str) -> bool:
        """
        Checks if a key exists in Redis.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise or if disconnected.
        """
        if not self.is_connected():
            print("Error: Not connected to Redis.")
            return False
        try:
            # EXISTS returns the number of keys that exist (0 or 1 here)
            return self.client.exists(key) > 0
        except Exception as e:
            print(f"Error checking existence for key '{key}': {e}")
            return False
        
    def set_with_ttl(self, key: str, value, ttl_seconds: int, use_json: bool = False) -> bool:
        """
        Sets a key-value pair with a Time-To-Live (TTL) in seconds.
        The key will automatically expire after ttl_seconds.

        Args:
            key (str): The key to store.
            value: The value to store.
            ttl_seconds (int): The time-to-live in seconds.
            use_json (bool): If True, serialize 'value' to JSON. Defaults to False.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.is_connected():
            print("Error: Not connected to Redis.")
            return False
        try:
            data_to_store = json.dumps(value) if use_json else value
            # Use setex for simplicity if value is string/bytes
            # Or use set with ex parameter for more general types
            # return self.client.setex(key, ttl_seconds, data_to_store)
            return self.client.set(key, data_to_store, ex=ttl_seconds)
        except TypeError as e:
             print(f"Error: Could not serialize value for key '{key}'. Ensure it's JSON serializable if use_json=True. Error: {e}")
             return False
        except Exception as e:
            print(f"Error in redis_db.set_with_ttl, Error setting key '{key}' with TTL: {e}")
            return False
        
    def close_connection(self):
        """Closes the Redis connection if it's open."""
        if self.client:
            try:
                self.client.close()
                print("Redis connection closed.")
                self.client = None
            except Exception as e:
                print(f"Error closing Redis connection: {e}")


def get_value_from_redis(key:str):
    """
        This function is used to get the key from redis
    """
    try:
        db=redis_db()
        data=db.get(key)
        db.close_connection()
        return json.loads(data)
    except Exception as e:
        print(f"Error getting key from Redis: {e}")
        return None