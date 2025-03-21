# tools.py

import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from agents import function_tool
import time
import logging
import threading
import hashlib
from functools import lru_cache

# Configure logging
logging.basicConfig(filename='sql_agent.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT', '5432'),
    'database': os.getenv('DATABASE_NAME'),
    'user': os.getenv('DATABASE_USER'),
    'password': os.getenv('DATABASE_PASSWORD')
}

# Connection timeout in seconds (5 minutes)
CONNECTION_TIMEOUT = 300

class ConnectionManager:
    """Manages database connections with timeout-based cleanup"""
    
    def __init__(self, db_config, timeout=CONNECTION_TIMEOUT):
        self.db_config = db_config
        self.timeout = timeout
        self.connection = None
        self.last_used = 0
        self.lock = threading.Lock()
        self.timer = None
    
    def get_connection(self):
        """Get a database connection, creating a new one if needed or if the existing one has timed out"""
        with self.lock:
            current_time = time.time()
            
            # Check if we need to create a new connection
            if self.connection is None:
                logging.info("Creating new database connection")
                self.connection = self._create_connection()
            elif current_time - self.last_used > self.timeout:
                logging.info("Connection timed out, creating new connection")
                self._close_connection()
                self.connection = self._create_connection()
            elif not self._is_connection_alive():
                logging.info("Connection is dead, creating new connection")
                self.connection = self._create_connection()
            
            # Update last used time
            self.last_used = current_time
            
            # Reset the timeout timer
            self._reset_timer()
            
            return self.connection
    
    def _create_connection(self):
        """Create a new database connection"""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
    
    def _is_connection_alive(self):
        """Check if the current connection is still alive"""
        try:
            # Try a simple query to check connection
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logging.error(f"Connection check failed: {str(e)}")
            return False
    
    def _close_connection(self):
        """Close the current connection if it exists"""
        if self.connection is not None:
            try:
                self.connection.close()
                logging.info("Database connection closed due to timeout")
            except Exception as e:
                logging.error(f"Error closing connection: {str(e)}")
            finally:
                self.connection = None
    
    def _reset_timer(self):
        """Reset the timeout timer"""
        if self.timer:
            self.timer.cancel()
        
        self.timer = threading.Timer(self.timeout, self._timeout_callback)
        self.timer.daemon = True
        self.timer.start()
    
    def _timeout_callback(self):
        """Callback when connection times out"""
        with self.lock:
            if self.connection is not None and time.time() - self.last_used > self.timeout:
                self._close_connection()

# Initialize the connection manager
connection_manager = ConnectionManager(DB_CONFIG)

# Query cache with TTL (Time-To-Live)
class QueryCache:
    def __init__(self, max_size=100, ttl=300):  # 5 minutes TTL by default
        self.cache = {}
        self.max_size = max_size
        self.ttl = ttl
        self.lock = threading.Lock()
    
    def get(self, query):
        """Get cached result for a query if it exists and is not expired"""
        with self.lock:
            query_hash = self._hash_query(query)
            if query_hash in self.cache:
                timestamp, result = self.cache[query_hash]
                # Check if cache entry is still valid
                if time.time() - timestamp <= self.ttl:
                    logging.info(f"Cache hit for query: {query[:50]}...")
                    return result
                else:
                    # Remove expired entry
                    del self.cache[query_hash]
            return None
    
    def set(self, query, result):
        """Cache the result of a query"""
        with self.lock:
            # Only cache SELECT queries (read-only)
            if not query.strip().upper().startswith('SELECT'):
                return
                
            query_hash = self._hash_query(query)
            
            # If cache is full, remove oldest entry
            if len(self.cache) >= self.max_size:
                oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][0])
                del self.cache[oldest_key]
            
            self.cache[query_hash] = (time.time(), result)
    
    def _hash_query(self, query):
        """Create a hash of the query for cache key"""
        return hashlib.md5(query.encode()).hexdigest()

# Initialize query cache
query_cache = QueryCache()

# List to store all executed queries
all_queries = []

@function_tool
async def execute_sql(query: str) -> str:
    """
    Execute a SQL query and return the result.
    
    Args:
        query: SQL query to execute
    
    Returns:
        String representation of the query results
    """
    start_time = time.time()
    # Log the query being executed
    logging.info(f"LLM executed SQL query: {query}")

    # Store the query
    global all_queries
    
    # Check if query result is in cache
    cached_result = query_cache.get(query)
    if cached_result is not None:
        execution_time = time.time() - start_time
        all_queries.append((query, execution_time))
        logging.info(f'Cache hit for SQL query: {query} (Execution time: {execution_time:.2f}s)')
        return cached_result
    
    try:
        # Get connection from connection manager
        conn = connection_manager.get_connection()
        
        # Create a cursor with dictionary-like results
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            is_select = query.strip().upper().startswith('SELECT')
            
            # For SELECT queries, prepare the statement to improve performance
            if is_select:
                # Execute the query (psycopg2 handles prepared statements internally)
                cursor.execute(query)
                
                # Fetch all results
                results = cursor.fetchall()
                print("results", results)

                # Convert results to a list of dictionaries
                result_list = [dict(row) for row in results]
                
                # Format the results as a JSON string with indentation for readability
                formatted_results = json.dumps(result_list, indent=2, default=str)
                print("formatted_results", formatted_results)
                execution_time = time.time() - start_time
                all_queries.append((query, execution_time))
                logging.info(f'LLM executed SQL query: {query} (Execution time: {execution_time:.2f}s)')
                
                response = json.dumps({'query_result': formatted_results, 'all_queries': all_queries})
                # Cache the result for future use
                query_cache.set(query, response)
                return response
            else:
                # For non-SELECT queries (INSERT, UPDATE, DELETE)
                cursor.execute(query)
                conn.commit()
                affected_rows = cursor.rowcount
                print("affected_rows", affected_rows)
                execution_time = time.time() - start_time
                all_queries.append((query, execution_time))
                logging.info(f'LLM executed SQL query: {query} (Execution time: {execution_time:.2f}s)')
                return json.dumps({'query_result': f"Query executed successfully. Rows affected: {affected_rows}", 'all_queries': all_queries})
    
    except Exception as e:
        execution_time = time.time() - start_time
        all_queries.append((query, execution_time))
        logging.error(f'Error executing SQL query: {query} (Execution time: {execution_time:.2f}s) - Error: {str(e)}')
        return json.dumps({'query_result': f"Error executing SQL query: {str(e)}", 'all_queries': all_queries})