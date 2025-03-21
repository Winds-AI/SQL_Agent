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


# Configure logging
logging.basicConfig(filename='sql_agent.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create a specific logger for database connections
db_logger = logging.getLogger('database_connection')
db_logger.setLevel(logging.DEBUG)

# Create file handler for connection logs
conn_handler = logging.FileHandler('database_connection.log')
conn_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handler
conn_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [CONNECTION] - %(message)s')
conn_handler.setFormatter(conn_formatter)

# Add the handler to the logger
db_logger.addHandler(conn_handler)

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
            idle_time = current_time - self.last_used if self.last_used > 0 else 0
            
            # Check if we need to create a new connection
            if self.connection is None:
                db_logger.info(f"Creating new database connection - No existing connection")
                self.connection = self._create_connection()
                db_logger.info(f"Connection established to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']} as user {DB_CONFIG['user']}")
            elif current_time - self.last_used > self.timeout:
                db_logger.info(f"Connection timed out after {idle_time:.2f}s of inactivity (timeout: {self.timeout}s)")
                self._close_connection()
                self.connection = self._create_connection()
                db_logger.info(f"New connection established after timeout")
            elif not self._is_connection_alive():
                db_logger.warning(f"Connection is dead after {idle_time:.2f}s of inactivity, creating new connection")
                self.connection = self._create_connection()
                db_logger.info(f"New connection established after dead connection detection")
            else:
                db_logger.debug(f"Reusing existing connection (idle for {idle_time:.2f}s)")
            
            # Update last used time
            self.last_used = current_time
            
            # Reset the timeout timer
            self._reset_timer()
            
            return self.connection
    
    def _create_connection(self):
        """Create a new database connection"""
        start_time = time.time()
        db_logger.info(f"Attempting to connect to database {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            conn_time = time.time() - start_time
            db_logger.info(f"Connection established successfully in {conn_time:.4f}s")
            return conn
        except Exception as e:
            db_logger.error(f"Connection failed: {str(e)}")
            raise
    
    def _is_connection_alive(self):
        """Check if the current connection is still alive"""
        try:
            # Try a simple query to check connection
            start_time = time.time()
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                check_time = time.time() - start_time
                db_logger.debug(f"Connection health check successful in {check_time:.4f}s")
                return True
        except Exception as e:
            db_logger.error(f"Connection health check failed: {str(e)}")
            return False
    
    def _close_connection(self):
        """Close the current connection if it exists"""
        if self.connection is not None:
            try:
                conn_id = id(self.connection)
                self.connection.close()
                db_logger.info(f"Database connection (id: {conn_id}) closed after {time.time() - self.last_used:.2f}s of inactivity")
            except Exception as e:
                db_logger.error(f"Error closing connection: {str(e)}")
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
            if self.connection is not None:
                idle_time = time.time() - self.last_used
                if idle_time > self.timeout:
                    db_logger.info(f"Timeout callback triggered after {idle_time:.2f}s of inactivity")
                    self._close_connection()
                else:
                    db_logger.debug(f"Timeout callback checked connection (idle for {idle_time:.2f}s)")


class LLM:
    def __init__(self):
        self.start_time = None

    def start(self):
        self.start_time = time.time()
        db_logger.info("LLM operation started")

    def end(self):
        if self.start_time:
            duration = time.time() - self.start_time
            db_logger.info(f"LLM operation completed in {duration:.2f} seconds")
        else:
            db_logger.error("LLM operation end called without start")


# Initialize the connection manager
connection_manager = ConnectionManager(DB_CONFIG)



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
    llm = LLM()
    llm.start()
    start_time = time.time()
    # Log the query being executed
    logging.info(f"LLM executed SQL query: {query}")
    db_logger.info(f"Executing SQL query: {query}")

    # Store the query
    global all_queries
    

    
    try:
        # Get connection from connection manager
        conn = connection_manager.get_connection()
        
        # Create a cursor with dictionary-like results
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Execute the query
            cursor.execute(query)

            print("cursor description", cursor.description)            
            # Check if the query is a SELECT query (returns results)
            if cursor.description:
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
                llm.end()
                return json.dumps({'query_result': formatted_results, 'all_queries': all_queries})
            else:
                # For non-SELECT queries (INSERT, UPDATE, DELETE)
                conn.commit()
                affected_rows = cursor.rowcount
                print("affected_rows", affected_rows)
                execution_time = time.time() - start_time
                all_queries.append((query, execution_time))
                logging.info(f'LLM executed SQL query: {query} (Execution time: {execution_time:.2f}s)')
                llm.end()
                return json.dumps({'query_result': f"Query executed successfully. Rows affected: {affected_rows}", 'all_queries': all_queries})
    
    except Exception as e:
        execution_time = time.time() - start_time
        all_queries.append((query, execution_time))
        logging.error(f'Error executing SQL query: {query} (Execution time: {execution_time:.2f}s) - Error: {str(e)}')
        llm.end()
        return json.dumps({'query_result': f"Error executing SQL query: {str(e)}", 'all_queries': all_queries})