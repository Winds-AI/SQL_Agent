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
import re

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

class DatabaseSchemaMemory:
    def __init__(self):
        self.schema = {}
        self.load_schema()
        
    def load_schema(self):
        try:
            with open('schema_memory.json', 'r') as f:
                self.schema = json.load(f)
            db_logger.info("Loaded database schema from memory")
        except FileNotFoundError:
            db_logger.warning("No existing schema memory found")

    def save_schema(self):
        with open('schema_memory.json', 'w') as f:
            json.dump(self.schema, f, indent=2)
        db_logger.info("Saved database schema to memory")

    def update_from_query(self, query, result=None):
        try:
            query = query.lower()
            
            if 'create table' in query:
                table_name = query.split('create table')[-1].split('(')[0].strip()
                columns = re.findall(r'(\w+)\s+([\w\(\)]+)', query.split('(', 1)[-1].split(')')[0])
                self.schema[table_name] = {
                    'columns': {col[0]: col[1] for col in columns},
                    'relationships': []
                }
                db_logger.info(f"Updated schema with new table: {table_name}")

            elif 'alter table' in query:
                table_name = query.split('alter table')[-1].split('add')[0].strip()
                if 'add column' in query:
                    column_def = query.split('add column')[-1].strip()
                    col_name, col_type = re.match(r'(\w+)\s+([\w\(\)]+)', column_def).groups()
                    self.schema[table_name]['columns'][col_name] = col_type
                    db_logger.info(f"Added column {col_name} to {table_name}")

            elif result and 'select' in query:
                try:
                    data = json.loads(result)
                    if 'query_result' in data and isinstance(data['query_result'], list):
                        first_row = data['query_result'][0]
                        table_name = query.split('from')[-1].split()[0].strip()
                        if table_name not in self.schema:
                            self.schema[table_name] = {
                                'columns': {k: 'TEXT' for k in first_row.keys()},
                                'relationships': []
                            }
                            db_logger.info(f"Inferred schema for {table_name} from query results")
                except Exception as e:
                    db_logger.error(f"Schema inference error: {str(e)}")

        except Exception as e:
            db_logger.error(f"Schema update failed: {str(e)}")
        finally:
            self.save_schema()

# Initialize memory system
schema_memory = DatabaseSchemaMemory()

@function_tool
async def execute_sql(query: str) -> str:
    global all_queries
    llm = LLM()
    llm.start()
    start_time = time.time()
    try:
        conn = connection_manager.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            
            schema_memory.update_from_query(query)
            
            if query.strip().lower().startswith('select'):
                results = cursor.fetchall()
                formatted_results = [dict(row) for row in results]
                schema_memory.update_from_query(query, json.dumps({'query_result': formatted_results}))
                response = json.dumps({'query_result': formatted_results, 
                                     'schema': schema_memory.schema,
                                     'all_queries': all_queries})
            else:
                affected_rows = cursor.rowcount
                response = json.dumps({'query_result': f"Rows affected: {affected_rows}",
                                     'schema': schema_memory.schema,
                                     'all_queries': all_queries})
                
            all_queries.append((query, time.time() - start_time))
            return response
            
    except Exception as e:
        db_logger.error(f"Query execution failed: {str(e)}")
        return json.dumps({'error': str(e), 'schema': schema_memory.schema})
    finally:
        llm.end()