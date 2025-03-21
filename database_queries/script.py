# main.py

from agents import Agent, Runner
from tools import execute_sql
from dotenv import load_dotenv
load_dotenv()

# Create an agent with the SQL execution tool
agent = Agent(
    name="SQL Assistant",
    instructions="You are a helpful assistant that can execute SQL queries on a PostgreSQL database. You can also write SQL queries to get information from the database.",
    tools=[execute_sql]
)

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql_script(agent, script):
    return Runner.run_sync(agent, f"Execute this SQL script: {script}")

# Test SQL query execution
def test_sql_agent():
    # Create tables
    create_script = read_sql_file('database_queries/create.sql')
    execute_sql_script(agent, create_script)
    
    # Insert data
    insert_script = read_sql_file('database_queries/insert.sql')
    execute_sql_script(agent, insert_script)
    
    # Test with a query to list tables
    tables_prompt = "Execute this SQL query: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 5;"
    result = Runner.run_sync(agent, tables_prompt)
    print("\nAvailable Tables:")
    print(result.final_output)

if __name__ == "__main__":
    print("Testing SQL execution through the Agent...")
    test_sql_agent()
