# main.py

import logging
from agents import Agent, Runner
from tools import execute_sql
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('sql_agent.log'), logging.StreamHandler()]
)

# Create an agent with the SQL execution tool
agent = Agent(
    name="SQL Assistant",
    instructions="You are a helpful SQL assistant that can execute queries on a PostgreSQL database. IMPORTANT: Before writing any query, FIRST explore the database structure by executing exploratory queries like 'SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\'' to discover table names, and 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'<discovered_table_name>\'' to learn column details. NEVER use placeholder names like 'my_table'. Always use real table and column names from the database.",
    tools=[execute_sql]
)

# # Test SQL query execution
# def test_sql_agent():
#     # Test with a query to list tables
#     tables_prompt = "Execute this SQL query: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 5;"
#     logging.info(f'Executing SQL query: {tables_prompt}')
#     result = Runner.run_sync(agent, tables_prompt)
#     logging.info('Query executed successfully')
#     print("\nAvailable Tables:")
#     print(result.final_output)

# if __name__ == "__main__":
#     logging.info('Starting SQL execution test...')
#     print("Testing SQL execution through the Agent...")
#     test_sql_agent()
#     logging.info('Test completed')
