# app.py

import streamlit as st
from main import agent, Runner
import asyncio
from tools import all_queries, connection_manager
import time

st.title('SQL Assistant')

# Initialize the database connection when the app starts
if 'connection_initialized' not in st.session_state:
    # Get a connection to initialize it
    connection_manager.get_connection()
    st.session_state.connection_initialized = True
    st.sidebar.success("Database connection initialized")

# Input for SQL query
query = st.text_input("Enter your query here...", placeholder="how many tables do i have")

if st.button('Execute'):
    start_time = time.time()
    if query.strip():
        try:
            # Clear previous queries
            all_queries.clear()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = Runner.run_sync(agent, f"{query}")
            st.write('Query Result:')
            st.write(result.final_output)
        except Exception as e:
            st.error(f'Error executing query: {str(e)}')
    else:
        st.warning('Please enter a SQL query')
    total_time = time.time() - start_time
    st.write(f'Total execution time: {total_time:.2f} seconds')

with st.sidebar:
    st.write('All Queries:')
    for query, exec_time in all_queries:
        st.write(f'Query: {query}')
        st.write(f'Execution time: {exec_time:.2f} seconds')
        st.write('---')
