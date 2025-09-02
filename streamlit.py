import os
import pandas as pd
from databricks import sql

def connect_to_databricks(server_hostname, http_path, access_token):

    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        print("Successfully connected to Databricks!")
        return connection
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def query_databricks_to_dataframe(connection, sql_query):
    """
    Execute a SQL query on Databricks and return results as pandas DataFrame
    
    Args:
        connection: Databricks connection object
        sql_query (str): SQL query to execute
    
    Returns:
        pandas.DataFrame: Query results as DataFrame
    """
    if connection is None:
        print("No connection available")
        return pd.DataFrame()
    
    try:
        cursor = connection.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=column_names)
        
        cursor.close()
        return df
        
    except DatabaseError as e:
        print(f"Query execution failed: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error: {e}")
        return pd.DataFrame()
    

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Databricks Data Viewer", layout="wide")
st.title("Databricks Data Viewer")
st.sidebar.header("Connection Settings")

server_hostname = st.sidebar.text_input("Server Hostname", "xxx.cloud.databricks.com")
http_path = st.sidebar.text_input("HTTP Path")
access_token = st.sidebar.text_input("Access Token", type="password")
sql_query = st.sidebar.text_area("SQL Query", height=100, 
                                value="SELECT * FROM your_table LIMIT 1000")

# Connect and query button
if st.sidebar.button("Run Query"):
    if not all([server_hostname, http_path, access_token, sql_query]):
        st.sidebar.error("Please fill all connection parameters")
    else:
        with st.spinner("Connecting to Databricks..."):
            connection = connect_to_databricks(server_hostname, http_path, access_token)
            
        if connection:
            with st.spinner("Executing query..."):
                df = query_databricks_to_dataframe(connection, sql_query)
                
            if not df.empty:
                st.success(f"Query executed successfully! Retrieved {len(df)} rows.")
                
                
                st.subheader("Query Results")
                st.dataframe(df)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Rows", len(df))
                with col2:
                    st.metric("Total Columns", len(df.columns))
                with col3:
                    st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name="databricks_data.csv",
                    mime="text/csv",
                )
            else:
                st.error("No data returned from query or query failed.")
            
            connection.close()
        else:
            st.error("Failed to connect to Databricks. Please check your credentials.")

if st.button("Load Example Data"):
    connection = connect_to_databricks(
        server_hostname="databricksserver",
        http_path="clusterpath",
        access_token="accesstoken"
    )
    df = query_databricks_to_dataframe(connection, "SELECT * FROM aliworkspace.calendly_silver.cost_per_booking_by_channel LIMIT 10")
    st.dataframe(df)
    connection.close()