import pyodbc

def get_db_connection():
    conn_str = (
        "Driver={SQL Server};"
        "Server=ACADEMY06;"
        "Database=KafkaMessageJson;"
        "Trusted_Connection=yes;"
        "Auto_Commit=true;"
    )
    return pyodbc.connect(conn_str)

def execute_insert_query(cursor, query, json_data, processing_time, entity_name, entity_id):
    cursor.execute(query, json_data, processing_time, entity_name, entity_id)
    