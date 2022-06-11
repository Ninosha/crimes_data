def delete(client, table_name, column, value):
    query_delete = f"""
        DELETE FROM {table_name} WHERE {column} = '{value}';
    """
    job = client.query(query_delete)
    job.result()
    print("Deleted!")
