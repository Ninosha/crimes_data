def read(client, table_id):

    query_string = f"""SELECT * FROM {table_id}"""

    dataframe = (
        client.query(query_string)
        .result()
        .to_dataframe()
    )
    data = dataframe.head()

    return data.to_json()
