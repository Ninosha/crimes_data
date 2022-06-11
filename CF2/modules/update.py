import json

sl = '{"column": "value", "column1": "value"}'

sl.replace("{", "")
print(sl)

loaded = json.loads(sl)


# print(loaded)
# string = ""
# try:
#
#     for key in loaded.keys():
#         a = f"{key} = {loaded[key]}, "
#         string += a
# finally:
#     string = string.replace(string[-2:], "")
#     print(string)
# print(string)

def update(client, column_name, value, table_name):
    sql_all = f"""UPDATE {table_name} SET {column_name} = '{value}' WHERE 1=1;"""
    # columns = data.keys()
    # values = data.values()
    # update_statement = f'UPDATE {table_id} SET ({", ".join(columns)}) = ({", ".join(values)})'

    # {"string_filed_1": "sj"}
    query_job = client.query(sql_all)
    query_job.result()
