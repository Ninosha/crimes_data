import logging


def table_insert_rows(client, table_id, rows_to_insert):
    # rows_to_insert = [
    #     {u"full_name": u"Phred Phlyntstone", u"age": 32},
    #     {u"full_name": u"Wylma Phlyntstone", u"age": 29},
    # ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        logging.info("New rows have been added.")
    else:
        logging.error(
            "Encountered errors while inserting rows: {}".format(errors)
        )


def create_update_table(project, table_name, df):
    df.to_gbq(
        destination_table=table_name,
        project_id=project,
        if_exists="replace",
    )


def create_view_table(client, table_id, view_dataset, cols):
    view_table = table_id.replace(table_id.split(".")[1], view_dataset)
    # cols = ["unique_key", "case_number", "date",
    #         "block", "description", "location_description",
    #         "arrest", "location"]

    # sheamoowmeb requestshi tu aris none tu None aris mashin custom
    # columns environemntidan vadzzlevt pirdapir tu requestshi aris
    # columns sheudzlia iyos list/tuple, tua list/tuple vajoinebt

    sql = f"""CREATE VIEW {view_table} AS
        SELECT 
        {cols}
        FROM {table_id}; """

    query_job = client.query(sql)
    query_job.result()
