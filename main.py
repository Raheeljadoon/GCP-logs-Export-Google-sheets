from datetime import timezone
from google.cloud import logging
from datetime import datetime, timedelta
import pandas as pd
import google.auth
import gspread
import re


def get_time(time):
    """
    convert seconds to str time for used in google log filtration.
    :param time: times in seconds
    :return: Str(time)
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(seconds=time)

    start_time = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return start_time, end_time


def update_google_sheet(results):
    """
    Upload dataframe to google sheets
    :param results: log_dataframe
    :return: True
    """
    gc = gspread.service_account(filename="cred.json")
    sh = gc.open('cloud_sql_logs').sheet1
    sh.update([results.columns.values.tolist()] + results.values.tolist())
    return True


def match_table_name(log_detail):
    pattern = r"(update|delete|into)\s+([\w\.]+)"
    return re.search(pattern, log_detail, re.IGNORECASE)


def replace_parameter(each_log):
    """
    takes a list of logs as input, processes each log and replaces parameters
     in the first log with their respective values.
    :type each_log: object
    """
    log2_values = each_log[1].split("parameters: ")[1].split(", ")
    log2_dict = {}
    for value in log2_values:
        key, val = value.split(" = ")
        log2_dict[key] = val
    for key, val in log2_dict.items():
        each_log[0] = each_log[0].replace(key, val)
    return ''.join(each_log[0].split(':')[5:])


def get_gce_operation(log_detail):
    """
    "Determines the operation ('INSERT', 'UPDATE', or 'DELETE') in the provided log detail string."
    :param log_detail: str(log)
    :return: operation
    """
    if 'INSERT' in log_detail:
        operation = 'INSERT'
    elif 'UPDATE' in log_detail:
        operation = 'UPDATE'
    else:
        operation = 'DELETE'

    return operation


def get_operation_and_table(each_log, log_data, log1):
    """
     Parses a log entry for 'INSERT' or 'UPDATE' operations and table names, adds the operation, table name,
     and query to a dictionary, and appends it to the log data list.
    :param each_log: str(log)
    :param log_data: list(log)
    :param log1: cleaned(log)
    :return: List(log_data)
    """
    if 'INSERT' in each_log[0] or 'UPDATE' in each_log[0]:

        log_dict = {'sql_operation': '', 'sql_table_name': '', 'DataFlow_Logs': each_log[0].split(':')[5]}
        # table = log_dict['query'].split()
        log_dict['sql_operation'] = log_dict['DataFlow_Logs'].split()[0]
        match = match_table_name(log_dict['DataFlow_Logs'])
        if match:
            log_dict['sql_table_name'] = match.group(2)
        log_dict['DataFlow_Logs'] = log1
        log_data.append(log_dict)
    return log_data


def get_cloudsql_logs():
    """
    get logs for Google cloud sql and then convert these logs into pandas dataframe based on 'Query', TableName and
     operations
     Query -> It will filter logs which we need like i
    :return: Logs Dataframe
    """
    try:
        print("yes-----------------------------------")
        credentials, project = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        client = logging.Client(credentials=credentials)
        start_time, end_time = get_time(7)

        filter_query = f'resource.labels.database_id="next-best-action-nonprod:nba-api-db" AND ' \
                       f'(textPayload=~"user=eligibility-loader" OR ": INSERT INTO" OR "DETAIL: parameters: " OR' \
                       f' ": UPDATE" OR ": DELETE") (textPayload!~"COMMIT" AND NOT "BEGIN" AND NOT "disconnection" ' \
                       f'AND NOT "connection authorized" ) '\
                       f'logName="projects/next-best-action-nonprod`/logs/cloudsql.googleapis.com%2Fpostgres.log" ' \
                       f'AND timestamp >= "{start_time}" AND timestamp < "{end_time}"'

        results = get_result_from_cloud_sql(client, filter_query)
        print(results)
        return results
    except Exception as er:
        # print(f'Error Occurred -> {str(er)}')
        pass


def get_result_from_cloud_sql(client, filter_query):
    """
    Fetches logs from CloudSQL using provided client and filter query, parses logs for
     'INSERT', 'UPDATE', and 'DELETE' operations and table names.
    :param client: logging_client
    :param filter_query: query_for_log_filtration
    :return: Log_Dataframe
    """
    log_data = []
    chunk_size = 2
    results = client.list_entries(filter_=filter_query)

    filter_result = [each_log.to_api_repr().get('textPayload') for each_log in results if 'INSERT'
                     in each_log.to_api_repr().get('textPayload') or
                     'UPDATE' in each_log.to_api_repr().get('textPayload') or
                     'DELETE' in each_log.to_api_repr().get('textPayload') or
                     'parameters' in each_log.to_api_repr().get('textPayload')]

    list_chunked = [filter_result[i:i + chunk_size] for i in range(0, len(filter_result), chunk_size)]
    for each_log in list_chunked:
        try:
            each_log = sorted(each_log, reverse=False)
            log1 = replace_parameter(each_log)
            log_data = get_operation_and_table(each_log, log_data, log1)

        except Exception as er:
            # print(f'error Occurred -> {str(er)}')
            continue

    return pd.DataFrame(log_data)


def compute_inst_logs():
    """
    Function which will get postgres logs from GCP compute engine
    :return: Logs Dataframe
    """
    try:
        client = logging.Client()
        start_time, end_time = get_time(60)

        filter_query = f'resource.type="gce_instance"  AND resource.labels.instance_id="2521144165253397181" ' \
                       f'AND severity= "NOTICE" AND timestamp >= "{start_time}" AND timestamp < "{end_time}"'

        results = get_result_from_compute_engine(client, filter_query)
        print(results)
        return results
    except Exception as er:
        # print(f'Error Occurred -> {str(er)}')
        pass


def get_result_from_compute_engine(client, filter_query):
    """
    Fetches logs from Compute Engine using provided client and filter query, parses logs for
    'INSERT', 'UPDATE' or 'DELETE' operations and table names.
    :param client: logging_client
    :param filter_query: log_filter_query
    :return: Log_Dataframe
    """
    results = client.list_entries(filter_=filter_query)
    log_data = []

    for each_log in results:
        log_dict = dict()
        try:
            each_log = each_log.to_api_repr()
            log_detail = each_log.get('textPayload')
            if 'INSERT' in log_detail or 'UPDATE' in log_detail or 'DELETE' in log_detail:

                log_dict['gce_operation'] = get_gce_operation(log_detail)
                match = match_table_name(log_detail)
                if match:
                    table_name = match.group(2)
                    log_dict['gce_table_name'] = table_name

                text = log_detail.strip()
                new_text = " ".join(text.split())
                final_text = new_text.replace('b"', '')
                log_dict['Python_Project_Logs'] = final_text
                log_data.append(log_dict)
        except Exception as er:
            # print(f'Error Occurred -> {str(er)}')
            continue

    return pd.DataFrame(log_data)


def get_cloud_sql_and_compute_engine_logs() -> True:  # main function
    """
    "Retrieves CloudSQL and Compute Engine logs, merges them, fills null values with "Null",
     and updates the Google Sheet
    :return: True
    """
    try:
        cloud_sql_logs_df = get_cloudsql_logs()
        compute_engine_logs_df = compute_inst_logs()

        final_df = pd.concat([cloud_sql_logs_df, compute_engine_logs_df], axis=1)

        after_drop_df = final_df.dropna(axis=0).reset_index(drop=True)
        after_compare_df = after_drop_df[(after_drop_df['sql_operation'] == after_drop_df['gce_operation']) &
                                         (after_drop_df['sql_table_name'] == after_drop_df['gce_table_name'])]

        upload_df = after_compare_df.drop(columns=['sql_operation', 'sql_table_name', 'gce_operation',
                                                   'gce_table_name'])
        print(upload_df)
        update_google_sheet(upload_df)
        return True
    except Exception as er:
        # print(f'Error Occurred -> {str(er)}')
        pass


get_cloud_sql_and_compute_engine_logs()
