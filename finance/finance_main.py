import os
import json
import xlsxwriter
import pandas as pd
from pathlib import Path
from finance.utils.gen_unredeemed import accumulated_unredeemed
from finance.utils.excel_func import write_data, add_headers
from datetime import date
from utils.db_connect import connect

# Declare variables
json_dict = {}
today = date.today()
report_date = date(2019, 1, 1)

# List of sql files and json files
mssql_scripts_dir = os.path.join(os.getcwd(), "queries")
sql_files = list(Path(mssql_scripts_dir).rglob("*.[sS][qQ][lL]"))
json_files = list(Path(mssql_scripts_dir).rglob("*.json"))

# Create an excel file named 'finance YYYY-MM-DD.xlsx'
workbook = xlsxwriter.Workbook('C:\\Users\\DK\\Documents\\finance ' + str(today) + '.xlsx')

# Add headers to excel file
add_headers(workbook, "C:\\Users\\DK\\PycharmProjects\\imfree\\configs\\headers.json")

# Create connection to DB
conn = connect('postgre')

# Read json files and store data in dictionary
for json_file in json_files:
    f = open(json_file, 'r')
    json_data = json.load(f)

    json_dict[json_data['name']] = json_data['sheet'] + '|' + str(json_data['row']) + '|' + str(json_data['col'])

# Open SQL files for processing
for sql_file in sql_files:
    req_name = str(sql_file).split('\\')[-1].split('.')[0]
    json_arr = json_dict[req_name].split('|')
    sheet = json_arr[0]
    row = int(json_arr[1])
    col = int(json_arr[2])

    # Create excel sheet if sheet does not exist
    if sheet not in workbook.sheetnames:
        worksheet = workbook.add_worksheet(sheet)

    # Set actively working excel sheet
    active_ws = workbook.get_worksheet_by_name(sheet)

    f = open(sql_file, 'r')
    pg_query = f.read()

    # req_1-2 results are from utils.gen_unredeemed
    # the rest comes from sql files
    if req_name != 'req_1-2':
        pg_data = pd.read_sql_query(pg_query, conn)
    else:
        pg_data = accumulated_unredeemed()

    # Apply logic based on req version
    for k, v in pg_data.iterrows():
        if req_name == 'req_1-1':
            write_data(active_ws, row, col, str(v['dt']), v['reward'], v['redeemed'])
            row += 1

        elif req_name == 'req_1-2':
            if k >= report_date:
                for k1, v1 in v.items():
                    write_data(active_ws, row, col, v1)
                    row += 1

        elif req_name == 'req_2-1':
            write_data(active_ws, row, col,
                       str(int(v['year'])), v['month'], v['redemption_channel'],
                       str(v['redemption']))
            row += 1

        elif req_name == 'req_2-2':
            write_data(active_ws, row, col,
                       str(int(v['year'])), v['month'], v['transaction_name'],
                       str(v['amount_earned']), str(v['amount_redeemed']),
                       str(int(v['count_earned'])), str(int(v['count_redeemed'])))
            row += 1

        elif req_name == 'req_3':
            write_data(active_ws, row, col,
                       str(int(v['year'])), v['month'],
                       str(int(v['mgm_no_of_users'])), str(v['mgm']),
                       str(int(v['reg_no_of_users'])), str(v['reg']),
                       str(int(v['convo_no_of_users'])), str(v['convo']),
                       str(int(v['migames_no_of_users'])), str(v['migames']))
            row += 1

    f.close()
workbook.close()
