import os
import xlsxwriter
import pandas as pd
from datetime import date
from utils.db_connect import connect
from finance.utils.excel_func import read_excel, add_headers
from campaign.bgc_qsr.utils.df_func import qsr_excel_to_dict, qsr_valid_user_details

conn = connect('sqlserver')
today = date.today()
main_dict = {}


def generate_winners(source_file, target_file, main_dict_answer):
    # Target worksheet preparation
    workbook = xlsxwriter.Workbook(target_file)
    json_path = os.getcwd() + '\\configs' + '\\headers.json'
    add_headers(workbook, json_path)
    worksheet1 = workbook.get_worksheet_by_name("app_user_id")
    worksheet2 = workbook.get_worksheet_by_name("app_user_details")

    # Create dataframes from source file
    df_app = read_excel(source_file, 'app')
    df_fb = read_excel(source_file, 'fb')

    # Generate dictionary of users with correct answer
    user = qsr_excel_to_dict(df_app, main_dict_answer, 'Guess the BGC restaurant.', 'aid', 'app')
    i = 'Thanks for answering. For us to verify your account in the Imfree app, can you tell us your mobile number?'
    user.update(qsr_excel_to_dict(df_fb, main_dict_answer, 'Guess the BGC restaurant.', i, 'fb'))

    for k, v in user.items():
        # Open sql files based on data source (APP/FB)
        # Execute query then store outputs on main_dict
        if v == 'app':
            fd = open(os.getcwd() + '\\query' + '\\app_query.sql', 'r')
            app_query = fd.read()
            main_dict_app = qsr_valid_user_details(pd.read_sql_query(app_query.format(k), conn))
            for k1, v1 in main_dict_app.items():
                main_dict[k1] = v1

        elif v == 'fb':
            fd = open(os.getcwd() + '\\query' + '\\fb_query.sql', 'r')
            fb_query = fd.read()
            main_dict_fb = qsr_valid_user_details(pd.read_sql_query(fb_query.format(k), conn))
            for k1, v1 in main_dict_fb.items():
                main_dict[k1] = v1

    # Write files to excel
    row = 1
    col = 0
    for k in sorted(main_dict.keys()):
        v_list = main_dict[k].split('|')
        worksheet1.write(row, col, k)
        worksheet2.write(row, col, k)
        worksheet2.write(row, col + 1, v_list[0])
        worksheet2.write(row, col + 2, v_list[1])
        worksheet2.write(row, col + 3, v_list[2])
        worksheet2.write(row, col + 4, v_list[3])
        row += 1

    workbook.close()
