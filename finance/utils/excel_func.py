import pandas as pd
import json


def add_headers(workbook, header_file):
    f = open(header_file, 'r')
    json_data = json.load(f)

    for i in json_data['sheets']:
        sheet = i['sheet_name']
        worksheet = workbook.add_worksheet(sheet)
        for header in i['headers']:
            row = header['row']
            col = header['col']
            for header_name in header['header_names']:
                worksheet.write(row, col, header_name)
                col += 1


def write_data(worksheet, row, col, *argv):
    c = col
    for k in argv:
        worksheet.write(row, c, k)
        c += 1


def read_excel(worksheet, sheet):
    df = pd.read_excel(worksheet, sheet_name=sheet)

    my_dict = {}
    for k, v in df.iterrows():
        my_dict[k] = v

    return my_dict
