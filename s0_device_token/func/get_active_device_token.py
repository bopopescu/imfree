import pandas as pd
from utils.db_connect import connect


def gen_data(start_date):
    conn = connect('mssql')
    query = """
            select
                distinct
                dev.device_token
            from app_user_impression imp
            left join app_user au
                on imp.app_user_id = au.app_user_id
            left join device dev
                on imp.app_user_id = dev.app_user_id
                and dev.device_is_active = 1
            where 
                cast(imp.app_user_impression_date as date) > '{0}'
                and au.fraud = 0
                and dev.device_token is not null
            """.format(start_date)
    data = pd.read_sql_query(query, conn)

    return data


def generate_file(path, process_date):
    data_list = []
    data = gen_data(process_date)
    for key, value in data.iterrows():
        data_list.append(value['device_token'])

    def splitter(lines, n):
        for i in range(0, len(lines), n):
            yield lines[i: i + n]

    for index, line in enumerate(splitter(data_list, 1000)):
        with open(path + "device_token_" + str(index) + '.txt', 'w+') as f:
            f.write(str(line))
