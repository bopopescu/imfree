import psycopg2
from migames.livequiz.functions.json_read import json_categ


def user_voted(pg_conn, cursor, event_time, event_type, data, jsonfile):
    data = data.split('|||')
    data = [x.strip() for x in data]
    gamecode = data[1]
    user_id = data[3]
    category_id = int(data[5])

    data_new = {}
    data = json_categ(jsonfile)
    for k, v in data.items():
        if k != 'gamecode':
            data_new[v] = k

    query = "insert into reports.livequiz_voted_category values({0}, '{1}', '{2}', '{3}', {4}, '{5}')" \
        .format(user_id, gamecode, event_type, event_time, category_id, data_new[category_id])
    try:
        cursor.execute(query)
    except psycopg2.IntegrityError:
        pg_conn.rollback()

    return data_new[category_id]
