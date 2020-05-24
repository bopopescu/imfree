import psycopg2


def user_joined_game(pg_conn, cursor, event_time, event_type, data):
    data = data.split('|||')
    data = [x.strip() for x in data]
    gamecode = data[1]
    user_id = data[3]

    query = "insert into reports.livequiz_gamecode values ({0}, '{1}', '{2}', '{3}')" \
        .format(user_id, gamecode, event_type, event_time)
    try:
        cursor.execute(query)
    except psycopg2.IntegrityError:
        pg_conn.rollback()
