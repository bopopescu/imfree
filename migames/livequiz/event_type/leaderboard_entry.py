import psycopg2


def leaderboard_entry(pg_conn, cursor, event_time, event_type, data):
    data = data.split('|||')
    data = [x.strip() for x in data]
    gamecode = data[1]
    user_id = data[3]
    timespent = data[5]
    score = data[7]
    prize = data[9]

    query = "insert into reports.livequiz_leaderboards values({0}, '{1}', '{2}', '{3}', {4}, {5}, {6})" \
        .format(user_id, gamecode, event_type, event_time, timespent, score, prize)
    try:
        cursor.execute(query)
    except psycopg2.IntegrityError:
        pg_conn.rollback()
