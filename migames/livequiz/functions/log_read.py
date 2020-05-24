import operator
from utils.db_connect import connect
from migames.livequiz.functions.json_read import json_categ
from migames.livequiz.event_type import user_joined_game, user_voted, leaderboard_entry, user_answered

pg_conn = connect('postgre')
cursor = pg_conn.cursor()


def log_read(logfile, jsonfile):
    json_data = json_categ(jsonfile)
    data = {}
    for k, v in json_data.items():
        if k != 'gamecode':
            data[k] = 0

    with open(logfile, encoding='utf8') as f:
        for line in f:
            if "event_type" in line and line.startswith("20"):
                line = line.replace("\'", "\'\'")
                line = line.split('[info]')
                left_info = line[0].split(' ')
                event_type = left_info[3].split('=')[1]
                event_type = event_type.strip()
                event_time = left_info[0] + ' ' + left_info[1]

                if event_type == "user_joined_game":
                    user_joined_game.user_joined_game(pg_conn, cursor, event_time, event_type, line[1])
                elif event_type == "user_voted":
                    vote = user_voted.user_voted(pg_conn, cursor, event_time, event_type, line[1], jsonfile)
                    data[vote] += 1
                elif event_type == "leaderboard_entry":
                    leaderboard_entry.leaderboard_entry(pg_conn, cursor, event_time, event_type, line[1])
                elif event_type == "user_answered":
                    voted_category = max(data.items(), key=operator.itemgetter(1))[0]
                    user_answered.user_answered(pg_conn, cursor, event_time, event_type, line[1],
                                                json_data[voted_category], jsonfile)

    pg_conn.commit()
