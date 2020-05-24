import psycopg2
import base64
from migames.livequiz.functions.json_read import json_qna


def user_answered(pg_conn, cursor, event_time, event_type, data, voted_category, jsonfile):
    data = data.split('|||')
    data = [x.strip() for x in data]
    gamecode = data[1]
    user_id = data[3]
    question_id = int(data[5])
    selected_answer = data[7]
    selected_answer_text = data[9]
    timespent = data[11]
    answer_label = str(base64.b64decode(selected_answer_text)[3:])[2:-1]
    answer_label = answer_label.replace("\'", "\'\'")

    ref = json_qna(jsonfile, voted_category)
    correct_answer = ref[1][question_id]
    correct_answer = correct_answer.replace("\'", "\'\'")
    is_correct = 1 if answer_label.strip() == correct_answer else 0
    question_label = ref[2][question_id]
    question_label = question_label.replace("\'", "\'\'")

    query = "insert into reports.livequiz_user_response " \
            "values({0}, '{1}', '{2}', '{3}', {4}, '{5}', '{6}', '{7}', {8}, {9})" \
        .format(user_id, gamecode, event_type, event_time, question_id, question_label,
                selected_answer, answer_label, is_correct, timespent)
    try:
        cursor.execute(query)
    except psycopg2.IntegrityError:
        pg_conn.rollback()
