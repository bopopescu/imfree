import boto3
import json
import pandas as pd
from datetime import datetime
from utils.db_connect import set_engine


def trivia(process_date):
    resource = boto3.resource('dynamodb')
    table = resource.Table('migames-production-trivias-question-v3')
    response = table.scan()
    engine = set_engine('postgre')
    data = []

    for i in response['Items']:
        serving_dt = datetime.strptime(i['serving_dt'], '%Y-%m-%d').date()
        process_dt = datetime.strptime(process_date, '%Y-%m-%d').date()

        if serving_dt == process_dt:
            content = json.loads(i['content'])
            question = content['text'] if 'text' in content else None
            answer = content['answer'] if 'answer' in content else None
            choices = content['choices'] if 'choices' in content else None
            sequence = i['sequence'] if 'sequence' in i else None
            time_limit = i['time_limit'] if 'time_limit' in i else None
            category = i['category'] if 'category' in i else None
            difficulty = i['difficulty'] if 'difficulty' in i else None
            game_id = i['game_id'] if 'game_id' in i else None

            data.append([serving_dt, game_id, sequence, difficulty, category, question, choices, answer, time_limit])

    df = pd.DataFrame(data, columns=['event_date', 'game_id', 'sequence_id', 'difficulty', 'category', 'question_text',
                                     'choices', 'correct_answer', 'time_limit'])
    df.to_sql('pakquiz_questions', engine, schema='reports', if_exists='append', index=False)


if __name__ == '__main__':
    trivia('2019-08-04')
