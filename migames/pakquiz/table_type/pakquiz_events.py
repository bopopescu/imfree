import json
import boto3
import pandas as pd
from datetime import datetime, date
from utils.db_connect import set_engine
from boto3.dynamodb.conditions import Key
from utils.date_func import daterange, epoch_sec

today = date.today()


def event(process_date):
    resource = boto3.resource('dynamodb')
    table = resource.Table('migames-production-events-v3')
    engine = set_engine('postgre')
    process_dt = datetime.strptime(process_date, '%Y-%m-%d').date()
    df = pd.DataFrame(columns=['event_id', 'event_date', 'event_type', 'question_id', 'choices', 'answer', 'time_start',
                               'time_end', 'time_spent', 'tstamp', 'batch_id', 'game_id', 'player_id'])

    for single_date in daterange(process_dt, today):
        epoch_time = epoch_sec(single_date)

        response = table.scan(
            FilterExpression=Key('event_date').between(int(epoch_time[0]), int(epoch_time[1]))
        )

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'],
                FilterExpression=Key('event_date').between(int(epoch_time[0]), int(epoch_time[1]))
            )

            for i in response['Items']:
                event_date = datetime.fromtimestamp(i['event_date'])
                event_id = i['event_id'] if 'event_id' in i else None
                event_type = i['event_type'] if 'event_type' in i else None
                game_id = i['game_id'] if 'game_id' in i else None
                player_id = i['player_id'] if 'player_id' in i else None
                event_json = json.loads(i['event_json'])
                event_list = ['EVENT_GAME_OPENED', 'EVENT_GAME_STARTED', 'EVENT_GAME_PAUSED', 'EVENT_GAME_RESUMED',
                              'EVENT_GAME_CANCELLED', 'EVENT_GET_QUESTION']

                if event_type in event_list:
                    batch_id = event_json['batchId']
                    epoch_t = event_json['timestamp']
                    timestamp = datetime.fromtimestamp(epoch_t / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    data = [[event_id, event_date, event_type, timestamp, batch_id, game_id, player_id]]
                    df_new = pd.DataFrame(data, columns=['event_id', 'event_date', 'event_type', 'tstamp', 'batch_id',
                                                         'game_id', 'player_id'])
                    df = df.append(df_new, ignore_index=True, sort=False)

                elif event_type == 'EVENT_ANSWER_QUESTION':
                    question_id = event_json['question_id'] if 'question_id' in event_json else None
                    choices = event_json['choices'] if 'choices' in event_json else None
                    answer = event_json['answer'] if 'answer' in event_json else None
                    time_start = event_json['time_start'] if 'time_start' in event_json else None
                    time_end = event_json['time_end'] if 'time_end' in event_json else None
                    time_spent = event_json['time_spent'] if 'time_spent' in event_json else None
                    batch_id = event_json['batchId'] if 'batchId' in event_json else None
                    epoch_t = event_json['timestamp'] if 'timestamp' in event_json else None
                    timestamp = datetime.fromtimestamp(epoch_t / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    data = [[event_id, event_date, event_type, question_id, choices, answer, time_start, time_end,
                             time_spent, timestamp, batch_id, game_id, player_id]]
                    df_new = pd.DataFrame(data,
                                          columns=['event_id', 'event_date', 'event_type', 'question_id', 'choices',
                                                   'answer', 'time_start', 'time_end', 'time_spent', 'tstamp',
                                                   'batch_id', 'game_id', 'player_id'])
                    df = df.append(df_new, ignore_index=True, sort=False)

    df = df.iloc[df.astype(str).drop_duplicates().index]
    df.to_sql('pakquiz_events', engine, schema='reports', if_exists='append', index=False)
