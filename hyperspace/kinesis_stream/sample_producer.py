import boto3
import random
import time

my_stream_name = 'de_workshop'

client = boto3.client('kinesis', region_name='ap-southeast-1')


def put_to_stream():
    '''
    payload = '{ "app_user_id": 20, "app_user_gender": "Female", "app_user_marital_status": "Married", "age_group_' \
              'description": "22 - 25", "area_name": "Metro Manila", "province_name": "Metro Manila", "municipality_' \
              'city_name": "PASAY CITY", "sec_name": "C2", "brand_id": 1, "brand_name": "Din Tai Fung", "branch_name": ' \
              '"7-11 Accralaw", "product_description": "", "promo_name": "Dumplings Promo", "deployment_strategy": "", ' \
              '"served_date": "2019-08-20T10:25:32.204Z", "redemption_date": "2019-09-05T16:10:14.722Z", "redeemed": 1, ' \
              '"hour_of_day": 16, "day_of_week": "Thursday", "day_arranger": 4, "location": "14.553715 , 121.0427483" }'

    '''
    payload = '{"app_user_id": "20", "app_user_gender": "Female", "app_user_marital_status": "Married", ' \
              '"age_group_description": "22 - 25", "area_name": "Metro Manila", "province_name": "Metro Manila", ' \
              '"municipality_city_name": "PASAY CITY", "sec_name": "C2", "brand_id": 1, "brand_name": "Din Tai Fung", ' \
              '"program_type": "measuring", "campaign_name": "Were not stragers to love.", "campaign_title": ' \
              '"Are you a stranger to love?", "campaign_description": " Lorem ipsum dolor whatever blah blah blah", ' \
              '"campaign_served_date": "2019-08-22T07:59:31.035Z", "campaign_completed_date": "2019-09-05T15:28:03.919Z", ' \
              '"completed": 1, "deployment_strategy": "", "hour_of_day": 15, "day_of_week": "Thursday", "day_arranger": 4}'

    #payload = '{"id": 22, "fname": "data", "lname": "engineer"}'

    print(payload)

    client.put_record(
        StreamName=my_stream_name,
        Data=payload,
        PartitionKey=str(id)
    )


while True:
    val = random.randint(1, 9999)
    put_to_stream()
    time.sleep(5)
