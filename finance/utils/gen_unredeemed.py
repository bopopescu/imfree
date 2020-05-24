import psycopg2
from utils.cfg_parse import config
import pandas as pd
from datetime import date, timedelta
from utils.date_func import daterange


def accumulated_unredeemed():
    pg_params = config('postgre')
    pg_conn = psycopg2.connect(dbname=pg_params['database'],
                               host=pg_params['host'],
                               user=pg_params['user'],
                               password=pg_params['password'])

    users = {}
    dt_sum = {}
    start_date = date(2018, 4, 18)
    today = date.today()
    tomorrow = today + timedelta(days=1)

    for process_dt in daterange(start_date, tomorrow):
        pg_query = """
                    with max_trans as (
                    select
                        app_user_id
                        ,max(transaction_date) as transaction_date
                    from staging.user_reward_history
                    group by app_user_id
                    ),

                    trans as (
                    select
                        app_user_id
                        ,transaction_date
                        ,wallet
                    from (
                            select
                                app_user_id
                                ,transaction_date
                                ,wallet
                                ,rank() over (partition by app_user_id, transaction_date::date 
                                              order by transaction_date desc) as rnk
                            from staging.user_reward_history
                            where
                                transaction_date::date = '{0}'
                        ) src
                    where rnk = 1
                    group by 
                        app_user_id
                        ,transaction_date
                        ,wallet
                    )

                    select
                        t.app_user_id
                        ,t.transaction_date::date as transaction_date
                        ,case
                            when t.transaction_date = mt.transaction_date
                            then	case
                                        when wallet > 0
                                        then wallet
                                        else 0
                                    end
                            else wallet
                        end as unredeemed
                    from trans t
                    left join max_trans mt
                        on mt.app_user_id = t.app_user_id
                        and mt.transaction_date = t.transaction_date
                    """.format(process_dt)
        pg_data = pd.read_sql_query(pg_query, pg_conn)

        for k, v in pg_data.iterrows():
            users[v['app_user_id']] = v['unredeemed']

        dt_sum[process_dt] = sum(users.values())
        df_sum = pd.DataFrame.from_dict(dt_sum, orient='index')

    return df_sum
