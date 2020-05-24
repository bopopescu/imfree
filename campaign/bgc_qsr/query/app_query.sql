select
    distinct
    au.app_user_id
    ,au.app_user_first_name as first_name
    ,au.app_user_version
    ,mc.municipality_city_name as city
    ,d.mobile_number
from app_user au
left join municipality_city mc
    on au.municipality_city_id = mc.municipality_city_id
left join device d
    on au.app_user_id = d.app_user_id
where
    au.app_user_qr_code = '{0}'
