select
    distinct
    d.app_user_id
    ,au.app_user_first_name as first_name
    ,au.app_user_version
    ,mc.municipality_city_name as city
    ,d.mobile_number
from device d
left join app_user au
    on au.app_user_id = d.app_user_id
left join municipality_city mc
    on au.municipality_city_id = mc.municipality_city_id
where
    d.mobile_number = {0}
