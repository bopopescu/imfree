--req 3
with src as (
select
	extract(year from transaction_date) as year
	,extract(month from transaction_date) as month_number
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
from staging.user_reward_history
where
	action = 'EARNED'
	and is_voucher = 0
	--and transaction_date::date >= '2019-01-01'
group by
	extract(year from transaction_date)
	,extract(month from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
),

mgm as (
select
	extract(year from transaction_date) as year
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,count(distinct app_user_id) as no_of_users
	,transaction_type
	,sum(amount) as mgm
from staging.user_reward_history
where
	action = 'EARNED'
	and is_voucher = 0
	and transaction_type = 'MGM'
	--and transaction_date::date >= '2019-01-01'
group by
	extract(year from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_type
),

registration as (
select
	extract(year from transaction_date) as year
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,count(distinct app_user_id) as no_of_users
	,transaction_type
	,sum(amount) as reg
from staging.user_reward_history
where
	action = 'EARNED'
	and is_voucher = 0
	and transaction_type = 'REGISTRATION'
	--and transaction_date::date >= '2019-01-01'
group by
	extract(year from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_type
),

convo as (
select
	extract(year from transaction_date) as year
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,count(distinct app_user_id) as no_of_users
	,transaction_type
	,sum(amount) as convo
from staging.user_reward_history
where
	action = 'EARNED'
	and is_voucher = 0
	and transaction_type = 'CONVO'
	--and transaction_date::date >= '2019-01-01'
group by
	extract(year from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_type
),

migames as (
select
	extract(year from transaction_date) as year
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,count(distinct app_user_id) as no_of_users
	,transaction_type
	,sum(amount) as migames
from staging.user_reward_history
where
	action = 'EARNED'
	and is_voucher = 0
	and transaction_type = 'MiGames'
	--and transaction_date::date >= '2019-01-01'
group by
	extract(year from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_type
)

select
	2018 as year
	,'As of December 31' as month
	,coalesce(sum(m.no_of_users), 0) as mgm_no_of_users
	,coalesce(sum(m.mgm), 0) as mgm
	,coalesce(sum(r.no_of_users), 0) as reg_no_of_users
	,coalesce(sum(r.reg), 0) as reg
	,coalesce(sum(c.no_of_users), 0) as convo_no_of_users
	,coalesce(sum(c.convo), 0) as convo
	,coalesce(sum(mi.no_of_users), 0) as migames_no_of_users
	,coalesce(sum(mi.migames), 0) as migames
from src s
left join mgm m
	on s.year = m.year
	and s.month = m.month
left join registration r
	on s.year = r.year
	and s.month = r.month
left join convo c
	on s.year = c.year
	and s.month = c.month
left join migames mi
	on s.year = mi.year
	and s.month = mi.month
where s.year = 2018

union all

select
	s.year
	,s.month
	,coalesce(m.no_of_users, 0) as mgm_no_of_users
	,coalesce(m.mgm, 0) as mgm
	,coalesce(r.no_of_users, 0) as reg_no_of_users
	,coalesce(r.reg, 0) as reg
	,coalesce(c.no_of_users, 0) as convo_no_of_users
	,coalesce(c.convo, 0) as convo
	,coalesce(mi.no_of_users, 0) as migames_no_of_users
	,coalesce(mi.migames, 0) as migames
from src s
left join mgm m
	on s.year = m.year
	and s.month = m.month
left join registration r
	on s.year = r.year
	and s.month = r.month
left join convo c
	on s.year = c.year
	and s.month = c.month
left join migames mi
	on s.year = mi.year
	and s.month = mi.month
where s.year = 2019