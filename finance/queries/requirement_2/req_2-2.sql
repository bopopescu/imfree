--req 2.2
with base as (
select
	extract(year from transaction_date) as year
	,extract(month from transaction_date) as month_number
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,transaction_name
from staging.user_reward_history
where
	is_regular_voucher = 1
group by
	extract(year from transaction_date)
	,extract(month from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_name
),

earned as (
select
	extract(year from transaction_date) as year
	,extract(month from transaction_date) as month_number
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,transaction_name
	,sum(amount) as amount_earned
	,count(amount) as count_earned
from staging.user_reward_history
where
	is_regular_voucher = 1
group by
	extract(year from transaction_date)
	,extract(month from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_name
),

redeemed as (
select
	extract(year from transaction_date) as year
	,extract(month from transaction_date) as month_number
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
	,transaction_name
	,sum(amount) as amount_redeemed
	,count(amount) as count_redeemed
from staging.user_reward_history
where
	is_regular_voucher = 1
	and action = 'REDEEMED'
group by
	extract(year from transaction_date)
	,extract(month from transaction_date)
	,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
	,transaction_name
)

select
	2018 as year
	,'As of December 31' as month
	,b.transaction_name
	,coalesce(sum(e.amount_earned), 0) as amount_earned
	,coalesce(sum(r.amount_redeemed), 0) as amount_redeemed
	,coalesce(sum(e.count_earned), 0) as count_earned
	,coalesce(sum(r.count_redeemed), 0) as count_redeemed
from base b
left join earned e
	on b.year = e.year
	and b.month = e.month
	and b.month_number = e.month_number
	and b.transaction_name = e.transaction_name
left join redeemed r
	on b.year = r.year
	and b.year = r.year
	and b.month_number = r.month_number
	and b.transaction_name = r.transaction_name
where b.year = 2018
group by
	b.transaction_name

union all

select
	b.year
	,b.month
	,b.transaction_name
	,coalesce(e.amount_earned, 0) as amount_earned
	,coalesce(r.amount_redeemed, 0) as amount_redeemed
	,coalesce(e.count_earned, 0) as count_earned
	,coalesce(r.count_redeemed, 0) as count_redeemed
from base b
left join earned e
	on b.year = e.year
	and b.month = e.month
	and b.month_number = e.month_number
	and b.transaction_name = e.transaction_name
left join redeemed r
	on b.year = r.year
	and b.year = r.year
	and b.month_number = r.month_number
	and b.transaction_name = r.transaction_name
where b.year = 2019