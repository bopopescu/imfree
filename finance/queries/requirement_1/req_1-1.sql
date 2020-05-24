with base as (
select
	distinct
	transaction_date::date as dt
from staging.user_reward_history
),

rewards as (
select
	dt
	,sum(reward) over (order by dt asc) as reward
from	(
		select
			transaction_date::date as dt
			,sum(amount) as reward
		from staging.user_reward_history
		where
			action = 'EARNED'
			and is_regular_voucher = 0
		group by transaction_date::date
		) rw
),

optout as (
select
	transaction_date::date as dt
	,sum(amount) as amt
from staging.user_reward_history
where
	action = 'OPTOUT'
	and is_regular_voucher = 0
group by transaction_date::date
),

refunded as (
select
	transaction_date::date as dt
	,sum(amount) as amt
from staging.user_reward_history
where
	action = 'REFUNDED'
	and is_regular_voucher = 0
group by transaction_date::date
),

redeemed as (
select
	transaction_date::date as dt
	,sum(amount) as amt
from staging.user_reward_history urh
where
	action = 'REDEEMED'
	and is_regular_voucher = 0
	and transaction_type = 'LOAD REDEMPTION'
group by transaction_date::date
),

redeemed2 as (
select
	transaction_date::date as dt
	,sum(amount) as amt
from staging.user_reward_history urh
where
	action = 'REDEEMED'
	and is_converted_voucher = 1
group by transaction_date::date
),

redeem as (
select
	dt
	,sum(redeemed) over (order by dt asc) as redeemed
from	(
		select
			b.dt
			,((coalesce(red.amt, 0) + coalesce(red2.amt, 0)) - coalesce(opt.amt, 0) - coalesce(ref.amt, 0)) as redeemed
		from base b
		left join optout opt
			on opt.dt = b.dt
		left join refunded ref
			on ref.dt = b.dt
		left join redeemed red
			on b.dt = red.dt
        left join redeemed2 red2
            on b.dt = red2.dt
		) z
)

select
	b.dt
	,coalesce(rw.reward, 0) as reward
	,coalesce(rd.redeemed, 0) as redeemed
from base b
left join rewards rw
	on b.dt = rw.dt
left join redeem rd
	on b.dt = rd.dt
where b.dt >= '2019-01-01'