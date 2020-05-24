with deduct as (
select
	telco_name
	,sum(amount) as amount
from staging.user_reward_history
where
	action in ('OPTOUT', 'REFUNDED')
	and is_regular_voucher = 0
	and transaction_date::date < '2019-01-01'
group by
	telco_name
)

select
	year
	,month
	,redemption_channel
	,redemption
from	(
		select
			year
			,month_number
			,month
			,redemption_channel
			,(coalesce(redemption, 0) - coalesce(d.amount, 0)) as redemption
		from	(
				select
					2018 as year
					,12 as month_number
					,'As of December 31' as month
					,urh.telco_name as redemption_channel
					,sum(urh.amount) as redemption
				from staging.user_reward_history urh
				where
					action = 'REDEEMED'
					and transaction_type = 'LOAD REDEMPTION'
					and is_regular_voucher = 0
					and transaction_date::date < '2019-01-01'
				group by
					telco_name
				) src
		left join deduct d
			on src.redemption_channel = d.telco_name


		union all

		select
			2018 as year
			,12 as month_number
			,'As of December 31' as month
			,'Voucher (Converted)' as redemption_channel
			,sum(amount) as redemption
		from staging.user_reward_history
		where
			action = 'REDEEMED'
			and transaction_type = 'VOUCHER REDEMPTION'
			and is_converted_voucher = 1
			and transaction_date::date < '2019-01-01'

		union all

		select
			extract(year from transaction_date) as year
			,extract(month from transaction_date) as month_number
			,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
			,telco_name redemption_channel
			,sum(amount) as redemption
		from staging.user_reward_history
		where
			action = 'REDEEMED'
			and transaction_type = 'LOAD REDEMPTION'
			and is_regular_voucher = 0
			and transaction_date::date >= '2019-01-01'
		group by
			extract(year from transaction_date)
			,extract(month from transaction_date)
			,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
			,telco_name

		union all

		select
			extract(year from transaction_date) as year
			,extract(month from transaction_date) as month_number
			,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month') as month
			,'Voucher (Converted)' as redemption_channel
			,sum(amount) as redemption
		from staging.user_reward_history
		where
			action = 'REDEEMED'
			and transaction_type = 'VOUCHER REDEMPTION'
			and is_converted_voucher = 1
			and transaction_date::date >= '2019-01-01'
		group by
			extract(year from transaction_date)
			,extract(month from transaction_date)
			,to_char(to_timestamp (extract(month from transaction_date)::text, 'MM'), 'Month')
		) src
order by year, month_number, redemption_channel