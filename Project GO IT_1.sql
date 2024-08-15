with t1 as (
    select
        gpu.*,
        gp.payment_date as payment_date,
        gp.revenue_amount_usd as revenue_amount_usd
    from project.games_paid_users gpu
    left join project.games_payments gp on gpu.user_id = gp.user_id
),
monthly_revenue as (
    select
        t1.user_id as user_id,
        t1.game_name as game_name,
        t1.age as age,
        t1.language,
        date_trunc('month', payment_date)::date as month_payment,
        sum(revenue_amount_usd) as user_mrr
    from t1
    group by user_id, game_name, age, language, month_payment
),
new_paid_users as (
    select
        user_id,
        game_name,
        age,
        min(month_payment) over (partition by user_id) as first_payment_month
    from monthly_revenue
),
churned_users_prep as (
    select
        user_id,
        game_name,
        age,
        month_payment,
        user_mrr,
        max(month_payment) over (partition by user_id) as last_payment_month,
        lag(user_mrr) over (partition by user_id order by month_payment) as previous_month_mrr
    from monthly_revenue
),
churned_users as (
    select *
    from churned_users_prep
    where last_payment_month < date_trunc('month', current_date) - interval '1 month'
),
expansion_and_contraction as (
    select
        user_id,
        month_payment,
        game_name,
        age,
        case when user_mrr > previous_month_mrr then user_mrr - previous_month_mrr else 0 end as expansion_mrr,
        case when user_mrr < previous_month_mrr then user_mrr - previous_month_mrr else 0 end as contraction_mrr
    from churned_users_prep
)
select
    mr.game_name,
    mr.age,
    mr.language,
    mr.month_payment::date,
    npu.first_payment_month::date,
    cu.last_payment_month::date,
    count(distinct mr.user_id) 								as paid_users,                    	-- Paid Users
    mr.user_mrr 											as mrr,                             -- MRR (Monthly Recurring Revenue)
    round(mr.user_mrr / count(distinct mr.user_id), 2) 		as arppu, 							-- ARPPU (Average Revenue Per Paying User)
    date_part('month', age(cu.last_payment_month, npu.first_payment_month))+1 									as lt,    				-- Customer Life Time
    mr.user_mrr * (date_part('month', age(cu.last_payment_month, npu.first_payment_month))+1) 					as ltv,					-- lTV
    count(distinct case when mr.month_payment = npu.first_payment_month then mr.user_id end) 					as new_paid_users, 		-- New Paid Users
    case when mr.month_payment = npu.first_payment_month then mr.user_mrr end 									as new_mrr, 			-- New MRR
    count(distinct case when mr.month_payment = cu.last_payment_month then cu.user_id end) 						as churned_users, 		-- Churned Users
    case when mr.month_payment = cu.last_payment_month then mr.user_mrr end 									as churned_revenue, 	-- Churned Revenue
    round(count(distinct case when mr.month_payment = cu.last_payment_month then cu.user_id end) * 100.0 / 
        lag(count(distinct mr.user_id)) over (partition by mr.game_name, mr.age order by mr.month_payment), 2) 	as churn_rate, 			-- Churn Rate
    round(sum(case when mr.month_payment = cu.last_payment_month then cu.user_mrr else 0 end) * 100.0 / 
        lag(sum(mr.user_mrr)) over (partition by mr.game_name, mr.age order by mr.month_payment), 2) 			as revenue_churn_rate, 	-- Revenue Churn Rate
    eac.expansion_mrr 										as expansion_mrr,                    -- Expansion MRR
    eac.contraction_mrr 									as contraction_mrr                	 -- Contraction MRR
from monthly_revenue mr
left join new_paid_users npu on mr.user_id = npu.user_id
left join churned_users cu on mr.user_id = cu.user_id
left join expansion_and_contraction eac on mr.user_id = eac.user_id and mr.month_payment = eac.month_payment
group by mr.game_name, mr.age, mr.language, mr.month_payment, mr.user_mrr, npu.first_payment_month, cu.last_payment_month, eac.expansion_mrr, eac.contraction_mrr
order by mr.month_payment, mr.game_name, mr.age, mr.language
;
