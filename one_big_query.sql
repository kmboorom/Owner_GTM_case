
with 
stg_advertising_expense as (
    select 
        to_char(date_trunc('MONTH',try_to_date(MONTH, 'MON-YY')), 'YYYY-MM-DD') as expense_month,
        'advertising' as expense_type,
        null as team,
        round((try_to_number(REGEXP_REPLACE(ADVERTISING, '[^0-9]', '')))/100,2) as expense_amount_usd
    from DEMO_DB.GTM_CASE.EXPENSES_ADVERTISING
),

stg_salary_expense as (
    select * from DEMO_DB.GTM_CASE.EXPENSES_SALARY_AND_COMMISSIONS
),

int_salary_expenses_pivoted as (
    select 
        to_char(date_trunc('MONTH',try_to_date(MONTH, 'MON-YY')), 'YYYY-MM-DD') as expense_month,
        'outbound_sales_team' as expense_type,
        'outbound' as team,
        round((try_to_number(REGEXP_REPLACE(OUTBOUND_SALES_TEAM, '[^0-9]', '')))/100,2) as expense_amount_usd,
    from stg_salary_expense

    union all

    select 
        to_char(date_trunc('MONTH',try_to_date(MONTH, 'MON-YY')), 'YYYY-MM-DD') as expense_month,
        'inbound_sales_team' as expense_type,
        'inbound' as team,
        round((try_to_number(REGEXP_REPLACE(INBOUND_SALES_TEAM, '[^0-9]', '')))/100,2) as expense_amount_usd
    from stg_salary_expense
),
--this would most likely need to be an incremental model
stg_leads as (
    select 
        distinct
        lead_id,
        TO_CHAR(
                DATEADD(YEAR, 
                        CASE WHEN YEAR(TRY_TO_DATE(FORM_SUBMISSION_DATE)) < 100 
                             THEN 2000 
                             ELSE 0 
                        END, 
                        TRY_TO_DATE(FORM_SUBMISSION_DATE)),
                'YYYY-MM-DD'
        ) AS form_submission_date,
        SALES_CALL_COUNT,
        SALES_TEXT_COUNT,
        SALES_EMAIL_COUNT,
        FIRST_SALES_CALL_DATE as first_sales_call_ts,
        FIRST_TEXT_SENT_DATE as first_text_sent_ts,
        first_meeting_booked_date as first_meeting_booked_ts,
        last_sales_call_date as last_sales_call_ts,
        last_sales_email_date,
        last_sales_activity_date as last_sales_activity_ts,
        to_char(round(try_to_double(replace(predicted_sales_with_owner, ',', '.')), 2),'FM9999999990.00') as predicted_sales_with_owner,
        marketplaces_used,
        online_ordering_used,
        cuisine_types,
        location_count,
        case when location_count < 2 then '1'
            when location_count > 1 and location_count < 5 then '2-4'
            when location_count > 5 then '5+'
            end as location_buckets,
        connected_with_decision_maker,
        status,
        converted_opportunity_id
    from DEMO_DB.GTM_CASE.LEADS
),
--theres dupe opp_ids. 
stg_opportunities as (

    select 
        distinct
        opportunity_id,
        created_date as opportunity_created_date,
        stage_name,
        lost_reason_c as lost_reason,
        closed_lost_notes_c as closed_loss_notes,
        business_issue_c as business_issue,
        how_did_you_hear_about_us_c as customer_provided_lead_source,
        demo_held,
        demo_set_date,
        demo_time as demo_set_ts,
        TO_CHAR(
                DATEADD(YEAR, 
                        CASE WHEN YEAR(TRY_TO_DATE(close_date)) < 100 
                             THEN 2000 
                             ELSE 0 
                        END, 
                        TRY_TO_DATE(close_date)),
                'YYYY-MM-DD'
        ) AS close_date,
        last_sales_call_date_time
    from DEMO_DB.GTM_CASE.OPPORTUNITIES

),

int_expenses as (
    
    select * from (
    select * from stg_advertising_expense
    union all
    select * from int_salary_expenses_pivoted
    )
    order by 1
),

fct_customers as (
    select 
    lead_id,
    opportunity_id,
    iff(form_submission_date is not null, 'inbound','outbound') as sales_team,
    --LTV is impossible to calculate because I don't have any retention data, but the sub value is a good starting place
    predicted_sales_with_owner*.05 +500 as predicted_monthly_sub_value,
    --this is my best attempt at a conversion month field without more business context
    date_trunc('month',coalesce(form_submission_date,date(first_sales_call_ts))) as first_touch_month,
    l.* exclude(lead_id,converted_opportunity_id),
    o.* exclude(opportunity_id)
    from stg_leads l
    left join stg_opportunities o on l.converted_opportunity_id = o.opportunity_id
    order by lead_id

),

sales_team_metrics as (
    select
        sales_team,
        first_touch_month,
        count(lead_id) as lead_cnt,
        count(distinct iff(opportunity_id is not null,opportunity_id,null)) as opp_cnt,
        count(distinct (iff(stage_name = 'Closed Won',opportunity_id,null))) as closed_leads,
        sum(sales_call_count),
        sum(sales_text_count),
        sum(sales_email_count),
        (sum(sales_call_count) + sum(sales_text_count) + sum(sales_email_count)) as total_interactions,
        (count( distinct iff(stage_name = 'Closed Won' and location_buckets = '1',opportunity_id,null)) / count(distinct (iff(stage_name = 'Closed Won',opportunity_id,null)))) as "1_loc_per",
        (count( distinct iff(stage_name = 'Closed Won' and location_buckets = '2-4',opportunity_id,null)) / count(distinct (iff(stage_name = 'Closed Won',opportunity_id,null)))) as "2_4_loc_per",
        (count( distinct iff(stage_name = 'Closed Won' and location_buckets = '5+',opportunity_id,null)) / count(distinct (iff(stage_name = 'Closed Won',opportunity_id,null)))) as "5+_loc_per",
        sum(predicted_monthly_sub_value) as predicted_monthly_lead_sub_value,
        sum(iff(opportunity_id is not null,predicted_monthly_sub_value,0)) as predicted_monthly_conv_sub_value,
        sum(iff(stage_name = 'Closed Won',predicted_monthly_sub_value,0)) as predicted_monthly_cust_sub_value,
    
    from fct_customers c 
    where first_touch_month >= '2024-01-01'
    group by 1,2
    order by 1,2
),

add_expenses as (
    select 
        c.*,
        e.expense_amount_usd,
    from sales_team_metrics c
    left join int_expenses e on first_touch_month = e.expense_month and c.sales_team = e.team 
    order by 1,2
),

--it stands to reason that a resturaunt chain with multiple locations should have more sales than resturaunts with only one sale, but this seems to not always be the case. That would be 
--quite surprising if true. I only spent a couple minutes on this but that deserves some looking into. 
buckets_exploration as (
    select 
        first_touch_month,
        location_buckets,
        count(distinct opportunity_id),
        sum(iff(stage_name = 'Closed Won',predicted_monthly_sub_value,0)) / count(distinct opportunity_id)
    from fct_customers 
    where stage_name = 'Closed Won'
    and location_buckets is not null
    group by 1,2
    order by 1,2
),

--of the high lead resturaunts mexican and indian resturaunts convert the highest. Weighted for sales value though, american and mexican resturaunts remain the top customers
marketplaces_explo as (
    select 
        cuisine_types,
        count(distinct lead_id) as lead_cnt,
        count(distinct (case when stage_name = 'Closed Won' then opportunity_id end)),
        (count(distinct (case when stage_name = 'Closed Won' then opportunity_id end)) / count(distinct lead_id)),
        sum(iff(stage_name = 'Closed Won',predicted_monthly_sub_value,0)),
        --this is a sort of opportunity cost metric. It multiples the conversion rate of a national cusine by the avg sub value
        (count(distinct (case when stage_name = 'Closed Won' then opportunity_id end)) / count(distinct lead_id)) * sum(iff(stage_name = 'Closed Won',predicted_monthly_sub_value,0))
    from fct_customers 
    where first_touch_month > '2024-01-01'
    group by 1
    having count(distinct lead_id) > 100
    order by 4 desc

)

select * from buckets_exploration
