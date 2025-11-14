{{ config(materialized='table') }}

with exploded_tags as (
    select
        q.question_id,
        tag_id
    from {{ ref('questions') }} q,
    unnest(q.tag_ids) tag_id
),
answer_scores as (
    select
        a.parent_id as question_id,
        a.answer_id,
        a.owner_user_id,
        a.score,
        a.creation_date,
        case when a.answer_id = q.accepted_answer_id then 1 else 0 end as is_accepted
    from {{ ref('answers') }} a
    left join {{ ref('questions') }} q 
        on a.parent_id = q.question_id
),
accepted_answer_times as (
    select
        q.question_id,
        timestamp_diff(a.creation_date, q.creation_date, HOUR) as hours_to_accept
    from {{ ref('questions') }} q
    join {{ ref('answers') }} a
        on a.answer_id = q.accepted_answer_id
),
tag_users as (
    select
        et.tag_id,
        q.owner_user_id as question_user_id,
        a.owner_user_id as answer_user_id
    from exploded_tags et
    left join {{ ref('questions') }} q on q.question_id = et.question_id
    left join {{ ref('answers') }} a on a.parent_id = et.question_id
),
aggregated_users as (
    select
        tag_id,
        array_agg(distinct question_user_id) as askers,
        array_agg(distinct answer_user_id) as answerers
    from tag_users
    group by tag_id
),
tag_answer_counts as (
    select
        et.tag_id,
        count(a.answer_id) as total_answers
    from exploded_tags et
    left join answer_scores a on a.question_id = et.question_id
    group by et.tag_id
),
tag_views as (
    select
        et.tag_id,
        sum(q.view_count) as total_views,
        count(distinct et.question_id) as total_questions
    from exploded_tags et
    left join {{ ref('questions') }} q on q.question_id = et.question_id
    group by et.tag_id
),
top_user_contributions as (
    select
        et.tag_id,
        a.owner_user_id,
        count(*) as answers_per_user
    from exploded_tags et
    join answer_scores a on a.question_id = et.question_id
    group by 1,2
),
top_percent_contrib as (
    select
        tag_id,
        percentile_cont(answers_per_user, 0.9) over(partition by tag_id) as top10_cutoff,
        percentile_cont(answers_per_user, 0.75) over(partition by tag_id) as top25_cutoff,
        percentile_cont(answers_per_user, 0.5) over(partition by tag_id) as top50_cutoff
    from top_user_contributions
)

select
    t.tag_id,
    tg.tag_name,
    tv.total_questions,
    count(distinct case when q.accepted_answer_id is not null then et.question_id end) as answered_questions,
    safe_divide(count(distinct case when q.accepted_answer_id is not null then et.question_id end), tv.total_questions) as answer_ratio,
    avg(a.score) as avg_answer_quality,
    avg(case when a.is_accepted = 1 then a.score end) as avg_accepted_answer_quality,
    avg(case when a.is_accepted = 0 then a.score end) as avg_non_accepted_answer_quality,
    avg(att.hours_to_accept) as avg_hours_to_accepted_answer,
    cardinality(array_union(au.askers, au.answerers)) as total_users_interacting,
    safe_divide(cardinality(array_except(au.answerers, au.askers)), cardinality(array_union(au.askers, au.answerers))) as ratio_only_answering,
    safe_divide(cardinality(array_except(au.askers, au.answerers)), cardinality(array_union(au.askers, au.answerers))) as ratio_only_asking,
    safe_divide(tac.total_answers, tv.total_views) as answer_to_view_ratio
from exploded_tags et
left join {{ ref('tags') }} tg on tg.tag_id = et.tag_id
left join {{ ref('questions') }} q on q.question_id = et.question_id
left join answer_scores a on a.question_id = et.question_id
left join accepted_answer_times att on att.question_id = et.question_id
left join aggregated_users au on au.tag_id = et.tag_id
left join tag_answer_counts tac on tac.tag_id = et.tag_id
left join tag_views tv on tv.tag_id = et.tag_id
group by 1,2, tv.total_questions, tac.total_answers, tv.total_views;
