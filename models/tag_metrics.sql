{{ config(materialized='table') }}

with base as (
    select
        q.question_id,
        q.owner_user_id as question_user_id,
        q.accepted_answer_id,
        q.creation_date as question_creation,
        q.view_count,
        tag_id,
        a.answer_id,
        a.owner_user_id as answer_user_id,
        a.score as answer_score,
        a.creation_date as answer_creation,
        case when a.answer_id = q.accepted_answer_id then 1 else 0 end as is_accepted
    from {{ ref('questions') }} q
    CROSS JOIN UNNEST(q.tag_ids) AS tag_id
    left join {{ ref('answers') }} a on a.question_id = q.question_id
),

tag_metrics as (
    select
        tag_id,
        count(distinct question_id) as total_questions,
        sum(case when accepted_answer_id is not null then 1 else 0 end) as answered_questions,
        avg(answer_score) as avg_answer_quality,
        avg(case when is_accepted = 1 then answer_score end) as avg_accepted_answer_quality,
        avg(case when is_accepted = 0 then answer_score end) as avg_non_accepted_answer_quality,
        avg(case when is_accepted = 1 then timestamp_diff(answer_creation, question_creation, HOUR) end) as avg_hours_to_accepted_answer,
        count(answer_id) as total_answers,
        sum(view_count) as total_views
    from base
    group by tag_id
)

select
    tm.tag_id,
    tg.tag_name,
    tm.total_questions,
    tm.answered_questions,
    safe_divide(tm.answered_questions, tm.total_questions) as answer_ratio,
    tm.avg_answer_quality,
    tm.avg_accepted_answer_quality,
    tm.avg_non_accepted_answer_quality,
    tm.avg_hours_to_accepted_answer,
    safe_divide(tm.total_answers, tm.total_views) as answer_to_view_ratio
from tag_metrics tm
left join {{ ref('tags') }} tg on tg.tag_id = tm.tag_id;
