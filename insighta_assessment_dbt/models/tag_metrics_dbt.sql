{{ config(materialized='table') }}

with question_tags as (
  select
    q.question_id,
    tag_id
  from {{ ref('questions_dbt') }} q,
  unnest(q.tag_ids) as tag_id
),
metrics as (
  select
    t.tag_id,
    t.tag_name,
    count(distinct qt.question_id) as total_questions,
    sum(case when fqas.is_unanswered = 0 then 1 else 0 end) as answered_questions,
    sum(case when fqas.is_unanswered = 1 then 1 else 0 end) as unanswered_questions,
    safe_divide(sum(case when fqas.is_unanswered = 1 then 1 else 0 end), count(distinct qt.question_id)) as unanswered_ratio
  from question_tags qt
  join {{ ref('question_metrics_dbt') }} fqas
    on qt.question_id = fqas.question_id
  join {{ ref('tags_dbt') }} t
    on qt.tag_id = t.tag_id
  group by 1,2
)
select * from metrics
