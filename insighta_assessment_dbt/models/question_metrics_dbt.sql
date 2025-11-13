{{ config(materialized='table') }}

with questions as (
  select * from {{ ref('questions_dbt') }}
),
answers as (
  select * from {{ ref('answers_dbt') }}
)
select
  q.question_id,
  q.title,
  q.creation_date as question_date,
  q.view_count,
  q.comment_count,
  q.owner_user_id,
  count(a.answer_id) as total_answers,
  sum(case when a.answer_id = q.accepted_answer_id then 1 else 0 end) as accepted_answers,
  case when count(a.answer_id) = 0 then 1 else 0 end as is_unanswered
from questions q
left join answers a
  on q.question_id = a.question_id
group by 1,2,3,4,5,6
