{{ config(materialized='table') }}

select
  corr(u.reputation, a.score) as corr_reputation_answer_score,
  count(distinct a.answer_id) as total_answers
from {{ ref('answers_dbt') }} a
join {{ ref('users_dbt') }} u
  on a.owner_user_id = u.user_id
