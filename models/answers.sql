{{ config(materialized='table') }}

select
  id as answer_id,
  parent_id as question_id,
  owner_user_id,
  score,
  creation_date
from {{ source('stackoverflow_public', 'posts_answers') }}
