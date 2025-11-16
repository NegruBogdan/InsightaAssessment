{{ config(materialized='table') }}

with raw as (
    select
        id as answer_id,
        parent_id as question_id,
        owner_user_id,
        score,
        creation_date
    from {{ source('stackoverflow_public', 'posts_answers') }}
    where score >= 0
      and id is not null
      and parent_id is not null
      and owner_user_id is not null
      and creation_date is not null
),

valid_answers as (
    select r.*
    from raw r
    join {{ ref('questions') }} q
      on r.parent_id = q.question_id
)

select *
from valid_answers