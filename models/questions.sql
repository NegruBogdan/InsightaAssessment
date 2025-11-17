{{ config(materialized='table') }}

with raw as (
    select
        id as question_id,
        title,
        accepted_answer_id,
        comment_count,
        view_count,
        owner_user_id,
        creation_date,
        split(tags, '|') as tag_list
    from {{ source('stackoverflow_public', 'posts_questions') }}
)

select
    r.question_id,
    r.title,
    r.accepted_answer_id,
    r.comment_count,
    r.view_count,
    r.owner_user_id,
    r.creation_date,
    array_agg(t.tag_id order by t.tag_id) as tag_ids
from raw r
left join unnest(r.tag_list) as tag_name
left join {{ ref('tags') }} t
    on trim(tag_name) = trim(t.tag_name)
where t.tag_id is not null
and r.question_id is not null
and r.owner_user_id is not null
group by 1,2,3,4,5,6,7
