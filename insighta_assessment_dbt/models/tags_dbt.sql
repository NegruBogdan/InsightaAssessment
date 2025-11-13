{{ config(materialized='table') }}

select
  row_number() over(order by tag_name) as tag_id,
  tag_name
from {{ source('stackoverflow_public', 'tags') }}