{{ config(materialized='table') }}

select
  id as user_id,
  reputation
from {{ source('stackoverflow_public', 'users') }}
