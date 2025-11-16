select *
from {{ ref('answers') }} a
left join {{ ref('questions') }} q on a.question_id = q.question_id
left join {{ ref('users') }} u on a.owner_user_id = u.user_id
where q.question_id is null
   or u.user_id is null
   or a.score < 0
