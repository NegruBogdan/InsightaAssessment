select *
from {{ ref('questions') }} q
left join {{ ref('answers') }} a on q.accepted_answer_id = a.answer_id
where array_length(tag_ids) = 0
   or (q.accepted_answer_id is not null and a.question_id != q.question_id)
