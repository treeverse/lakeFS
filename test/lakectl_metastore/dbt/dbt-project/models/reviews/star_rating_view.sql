{{ config(materialized='view') }}
select star_rating,sum(helpful_votes) as  helpfull_votes,sum(total_votes) as total_votes
from {{ ref('amazon_reviews') }} group by 1

