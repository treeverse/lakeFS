
{{ config(materialized='table', properties={
      "format": "'PARQUET'",
        "partitioned_by":"ARRAY['year']",

    }) }}

with source_data as (
    select marketplace, customer_id, review_id, product_id, product_parent, product_title, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date, product_category, "year"  from s3.esti_system_testing.amazon_reviews_parquet_internal where year > 1980
)

select *
from source_data
