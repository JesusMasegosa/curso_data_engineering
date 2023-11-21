{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'promos') }}
),

renamed_casted AS (
    SELECT
        cast(
            {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as varchar(50)
        ) AS id_promo,
        promo_id AS desc_promo,
        discount,
        status,
        _fivetran_synced AS date_load
    FROM src_promos
)

SELECT * FROM renamed_casted
UNION ALL
SELECT
    '999' AS id_promo,
    '' as desc_promo,
    0 AS discount,
    'active' AS status,
    sysdate() AS date_load
