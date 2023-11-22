
{{
  config(
    materialized='incremental',
    unique_key='user_id'
  )
}}

WITH  src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(f_carga) from {{ this }})
    {% endif %}
    ),

renamed_casted AS (
    SELECT
         user_id
        , updated_at
        , address_id
        , last_name
        , created_at
        , cast(replace(phone_number,'-','') as number) as phone_number
        , total_orders
        , first_name
        , email
        , _fivetran_synced AS f_carga
    FROM src_users
        )

SELECT * FROM renamed_casted