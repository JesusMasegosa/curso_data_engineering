{{ config(
    materialized='incremental'
    ) 
    }}


WITH stg_incremental_users AS (
    SELECT * ,
    cast(phone_number as number)
    FROM {{ source('sql_server_dbo','users') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}
    ),

renamed_casted AS (
    SELECT
          user_id
        , first_name
        , last_name
        , address_id
        , phone_number
        , _fivetran_synced as f_carga
    FROM stg_incremental_users
    )



SELECT * FROM renamed_casted