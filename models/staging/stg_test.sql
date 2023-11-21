
WITH  src_orders AS (SELECT * FROM {{ source('sql_server_dbo', 'orders') }}),

renamed_casted as (
    SELECT 
        decode(promo_id, '', '999', {{dbt_utils.surrogate_key(["promo_id")] }})
    ) as promo_id
    from src_orders
)

SELECT * 
from renamed_casted

WITH  src_orders AS (SELECT * FROM {{ source('sql_server_dbo', 'orders') }}),

renamed_casted AS (
    SELECT
         order_id
        , shipping_service
        , shipping_cost
        , address_id
        , created_at
        , promo_id
        , estimated_Delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at 
        , tracking_id
        , status
        , _fivetran_synced AS date_load
    FROM src_orders
        )

SELECT * FROM renamed_casted