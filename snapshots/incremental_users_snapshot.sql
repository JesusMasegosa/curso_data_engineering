{% snapshot incremental_users_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='f_carga',
    )
}}

select * from {{ ref('incremental_users') }}

{% endsnapshot %}