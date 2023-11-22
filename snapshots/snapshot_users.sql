{% snapshot snapshot_users %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['address_id','last_name','phone_number','first_name','email'],
        )
}}

select * from {{ ref('users') }}
where f_carga > (select max(f_carga) from {{ ref('users') }})

{% endsnapshot %}