{{ config(
    materialized='table',
    schema='staging_entreprise'
) }}

select
    codgeo::text as codgeo,
    libgeo::text as libgeo,
    geometry::text 

from {{ source('dwh_raw', 'geopandas')}}