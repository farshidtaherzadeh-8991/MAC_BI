{{ config(
    schema='dwh_core_entreprise',
    materialized='table'
)}}

with population as (
    select
        code_commune,
        libelle_commune,
        population_2013,
        population_2014,
        population_2015,
        population_2016,
        population_2017,
        population_2018,
        population_2019,
        population_2020,
        population_2021
    from {{ ref( 'stg_population') }}
),

geo as (
    select
        codgeo,
        geometry
    from {{ ref( 'stg_geo') }}
),

ref_geo as (
    select
        code_commune,
        coordonnees,
        split_part(coordonnees, ',', 1)::float as latitude,
        split_part(coordonnees, ',', 2)::float as longitude
    from {{ ref( 'stg_ref_geo') }}
)

select
    p.code_commune,
    p.libelle_commune,
    p.population_2013,
    p.population_2014,
    p.population_2015,
    p.population_2016,
    p.population_2017,
    p.population_2018,
    p.population_2019,
    p.population_2020,
    p.population_2021,
    rg.coordonnees,
    rg.latitude,
    rg.longitude,
    g.geometry

from population p
left join geo g on p.code_commune = g.codgeo
left join ref_geo rg on p.code_commune = rg.code_commune