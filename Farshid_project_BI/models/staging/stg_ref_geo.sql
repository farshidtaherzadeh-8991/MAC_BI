{{ config(
    materialized='table',
    schema='staging_entreprise'
) }}

select
    "COM_CODE"::text as code_commune,
    "COM_NOM"::text as nom_commune,
    "AUC_NOM"::text as nom_arrondissement,
    geolocalisation::text as coordonnees

from {{ source('dwh_raw', 'fr_esr_referentiel_geographiqu')}}