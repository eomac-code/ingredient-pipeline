-- models/staging/stg_nutrition.sql
--
-- Deduplicates raw_nutrition by keeping the latest record per fdc_id.
-- Raw table grows with every pipeline run (append), so researchers
-- always see current values while full history is preserved in raw.

with source as (
    select * from {{ source('ingredients', 'raw_nutrition') }}
),

-- keep only the most recent record per food item
latest as (
    select *
    from source
    qualify row_number() over (
        partition by fdc_id
        order by _loaded_at desc
    ) = 1
)

select
    fdc_id                          as food_id,
    description                     as food_description,
    search_term,
    data_type,
    cast(protein_g as float64)      as protein_g,
    cast(fat_g as float64)          as fat_g,
    cast(carbohydrate_g as float64) as carbohydrate_g,
    cast(energy_kcal as float64)    as energy_kcal,
    cast(sugars_g as float64)       as sugars_g,
    cast(fiber_g as float64)        as fiber_g,
    cast(calcium_mg as float64)     as calcium_mg,
    cast(iron_mg as float64)        as iron_mg,
    cast(vitamin_c_mg as float64)   as vitamin_c_mg,
    _loaded_at                      as ingested_at

from latest