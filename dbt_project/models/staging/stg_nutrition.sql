-- stg_nutrition.sql
-- Casts and renames raw USDA FoodData Central nutrition records.
-- One row per food item (fdc_id).

with source as (
    select * from {{ source('raw', 'raw_nutrition') }}
),

renamed as (
    select
        fdc_id                                      as food_id,
        lower(trim(description))                    as food_description,
        lower(trim(search_term))                    as search_term,
        data_type,

        -- macronutrients (per 100g)
        cast(protein_g as double)                   as protein_g,
        cast(fat_g as double)                       as fat_g,
        cast(carbohydrate_g as double)              as carbohydrate_g,
        cast(sugars_g as double)                    as sugars_g,
        cast(fiber_g as double)                     as fiber_g,

        -- energy
        cast(energy_kcal as double)                 as energy_kcal,

        -- micronutrients
        cast(calcium_mg as double)                  as calcium_mg,
        cast(iron_mg as double)                     as iron_mg,
        cast(vitamin_c_mg as double)                as vitamin_c_mg,

        _loaded_at                                  as ingested_at

    from source
    where fdc_id is not null
)

select * from renamed
