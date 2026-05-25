-- models/staging/stg_nutrition.sql
--
-- Deduplicates raw_nutrition by keeping the latest record per fdc_id.
-- Raw table grows with every pipeline run (append), so researchers
-- always see current values while full history is preserved in raw.

WITH source AS (
    SELECT * FROM {{ source('ingredients', 'raw_nutrition') }}
),

-- keep only the most recent record per food item
latest AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY fdc_id
        ORDER BY _loaded_at DESC
    ) = 1
)

SELECT
    fdc_id AS food_id,
    description AS food_description,
    search_term,
    data_type,
    cast(protein_g AS float64) AS protein_g,
    cast(fat_g AS float64) AS fat_g,
    cast(carbohydrate_g AS float64) AS carbohydrate_g,
    cast(energy_kcal AS float64) AS energy_kcal,
    cast(sugars_g AS float64) AS sugars_g,
    cast(fiber_g AS float64) AS fiber_g,
    cast(calcium_mg AS float64) AS calcium_mg,
    cast(iron_mg AS float64) AS iron_mg,
    cast(vitamin_c_mg AS float64) AS vitamin_c_mg,
    _loaded_at AS ingested_at

FROM latest
