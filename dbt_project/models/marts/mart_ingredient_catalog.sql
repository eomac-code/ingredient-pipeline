-- mart_ingredient_catalog.sql
-- Final analytical model: one row per ingredient with chemical +
-- nutritional properties, classifications, and FAIR metadata.
-- Materialised as table in marts schema.

WITH enriched AS (
    SELECT * FROM {{ ref('int_ingredient_enriched') }}
),

categorised AS (
    SELECT
        -- identity
        compound_id,
        iupac_name AS ingredient_name,
        synonyms_json,
        molecular_formula,
        smiles,
        inchi_key,

        -- chemical properties
        hbond_donor_count,
        hbond_acceptor_count,
        formal_charge,
        food_id,
        food_description,

        -- lipinski rule of five classification
        has_nutrition_data,

        -- polarity classification based on xlogp
        compound_ingested_at,

        -- nutrition (per 100g, null if no match)
        nutrition_ingested_at,
        round(molecular_weight_g_mol, 4) AS molecular_weight_g_mol,
        round(xlogp, 3) AS xlogp,
        CASE
            WHEN
                molecular_weight_g_mol <= 500
                AND xlogp <= 5
                AND hbond_donor_count <= 5
                AND hbond_acceptor_count <= 10
                THEN 'drug_like'
            ELSE 'non_drug_like'
        END AS lipinski_class,
        CASE
            WHEN xlogp < -1 THEN 'hydrophilic'
            WHEN xlogp BETWEEN -1 AND 2 THEN 'moderate'
            WHEN xlogp > 2 THEN 'lipophilic'
            ELSE 'unknown'
        END AS polarity_class,
        round(energy_kcal, 2) AS energy_kcal,
        round(protein_g, 3) AS protein_g,
        round(fat_g, 3) AS fat_g,
        round(carbohydrate_g, 3) AS carbohydrate_g,
        round(sugars_g, 3) AS sugars_g,
        round(fiber_g, 3) AS fiber_g,

        -- data quality flags
        round(calcium_mg, 3) AS calcium_mg,
        round(iron_mg, 3) AS iron_mg,
        round(vitamin_c_mg, 3) AS vitamin_c_mg,

        -- FAIR metadata
        coalesce(smiles IS NOT null, false) AS has_smiles,
        coalesce(inchi_key IS NOT null, false) AS has_inchi_key,
        'https://pubchem.ncbi.nlm.nih.gov/compound/' || compound_id AS pubchem_url,
        current_timestamp AS dbt_updated_at

    FROM enriched
)

SELECT * FROM categorised
