-- int_ingredient_enriched.sql
-- Joins compound chemical properties with nutritional data.
-- Matched on search_term ~ iupac_name or synonym.
-- Materialised as ephemeral (inlined into marts, no table created).

WITH compounds AS (
    SELECT * FROM {{ ref('stg_compounds') }}
),

nutrition AS (
    SELECT * FROM {{ ref('stg_nutrition') }}
),

-- fuzzy join: match USDA search term against IUPAC name
joined AS (
    SELECT
        c.compound_id,
        c.iupac_name,
        c.molecular_formula,
        c.molecular_weight_g_mol,
        c.smiles,
        c.inchi_key,
        c.xlogp,
        c.hbond_donor_count,
        c.hbond_acceptor_count,
        c.formal_charge,
        c.synonyms_json,
        c.ingested_at AS compound_ingested_at,

        n.food_id,
        n.food_description,
        n.energy_kcal,
        n.protein_g,
        n.fat_g,
        n.carbohydrate_g,
        n.sugars_g,
        n.fiber_g,
        n.calcium_mg,
        n.iron_mg,
        n.vitamin_c_mg,
        n.ingested_at AS nutrition_ingested_at,

        -- flag: did we find a nutrition match?
        coalesce(n.food_id IS NOT null, false) AS has_nutrition_data

    FROM compounds AS c
    LEFT JOIN nutrition AS n
        ON
            c.iupac_name LIKE '%' || n.search_term || '%'
            OR n.search_term LIKE '%' || c.iupac_name || '%'
)

SELECT * FROM joined
