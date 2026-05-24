-- int_ingredient_enriched.sql
-- Joins compound chemical properties with nutritional data.
-- Matched on search_term ~ iupac_name or synonym.
-- Materialised as ephemeral (inlined into marts, no table created).

with compounds as (
    select * from {{ ref('stg_compounds') }}
),

nutrition as (
    select * from {{ ref('stg_nutrition') }}
),

-- fuzzy join: match USDA search term against IUPAC name
joined as (
    select
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
        c.ingested_at                                   as compound_ingested_at,

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
        n.ingested_at                                   as nutrition_ingested_at,

        -- flag: did we find a nutrition match?
        case when n.food_id is not null then true else false end as has_nutrition_data

    from compounds c
    left join nutrition n
        on c.iupac_name like '%' || n.search_term || '%'
        or n.search_term like '%' || c.iupac_name || '%'
)

select * from joined
