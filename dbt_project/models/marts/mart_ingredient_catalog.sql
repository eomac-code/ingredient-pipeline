-- mart_ingredient_catalog.sql
-- Final analytical model: one row per ingredient with chemical +
-- nutritional properties, classifications, and FAIR metadata.
-- Materialised as table in marts schema.

with enriched as (
    select * from {{ ref('int_ingredient_enriched') }}
),

categorised as (
    select
        -- identity
        compound_id,
        iupac_name                                          as ingredient_name,
        synonyms_json,
        molecular_formula,
        smiles,
        inchi_key,

        -- chemical properties
        round(molecular_weight_g_mol, 4)                   as molecular_weight_g_mol,
        round(xlogp, 3)                                     as xlogp,
        hbond_donor_count,
        hbond_acceptor_count,
        formal_charge,

        -- lipinski rule of five classification
        case
            when molecular_weight_g_mol <= 500
             and xlogp <= 5
             and hbond_donor_count <= 5
             and hbond_acceptor_count <= 10
            then 'drug_like'
            else 'non_drug_like'
        end                                                 as lipinski_class,

        -- polarity classification based on xlogp
        case
            when xlogp < -1 then 'hydrophilic'
            when xlogp between -1 and 2 then 'moderate'
            when xlogp > 2 then 'lipophilic'
            else 'unknown'
        end                                                 as polarity_class,

        -- nutrition (per 100g, null if no match)
        food_id,
        food_description,
        round(energy_kcal, 2)                              as energy_kcal,
        round(protein_g, 3)                                as protein_g,
        round(fat_g, 3)                                    as fat_g,
        round(carbohydrate_g, 3)                           as carbohydrate_g,
        round(sugars_g, 3)                                 as sugars_g,
        round(fiber_g, 3)                                  as fiber_g,
        round(calcium_mg, 3)                               as calcium_mg,
        round(iron_mg, 3)                                  as iron_mg,
        round(vitamin_c_mg, 3)                             as vitamin_c_mg,

        -- data quality flags
        has_nutrition_data,
        case when smiles is not null then true else false end   as has_smiles,
        case when inchi_key is not null then true else false end as has_inchi_key,

        -- FAIR metadata
        'https://pubchem.ncbi.nlm.nih.gov/compound/' || compound_id as pubchem_url,
        compound_ingested_at,
        nutrition_ingested_at,
        current_timestamp                                   as dbt_updated_at

    from enriched
)

select * from categorised
