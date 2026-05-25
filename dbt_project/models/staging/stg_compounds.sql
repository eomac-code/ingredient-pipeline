-- models/staging/stg_compounds.sql
--
-- Deduplicates raw_compounds by keeping the latest record per CID.
-- Raw table grows with every pipeline run (append), so researchers
-- always see current values while full history is preserved in raw.

WITH source AS (
    SELECT * FROM {{ source('ingredients', 'raw_compounds') }}
),

-- keep only the most recent record per compound
latest AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY cid
        ORDER BY _loaded_at DESC
    ) = 1
)

SELECT
    cid AS compound_id,
    molecular_formula,
    cast(molecular_weight AS float64) AS molecular_weight_g_mol,
    iupac_name,
    isomeric_smiles AS smiles,
    inchi_key,
    cast(xlogp AS float64) AS xlogp,
    cast(hbond_donor_count AS int64) AS hbond_donor_count,
    cast(hbond_acceptor_count AS int64) AS hbond_acceptor_count,
    cast(charge AS int64) AS formal_charge,
    synonyms AS synonyms_json,
    _loaded_at AS ingested_at

FROM latest
