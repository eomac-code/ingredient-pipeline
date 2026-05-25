-- models/staging/stg_compounds.sql
--
-- Deduplicates raw_compounds by keeping the latest record per CID.
-- Raw table grows with every pipeline run (append), so researchers
-- always see current values while full history is preserved in raw.

with source as (
    select * from {{ source('ingredients', 'raw_compounds') }}
),

-- keep only the most recent record per compound
latest as (
    select *
    from source
    qualify row_number() over (
        partition by cid
        order by _loaded_at desc
    ) = 1
)

select
    cid                                    as compound_id,
    molecular_formula,
    cast(molecular_weight as float64)      as molecular_weight_g_mol,
    iupac_name,
    isomeric_smiles                        as smiles,
    inchi_key,
    cast(xlogp as float64)                 as xlogp,
    cast(hbond_donor_count as int64)       as hbond_donor_count,
    cast(hbond_acceptor_count as int64)    as hbond_acceptor_count,
    cast(charge as int64)                  as formal_charge,
    synonyms                               as synonyms_json,
    _loaded_at                             as ingested_at

from latest