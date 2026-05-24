-- stg_compounds.sql
-- Casts and renames raw PubChem compound data.
-- One row per compound (CID).

with source as (
    select * from {{ source('raw', 'raw_compounds') }}
),

renamed as (
    select
        cid                                         as compound_id,
        molecular_formula,
        cast(molecular_weight as double)            as molecular_weight_g_mol,
        lower(trim(iupac_name))                     as iupac_name,
        isomeric_smiles                             as smiles,
        inchi_key,
        cast(xlogp as double)                       as xlogp,
        cast(hbond_donor_count as integer)          as hbond_donor_count,
        cast(hbond_acceptor_count as integer)       as hbond_acceptor_count,
        cast(charge as integer)                     as formal_charge,
        synonyms                                    as synonyms_json,
        _loaded_at                                  as ingested_at

    from source
    where cid is not null
)

select * from renamed
