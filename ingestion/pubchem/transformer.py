"""
PubChem transformer.
Responsible ONLY for shaping raw API records into the schema
expected by the loader. No API calls, no DB calls — pure functions.
Easy to unit test.
"""

import json


class PubChemTransformer:

    def transform(self, records: list[dict]) -> list[dict]:
        """Transform a list of raw PubChem API records into loader-ready dicts."""
        return [self._transform_record(rec) for rec in records]

    def _transform_record(self, rec: dict) -> dict:
        return {
            "cid":                   rec.get("CID"),
            "molecular_formula":     rec.get("MolecularFormula"),
            "molecular_weight":      self._to_float(rec.get("MolecularWeight")),
            "iupac_name":            rec.get("IUPACName"),
            "isomeric_smiles":       rec.get("IsomericSMILES"),
            "inchi_key":             rec.get("InChIKey"),
            "xlogp":                 self._to_float(rec.get("XLogP")),
            "hbond_donor_count":     rec.get("HBondDonorCount"),
            "hbond_acceptor_count":  rec.get("HBondAcceptorCount"),
            "charge":                rec.get("Charge"),
            "synonyms":              json.dumps(rec.get("synonyms", [])),
        }

    @staticmethod
    def _to_float(value) -> float | None:
        """Safely cast a value to float, returning None if not possible."""
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None