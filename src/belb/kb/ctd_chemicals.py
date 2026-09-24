from collections.abc import Iterator
from pathlib import Path

from datasets import (
    DatasetInfo,
    DownloadManager,
    Features,
    GeneratorBasedBuilder,
    Sequence,
    Split,
    SplitGenerator,
    Value,
    Version,
)

from .base import ENTITIES_FEATURES, BaseKb, KbBuilderConfig, KbTable, kb_registry
from .ctd_diseases import get_aliases, get_metadata, stream_rows

DATASET_NAME = "ctd-chemicals"


def parse_ctd_chemicals(path: Path) -> Iterator[dict]:
    # https://ctdbase.org/downloads/#allchems
    #     ChemicalName
    #     ChemicalID (MeSH identifier)
    #     CasRN (CAS Registry Number, if available)
    #     PubChemCID (PubChem Compound, if available)
    #     PubChemSID (PubChem Substance, if available)
    #     DTXSID (CompTox Chemicals Dashboard, if available)
    #     InChIKey (InChIKey, if available)
    #     Definition
    #     ParentIDs (identifiers of the parent terms; '|'-delimited list)
    #     TreeNumbers (identifiers of the chemical's nodes; '|'-delimited list)
    #     ParentTreeNumbers (identifiers of the parent nodes; '|'-delimited list)
    #     MESHSynonyms ('|'-delimited list)
    #     CTDCuratedSynonyms ('|'-delimited list)
    for row in stream_rows(path, separator="\t"):
        entry = {
            "id": row["ChemicalID"].replace("MESH:", ""),
            "label": row["ChemicalName"],
            "description": None,
        }

        aliases = get_aliases(row=row)
        if aliases:
            entry["aliases"] = aliases

        metadata = get_metadata(row=row)
        columns = {
            "CasRN": "casrn",
            "PubChemCID": "pubchem_id",
            "PubChemSID": "pubchem_sid",
            "DTXSID": "dtxsid",
            "InChIKey": "inchikey",
        }
        for key, value in columns.items():
            if row.get(key):
                metadata[value] = row[key]

        entry.update(metadata)

        yield entry


class CtdChemicalsBuilder(GeneratorBasedBuilder):

    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            features = ENTITIES_FEATURES.copy()
            features.update(
                {
                    "description": Value("string"),
                    "casrn": Value("string"),
                    "pubchem_id": Value("string"),
                    "pubchem_sid": Value("string"),
                    "dtxsid": Value("string"),
                    "inchikey": Value("string"),
                    "parent_ids": Sequence(Value("string")),
                    "tree_numbers": Sequence(Value("string")),
                    "parent_tree_numbers": Sequence(Value("string")),
                }
            )
            return DatasetInfo(
                features=Features(features),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.ENTITIES),
            )
        else:
            raise ValueError(f"Invalid `name={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:

        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            gen_kwargs = {"file": Path(self.config.data_dir) / data_files["file"]}
        else:
            downloaded = dl_manager.download_and_extract(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, file: Path):
        for idx, e in enumerate(parse_ctd_chemicals(file)):
            e["idx"] = idx
            yield idx, e


@kb_registry.register(DATASET_NAME)
class CtdChemicalsKb(BaseKb):
    dataset_name = DATASET_NAME
    builder_class = CtdChemicalsBuilder

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
