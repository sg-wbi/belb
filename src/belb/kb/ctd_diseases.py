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

DATASET_NAME = "ctd-diseases"


def stream_rows(file: Path, separator: str) -> Iterator[dict]:
    prefields = False
    columns = None
    with open(file) as fp:
        for line in fp:
            if line.startswith("#"):
                if line.startswith("# Fields:"):
                    prefields = True
                else:
                    if prefields and columns is None:
                        columns = [
                            f.strip() for f in line.strip().replace("#", "").split("\t")
                        ]

            else:
                if columns is None:
                    raise RuntimeError("Detection of column names failed")
                values = line.strip().split(separator)
                row = dict(zip(columns, values))
                yield row


def get_aliases(row: dict):

    aliases = []
    for c in [c for c in row if "Synonyms" in c]:
        aliases.extend([a for a in row.get(c, "").split("|") if a != ""])

    return aliases


def get_metadata(row: dict):

    metadata = {}

    if row.get("Definition"):
        metadata["description"] = row["Definition"]

    columns = {
        "ParentIDs": "parent_ids",
        "TreeNumbers": "tree_numbers",
        "ParentTreeNumbers": "parent_tree_numbers",
    }

    for key, value in columns.items():
        if row.get(key):
            metadata[value] = [m.strip() for m in row[key].split("|")]

    return metadata


def parse_ctd_diseases(path: Path) -> Iterator[dict]:
    # https://ctdbase.org/downloads/#alldiseases
    #     DiseaseName
    #     DiseaseID (MeSH or OMIM identifier)
    #     Definition
    #     AltDiseaseIDs (alternative identifiers; '|'-delimited list)
    #     ParentIDs (identifiers of the parent terms; '|'-delimited list)
    #     TreeNumbers (identifiers of the disease's nodes; '|'-delimited list)
    #     ParentTreeNumbers (identifiers of the parent nodes; '|'-delimited list)
    #     Synonyms ('|'-delimited list)
    #     SlimMappings (MEDIC-Slim mappings; '|'-delimited list)
    for row in stream_rows(path, separator="\t"):
        entry = {
            "id": row["DiseaseID"].replace("MESH:", ""),
            "label": row["DiseaseName"],
            "description": None,
        }

        # root node
        if entry["id"] == "C" and entry["label"] == "Diseases":
            continue

        aliases = get_aliases(row=row)
        if aliases:
            entry["aliases"] = aliases

        metadata = get_metadata(row=row)
        columns = {
            "AltDiseaseIDs": "alternative_ids",
            "SlimMappings": "slim_mappings",
        }
        for key, value in columns.items():
            if row.get(key):
                metadata[value] = [m.strip() for m in row[key].split("|")]

        entry.update(metadata)

        yield entry


class CtdDiseasesBuilder(GeneratorBasedBuilder):

    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            features = ENTITIES_FEATURES.copy()
            features.update(
                {
                    "description": Value("string"),
                    "alternative_ids": Sequence(Value("string")),
                    "parent_ids": Sequence(Value("string")),
                    "tree_numbers": Sequence(Value("string")),
                    "parent_tree_numbers": Sequence(Value("string")),
                    "slim_mappings": Sequence(Value("string")),
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
        for idx, e in enumerate(parse_ctd_diseases(file)):
            e["idx"] = idx
            yield idx, e


@kb_registry.register(DATASET_NAME)
class CtdDiseasesKb(BaseKb):
    dataset_name = DATASET_NAME
    builder_class = CtdDiseasesBuilder

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
