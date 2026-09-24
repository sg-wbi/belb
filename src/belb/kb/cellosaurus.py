from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

from datasets import (
    Dataset,
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
from datasets.formatting.formatting import LazyBatch

from ..utils import NIL
from .base import (
    ENTITIES_FEATURES,
    HISTORY_FEATURES,
    BaseKb,
    KbBuilderConfig,
    KbTable,
    kb_registry,
)
from .transform import NAMES_FEATURES, NameView, nameview_registry

DATASET_NAME = "cellosaurus"

CELLOSAURUS_ENTITIES_FEATURES = ENTITIES_FEATURES.copy()

_CELLOSAURUES_COMMENT_FEATURES = [
    "anecdotal",
    "biotechnology",
    "breed/subspecies",
    "caution",
    "characteristics",
    "derived_from_metastatic_site",
    "derived_from_sampling_site",
    "discontinued",
    "donor_information",
    "doubling_time",
    "from",
    "genome_ancestry",
    "group",
    "hla_typing",
    "karyotypic_information",
    "knockout_cell",
    "microsatellite_instability",
    "miscellaneous",
    "misspelling",
    "monoclonal_antibody_isotype",
    "monoclonal_antibody_target",
    "omics",
    "part_of",
    "population",
    "problematic_cell_line",
    "registration",
    "selected_for_resistance_to",
    "senescence",
    "sequence_variation",
    "transfected_with",
    "transformant",
    "virology",
]


CELLOSAURUS_ENTITIES_FEATURES.update(
    {
        "alt_ids": Sequence(Value("string")),
        "xrefs": Sequence(Value("string")),
        "references": Sequence(Value("string")),
        "webpages": Sequence(Value("string")),
        "str_profile_data": Sequence(Value("string")),
        "diseases": Sequence(Value("string")),
        "taxonomy_ids": Sequence(Value("int32")),
        "taxonomy_scientific_names": Sequence(Value("string")),
        "taxonomy_common_names": Sequence(Value("string")),
        "hierarchy": Sequence(Value("string")),
        "originate_from_same_individual": Sequence(Value("string")),
        "sex_of_cell": Value("string"),
        "age_of_donor_at_sampling": Value("string"),
        "category": Value("string"),
        "entry_history": Value("string"),
        ###################################
        # EXTRACTED FROM COMMENTS
        ###################################
        **{k: Sequence(Value("string")) for k in _CELLOSAURUES_COMMENT_FEATURES},
    }
)


CELLOSAURUS_NAMES_FEATURES = {
    k: CELLOSAURUS_ENTITIES_FEATURES[k]
    for k in [
        "taxonomy_ids",
        "taxonomy_scientific_names",
        "taxonomy_common_names",
        "diseases",
        "category",
    ]
}

CELLOSAURUS_SPECIES_COLUMNS = [
    "taxonomy_common_names",
    "taxonomy_scientific_names",
]


def parse_entity(entry: dict) -> dict:
    if any("NCBI_TaxID=" not in s for s in entry["species"]):
        raise NotImplementedError(
            "Expected species to be linked to NCBI Taxonomy: this breaks parsing logic."
        )

    if "aliases" in entry:
        entry["aliases"] = [a.strip() for a in entry["aliases"].split(";")]

    if "xrefs" in entry:
        entry["xrefs"] = [
            ":".join([v.strip() for v in x.split(";")]) for x in entry["xrefs"]
        ]

    if "references" in entry:
        entry["references"] = [r.replace(";", "") for r in entry["references"]]

    if "alt_ids" in entry:
        entry["alt_ids"] = [i.strip() for i in entry["alt_ids"].split(";")]

    if "comments" in entry:
        for c in entry.pop("comments"):
            key, value = c.split(":", maxsplit=1)
            key = key.lower().replace(" ", "_")
            if key not in entry:
                entry[key] = []
            entry[key].append(value.strip())

    tax_ids, scientific_names, common_names = [], [], []
    for s in entry.pop("species"):
        tax_id, scientific_common_name = s.replace("!", "").split(";")

        tax_id = tax_id.strip().replace("NCBI_TaxID=", "")
        tax_ids.append(tax_id)

        scientific_name, common_name = scientific_common_name.split("(")

        scientific_name = scientific_name.strip()
        scientific_names.append(scientific_name)

        common_name = common_name.replace(")", "").strip()
        common_names.append(common_name)

    entry["taxonomy_ids"] = tax_ids
    entry["taxonomy_scientific_names"] = scientific_names
    entry["taxonomy_common_names"] = common_names

    entry["diseases"] = [d.split(";")[-1].strip() for d in entry.pop("diseases", [])]

    return entry


def stream_entities(path: str | Path) -> Iterator[dict]:
    # ---------  ---------------------------     ----------------------
    # Line code  Content                         Occurrence in an entry
    # ---------  ---------------------------     ----------------------
    # ID         Identifier (cell line name)     Once; starts an entry
    # AC         Accession (CVCL_xxxx)           Once
    # AS         Secondary accession number(s)   Optional; once
    # SY         Synonyms                        Optional; once
    # DR         Cross-references                Optional; once or more
    # RX         References identifiers          Optional: once or more
    # WW         Web pages                       Optional; once or more
    # CC         Comments                        Optional; once or more
    # ST         STR profile data                Optional; twice or more
    # DI         Diseases                        Optional; once or more
    # OX         Species of origin               Once or more
    # HI         Hierarchy                       Optional; once or more
    # OI         Originate from same individual  Optional; once or more
    # SX         Sex of cell                     Optional; once
    # AG         Age of donor at sampling        Optional; once
    # CA         Category                        Once
    # DT         Date (entry history)            Once
    # //         Terminator                      Once; ends an entry

    code_to_column = {
        "ID": "label",
        "AC": "id",
        "SY": "aliases",
        "AS": "alt_ids",
        "DR": "xrefs",
        "RX": "references",
        "WW": "webpages",
        "CC": "comments",
        "ST": "str_profile_data",
        "DI": "diseases",
        "OX": "species",
        "HI": "hierarchy",
        "OI": "originate_from_same_individual",
        "SX": "sex_of_cell",
        "AG": "age_of_donor_at_sampling",
        "CA": "category",
        "DT": "entry_history",
    }
    codes_with_list = ["DR", "RX", "WW", "CC", "ST", "DI", "OX", "HI", "OI"]

    entity: dict = {}
    with open(path, encoding="utf-8") as infile:
        for idx, line in enumerate(infile):
            if idx < 54:
                continue

            line = line.strip()

            if line == "//":
                yield parse_entity(entity)

                entity.clear()

                continue

            if "   " not in line:
                continue

            code, value = line.split("   ", maxsplit=1)

            key = code_to_column.get(code)

            if code in codes_with_list:
                if key not in entity:
                    entity[key] = []
                entity[key].append(value)
            elif key:
                entity[key] = value

    if entity:
        yield parse_entity(entity)


def stream_history(path: str | Path) -> Iterator[dict]:
    """
    Extract discontinued identifiers
    """

    with open(path, encoding="utf-8") as infile:
        for idx, line in enumerate(infile):
            if idx < 11:
                continue
            if line == "\n":
                continue

            obsolete, _ = line.strip().split("  ")

            yield {"obsolete": obsolete, "update": NIL}


class CellosaurusBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            return DatasetInfo(
                features=Features(CELLOSAURUS_ENTITIES_FEATURES),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.ENTITIES),
            )
        elif self.config.name == KbTable.HISTORY:
            return DatasetInfo(
                features=Features(HISTORY_FEATURES),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.HISTORY),
            )
        else:
            raise ValueError(f"Invalid `table={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            data_dir = Path(self.config.data_dir)
            gen_kwargs = {
                "file": data_dir / data_files["file"],
                "history_file": data_dir / data_files["history_file"],
            }
        else:
            downloaded = dl_manager.download_and_extract(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, file: Path, history_file: Path):
        if self.config.name == KbTable.ENTITIES:
            for idx, e in enumerate(stream_entities(path=file)):
                e["idx"] = idx
                yield idx, e
        elif self.config.name == KbTable.HISTORY:
            for idx, e in enumerate(stream_history(path=history_file)):
                yield (idx, e)


@dataclass(frozen=True)
class NameRecord:
    name: str
    type: str
    taxonomy_id: Optional[int] = None
    taxonomy_scientific_name: Optional[str] = None
    taxonomy_common_name: Optional[str] = None
    disease: Optional[str] = None


def _batch_explode_entities(
    batch: LazyBatch,
    indices: list[int],
    keep_columns: list[str],
    explode_species: bool = True,
    explode_diseases: bool = False,
) -> dict:
    out: dict[str, list] = {
        "entity_idx": [],
        "name": [],
        "type": [],
    }

    for column in keep_columns:
        out[column] = []

    if explode_species:
        out.update(
            {
                "taxonomy_id": [],
                "taxonomy_scientific_name": [],
                "taxonomy_common_name": [],
            }
        )

    if explode_diseases:
        out["disease"] = []

    for i in range(len(indices)):
        source_row_id = indices[i]

        aliases = batch["aliases"][i] or []
        aliases_type = (
            batch["aliases_type"][i]
            if "aliases_type" in batch
            else ["alias"] * len(aliases)
        )

        recs = [NameRecord(name=batch["label"][i], type="label")] + [
            NameRecord(name=a, type=t) for a, t in zip(aliases, aliases_type)
        ]

        if explode_species:
            taxids = batch["taxonomy_ids"][i]
            scientific_names = batch["taxonomy_scientific_names"][i]
            common_names = batch["taxonomy_common_names"][i]

            recs = [
                replace(
                    r,
                    taxonomy_id=tid,
                    taxonomy_scientific_name=sn,
                    taxonomy_common_name=cn,
                )
                for r in recs
                for tid, sn, cn in zip(taxids, scientific_names, common_names)
            ]

        if explode_diseases:
            ds_data = batch["diseases"][i] or [None]
            recs = [replace(r, disease=d) for r in recs for d in ds_data]

        for r in recs:
            out["entity_idx"].append(source_row_id)
            out["name"].append(r.name)
            out["type"].append(r.type)

            if explode_species:
                out["taxonomy_id"].append(r.taxonomy_id)
                out["taxonomy_scientific_name"].append(r.taxonomy_scientific_name)
                out["taxonomy_common_name"].append(r.taxonomy_common_name)

            if explode_diseases:
                out["disease"].append(r.disease)

            for column in keep_columns:
                out[column].append(batch[column][i])

    return dict(out)


@nameview_registry.register(DATASET_NAME)
class CellosaurusNameView(NameView):
    def __init__(
        self,
        *args,
        explode_species: bool = True,
        explode_diseases: bool = False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.keep_features.update(CELLOSAURUS_NAMES_FEATURES)
        self.explode_species = explode_species
        self.explode_diseases = explode_diseases

        self.species_column = (
            self.species_column
            if self.species_column is not None
            else "taxonomy_common_names"
        )
        self.species_column_fallback = (
            self.species_column_fallback
            if self.species_column_fallback is not None
            else "taxonomy_scientific_names"
        )

        for name, value in [
            ("species_column", self.species_column),
            ("species_column_fallback", self.species_column_fallback),
        ]:
            if value is not None and value not in CELLOSAURUS_SPECIES_COLUMNS:
                raise ValueError(
                    f"Invalid `{name}={value}, must be one of {CELLOSAURUS_SPECIES_COLUMNS}"
                )

        if self.explode_species:
            self.species_column = (
                self.species_column[:-1]
                if self.species_column is not None
                else self.species_column
            )
            self.species_column_fallback = (
                self.species_column_fallback[:-1]
                if self.species_column_fallback is not None
                else self.species_column_fallback
            )

    def convert_to_names(self, entities: Dataset) -> Dataset:
        features = NAMES_FEATURES.copy()

        if self.explode_species:
            for k in [
                "taxonomy_id",
                "taxonomy_scientific_name",
                "taxonomy_common_name",
            ]:
                features[k] = Value("string") if "name" in k else Value("int32")
                self.keep_features.pop(f"{k}s")

        if self.explode_diseases:
            features["disease"] = Value("string")
            self.keep_features.pop("diseases")

        features.update(self.keep_features)

        names = entities.map(
            _batch_explode_entities,
            fn_kwargs={
                "keep_columns": list(self.keep_features),
                "explode_species": self.explode_species,
                "explode_diseases": self.explode_diseases,
            },
            batched=True,
            with_indices=True,
            features=Features(features),
            remove_columns=entities.column_names,
            desc="BELB kb: convert entities to names",
        )

        return names


@kb_registry.register(DATASET_NAME)
class CellosaurusKb(BaseKb):
    dataset_name = DATASET_NAME
    has_history = True
    builder_class = CellosaurusBuilder

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        if self.name_transform is True:
            self.name_transform = CellosaurusNameView(
                explode_species=True,
                explode_diseases=False,
                species_column="taxonomy_common_names",
                species_column_fallback="taxonomy_scientific_names",
            )
