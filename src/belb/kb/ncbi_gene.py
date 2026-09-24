from collections.abc import Iterator
from importlib.resources import files
from pathlib import Path

import pandas as pd
from datasets import (
    Dataset,
    DatasetDict,
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
from datasets.formatting.formatting import LazyBatch, LazyRow
from loguru import logger

from ..utils import NIL, load_yaml_asset
from .base import (
    ENTITIES_FEATURES,
    HISTORY_FEATURES,
    BaseKb,
    KbBuilderConfig,
    KbTable,
    kb_registry,
)
from .transform import NameView, mark_homonyms, nameview_registry

DATASET_NAME = "ncbi-gene"

NCBI_GENE_SUBSETS = load_yaml_asset(
    files("belb.assets.subsets") / f"{DATASET_NAME}.yaml"
)

NCBI_GENE_SUBSETS["gnormplus-nlm-gene"] = set(NCBI_GENE_SUBSETS["gnormplus"]).union(
    set(NCBI_GENE_SUBSETS["nlm-gene"])
)

NCBI_GENE_IDS = load_yaml_asset(files("belb.assets.ids") / f"{DATASET_NAME}.yaml")


NCBI_GENE_FEATURES = ENTITIES_FEATURES.copy()
NCBI_GENE_FEATURES.update(
    {
        "taxonomy_id": Value("int32"),
        "taxonomy_scientific_name": Value("string"),
        "taxonomy_common_name": Value("string"),
        "aliases_type": Sequence(Value("string")),
        "description": Value("string"),
        "locustag": Value("string"),
        "xrefs": Sequence(Value("string")),
        "chr": Value("string"),
        "map_location": Value("string"),
        "type_of_gene": Value("string"),
        "nomenclature_status": Value("string"),
        "modification_date": Value("string"),
        "feature_types": Sequence(Value("string")),
    }
)


NCBI_GENE_NAMES_FEATURES = {
    k: NCBI_GENE_FEATURES[k]
    for k in [
        "taxonomy_id",
        "taxonomy_common_name",
        "taxonomy_scientific_name",
        "locustag",
        "chr",
        "map_location",
        "type_of_gene",
    ]
}

NCBI_GENE_SPECIES_COLUMNS = ["taxonomy_common_name", "taxonomy_scientific_name"]


def parse_subset(subset: str | None = None) -> str | None:
    if subset is not None and subset not in NCBI_GENE_SUBSETS:
        raise ValueError(
            f"Invalid subset `{subset}`. Must be one of {list(NCBI_GENE_SUBSETS)}"
        )

    return subset


def is_invalid_name(name: str):
    empty_entry_text = [
        "when different from all specified ones in Gene.",
        "Record to support submission of GeneRIFs for a gene not in Gene",
    ]

    newentry = name == "NEWENTRY"
    empty = name == ""
    text_comment = any(e in name for e in empty_entry_text)
    return any([newentry, empty, text_comment])


def get_aliases(row: dict) -> tuple[list[str], list[str]]:
    label = row["Symbol"]

    columns = [
        "Synonyms",
        "description",
        "Symbol_from_nomenclature_authority",
        "Full_name_from_nomenclature_authority",
        "Other_designations",
    ]

    aliases, aliases_type = [], []
    for column in columns:
        names_string = row.get(column, "-")
        if names_string == "-":
            continue

        names = [a.replace("'", "") for a in names_string.split("|")]
        names = [a for a in names if not is_invalid_name(a)]
        for a in names:
            if a != label and a not in aliases:
                aliases.append(a)
                aliases_type.append(column.lower())

    return aliases, aliases_type


def get_extra_features(row: dict) -> dict:
    """
    Get gene `attribute`
    """

    columns = [
        "LocusTag",
        "chromosome",
        "map location",
        "dbXrefs",
        "type of gene",
        "Nomenclature_status",
        "Modification_date",
        "Feature_type",
        "#tax_id",
    ]

    extra: dict = {}
    for column in columns:
        value = row.get(column, "-")
        if value != "-":
            column = "chr" if column == "chromosome" else column
            extra[column.lower()] = value

    extra["taxonomy_id"] = int(extra.pop("#tax_id"))

    if "feature_type" in extra:
        extra["feature_types"] = [
            f.strip() for f in extra.pop("feature_type").split("|")
        ]

    if "dbxrefs" in extra:
        extra["xrefs"] = [f.strip() for f in extra.pop("dbxrefs").split("|")]

    return extra


def parse_gene_info_row(row: dict) -> dict | None:
    label = row["Symbol"]

    if is_invalid_name(label):
        return None

    entry = {
        "id": str(row["GeneID"]),
        "label": label,
    }

    aliases, aliases_type = get_aliases(row=row)
    if aliases:
        entry["aliases"] = aliases
        entry["aliases_type"] = aliases_type

    entry.update(get_extra_features(row))

    return entry


def parse_gene_info(path: Path) -> Iterator[dict]:
    reader = pd.read_csv(
        path,
        chunksize=10_000,
        sep="\t",
        na_filter=False,
        low_memory=False,
        compression="gzip",
    )

    for chunk in reader:
        for row in chunk.to_dict("records"):
            entry = parse_gene_info_row(row)
            if entry is not None:
                yield entry


def parse_gene_history(path: Path) -> Iterator[dict]:
    reader = pd.read_csv(
        path,
        chunksize=10_000,
        sep="\t",
        na_filter=False,
        compression="gzip",
    )

    for chunk in reader:
        for row in chunk.to_dict("records"):
            update = row["GeneID"]
            update = str(update) if update != "-" else NIL
            yield {
                "obsolete": str(row["Discontinued_GeneID"]),
                "update": update,
            }


class NcbiGeneKbBuilderConfig(KbBuilderConfig):
    def __init__(
        self, taxid_names: dict | None = None, taxid_map: dict | None = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.taxid_names = taxid_names
        self.taxid_map = (
            taxid_map if taxid_map is not None else NCBI_GENE_IDS["taxonomy_id"]
        )

        if (
            self.name == KbTable.ENTITIES
            and self.taxid_names is None
            and self.data_dir is not None
        ):
            raise ValueError(
                f"`{self.__class__.__name__}` was initialized with `taxid_names=None`, but has no species names columns ({tuple(NCBI_GENE_SPECIES_COLUMNS),}). "
                "Species information is necessary for downstream use of this KB. "
                "Use `load_taxid_names` to load `taxid_names` and use it during initialization (e.g., as kwarg to `load_kb`)."
            )


class NcbiGeneBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = NcbiGeneKbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            return DatasetInfo(
                features=Features(NCBI_GENE_FEATURES),
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
            raise ValueError(f"Invalid `name={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            base_path = Path(self.config.data_dir)
            gen_kwargs = {k: base_path / v for k, v in data_files.items()}
        else:
            downloaded = dl_manager.download(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, **kwargs):
        if self.config.name == KbTable.ENTITIES:
            yield from self._generate_entities(
                info_file=kwargs["info_file"],
                summary_file=kwargs["summary_file"],
            )
        elif self.config.name == KbTable.HISTORY:
            yield from self._generate_history(history_file=kwargs["history_file"])

    def _generate_entities(self, info_file: Path, summary_file: Path):
        df_summary = pd.read_csv(summary_file, sep="\t")
        id_to_desc = {k: v for k, v in zip(df_summary["GeneID"], df_summary["Summary"])}
        idx = 0
        for e in parse_gene_info(path=info_file):
            gene_id_int = int(e["id"])
            desc = id_to_desc.get(gene_id_int)
            if desc:
                e["description"] = desc

            taxid = e["taxonomy_id"]
            taxonomy_scientific_name = None
            taxonomy_common_name = None

            taxid = self.config.taxid_map.get(taxid, taxid)
            if taxid != NIL:
                species_names = self.config.taxid_names[taxid]
                taxonomy_scientific_name = species_names["scientific_name"]
                taxonomy_common_name = species_names["common_name"]

            e["idx"] = idx
            e["taxonomy_scientific_name"] = taxonomy_scientific_name
            e["taxonomy_common_name"] = taxonomy_common_name

            yield idx, e

            idx += 1

    def _generate_history(self, history_file: Path):
        for i, e in enumerate(parse_gene_history(history_file)):
            yield i, e


def _batch_filter_subset(batch: LazyBatch, taxonomy_ids: set[int]):
    return [taxonomy_id in taxonomy_ids for taxonomy_id in batch["taxonomy_id"]]


def _insert_extension(base: str, insertion: str) -> str:
    """
    Insert a string into another string based on bracket presence.
    1. Before the first closing bracket ')' if present.
    2. Before the first opening square bracket '[' if ')' is absent.
    3. Appended to the end if neither is present.
    """
    idx = base.find(")")
    if idx == -1:
        idx = base.find("[")

    if idx == -1:
        return base + insertion

    return base[:idx] + insertion + base[idx:]


def _row_extend_disambiguation(row: LazyRow, column: str):
    if row["is_homonym"]:
        name = row["name"]
        insertion = row[column]
        if insertion is not None:
            insertion = f"{column}:{insertion}"
            idx = name.find(")")
            if idx > -1:
                idx = name.find(")")
                name = name[:idx] + f",{insertion}" + name[idx:]
            else:
                idx = name.find("[")
                insertion = f" ({insertion})"
                if idx > -1:
                    name = name[:idx] + insertion + name[idx:]
                else:
                    name = name + insertion

        row["name"] = name

    return row


@nameview_registry.register(DATASET_NAME)
class NcbiGeneNameView(NameView):
    def __init__(self, *args, extra_disambiguation_column: str = "chr", **kwargs):
        super().__init__(*args, **kwargs)
        self.keep_features.update(NCBI_GENE_NAMES_FEATURES)
        self.species_column = (
            self.species_column
            if self.species_column is not None
            else "taxonomy_common_name"
        )
        self.species_column_fallback = (
            self.species_column_fallback
            if self.species_column_fallback is not None
            else "taxonomy_scientific_name"
        )
        for name, value in [
            ("species_column", self.species_column),
            ("species_column_fallback", self.species_column_fallback),
        ]:
            if value is not None and value not in NCBI_GENE_SPECIES_COLUMNS:
                raise ValueError(
                    f"Invalid `{name}={value}, must be one of {NCBI_GENE_SPECIES_COLUMNS}"
                )
        self.extra_disambiguation_column = extra_disambiguation_column

    def disambiguate(
        self,
        names: Dataset,
        entities: Dataset,
        verbose: bool = True,
        num_proc: int | None = None,
    ) -> Dataset:
        names, metadata = super().disambiguate(
            names=names, entities=entities, num_proc=num_proc, verbose=False
        )

        tot = metadata["total"]

        names = names.map(
            _row_extend_disambiguation,
            fn_kwargs={"column": self.extra_disambiguation_column},
            num_proc=num_proc,
            desc="ncbi-gene: name view: refine disambiguation",
        )

        names = mark_homonyms(names)

        unresolved = sum(names["is_homonym"])
        metadata["unresolved"] = unresolved

        resolved = metadata["total"] - unresolved

        if verbose:
            logger.debug(
                "BELB kb: resolved {}% of homonyms ({}/{})",
                round((resolved / tot) * 100, 2),
                resolved,
                tot,
            )

        return names, metadata


@kb_registry.register(DATASET_NAME)
class NcbiGeneKb(BaseKb):
    dataset_name = DATASET_NAME
    has_history = True
    builder_class = NcbiGeneBuilder

    def __init__(
        self,
        subset: str | None = None,
        taxid_names: dict[int, str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.subset = parse_subset(subset=subset)

        if self.dedup_columns is not None:
            self.dedup_columns = sorted(set(self.dedup_columns + ["taxonomy_id"]))

        self.taxid_names = taxid_names

    def _get_builder_kwargs(self, table: KbTable, kwargs: dict) -> dict:
        if table == KbTable.ENTITIES:
            kwargs["taxid_names"] = self.taxid_names
        return kwargs

    def _postprocess(self, ds: DatasetDict) -> DatasetDict:
        entities = ds[KbTable.ENTITIES]
        if self.subset is not None:
            entities = entities.filter(
                _batch_filter_subset,
                fn_kwargs={"taxonomy_ids": sorted(NCBI_GENE_SUBSETS[self.subset])},
                batched=True,
                num_proc=self.num_proc,
                desc=f"ncbi-gene: subset `{self.subset}` (filter by `taxonomy_id`)",
            )
            entities = self.reindex(entities)

        ds[KbTable.ENTITIES] = entities
        return ds
