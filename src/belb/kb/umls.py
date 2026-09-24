import gzip
from collections.abc import Iterator
from pathlib import Path

from datasets import (
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
from loguru import logger

from ..utils import NIL
from .base import (
    ENTITIES_FEATURES,
    HISTORY_FEATURES,
    BaseKb,
    KbBuilderConfig,
    KbTable,
    kb_registry,
)

DATASET_NAME = "umls"

UMLS_ENTITIES_FEATURES = ENTITIES_FEATURES.copy()
UMLS_ENTITIES_FEATURES.update(
    {
        "language": Value("string"),
        "label_type": Value("string"),
        "aliases_type": Sequence(Value("string")),
        "xrefs": Sequence(Value("string")),
        "description": Value("string"),
        "semantic": [
            {
                "type": Value("string"),
                "type_code": Value("string"),
                "group": Value("string"),
                "group_symbol": Value("string"),
            }
        ],
    }
)


def parse_row(line: str, columns: list[str]) -> dict[str, str] | None:
    # UMLS RRF files use '|' as a separator and have a trailing '|'
    line = line.rstrip("\n")
    values = line.split("|")
    if line.endswith("|"):
        values = values[:-1]

    if len(values) != len(columns):
        logger.debug(
            "Skip row with {} columns ({} expected): {}",
            len(values),
            len(columns),
            line,
        )
        return None

    return dict(zip(columns, values))


def build_entity(
    cui: str,
    language: str,
    rows: list[dict[str, str]],
    semantic: list[dict] | None = None,
    description: str | None = None,
) -> dict:
    entry: dict = {"id": cui, "label": None, "aliases": []}

    if description is not None:
        entry["description"] = description

    if semantic is not None:
        entry["semantic"] = semantic

    label = None
    aliases = []
    aliases_type = []
    xrefs = set()
    for row in rows:
        name = row.get("STR")
        name_type = row.get("TTY")
        if name is None or name_type is None:
            continue

        if row.get("ISPREF") == "Y":
            label = name
            entry["label"] = label
            entry["label_type"] = name_type
        else:
            aliases.append(name)
            aliases_type.append(name_type)

        xref_source, xref_id = row.get("SAB"), row.get("CODE")
        if xref_source is not None and xref_id is not None:
            xrefs.add(f"{xref_source}:{xref_id}")

    entry["aliases"] = aliases
    entry["aliases_type"] = aliases_type
    entry["xrefs"] = sorted(list(xrefs))
    entry["language"] = language

    return entry


def load_semgroups(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    with open(path) as fp:
        for line in fp:
            # CHEM|Chemicals & Drugs|T196|Element, Ion, or Isotope
            line = line.strip()
            if not line:
                continue
            sgc, sg, stc, st = line.split("|")

            out[stc] = {"type": st, "type_code": stc, "group": sg, "group_symbol": sgc}

    return out


def load_mrdef(path: Path) -> dict[str, str]:
    out = {}
    with gzip.open(path, "rt", encoding="utf-8") as fp:
        for line in fp:
            # C0000084|A0016576|AT38151982||MSH|Found in various tissues...
            line = line.rstrip("\n")
            elems = line.split("|")
            out[elems[0]] = elems[5]
    return out


def load_mrsty(path: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    with gzip.open(path, "rt") as fp:
        for line in fp:
            # C0000039|T109|||||
            line = line.rstrip("\n")
            elems = line.split("|")
            cui = elems[0]
            semantic_type_code = elems[1]

            if cui not in out:
                out[cui] = []

            out[cui].append(semantic_type_code)
    return out


def load_cui_to_semantic(semgroups: Path, mrsty: Path):
    stc_to_semantic = load_semgroups(semgroups)
    cui_to_stcs = load_mrsty(mrsty)
    cui_to_semantic = {
        cui: [stc_to_semantic[stc] for stc in stcs] for cui, stcs in cui_to_stcs.items()
    }
    return cui_to_semantic


def parse_language(language: str | None = "ENG") -> str | None:
    if language is not None:
        language = language.strip().upper()
        if not language:
            raise ValueError("`language` cannot be empty")
    return language


def parse_mrcui(path: Path):
    # https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.retired_cui_mapping_file_mrcui_rr/
    columns = [
        "CUI1",
        "VER",
        "REL",
        "RELA",
        "MAPREASON",
        "CUI2",
        "MAPIN",
    ]
    with gzip.open(path, "rt") as infile:
        for line in infile:
            row = parse_row(line=line, columns=columns)

            if row is None:
                continue

            obsolete = row.get("CUI1")
            if not obsolete:
                continue

            relation = row.get("REL")
            if relation == "SY":
                update = row.get("CUI2") or NIL
            elif relation == "DEL":
                update = NIL
            else:
                continue

            yield {"obsolete": obsolete, "update": update}


def parse_umls(
    mrconso_aa: Path,
    mrconso_ab: Path,
    mrdef: Path,
    mrsty: Path,
    semgroups: Path,
) -> Iterator[dict]:
    cui_to_description = load_mrdef(mrdef)
    cui_to_semantic = load_cui_to_semantic(semgroups=semgroups, mrsty=mrsty)

    # NOTE: see Table 1 here for description:
    # https://www.ncbi.nlm.nih.gov/books/NBK9685/#ch03.sec3.3.4
    columns = [
        "CUI",
        "LAT",
        "TS",
        "LUI",
        "STT",
        "SUI",
        "ISPREF",
        "AUI",
        "SAUI",
        "SCUI",
        "SDUI",
        "SAB",
        "TTY",
        "CODE",
        "STR",
        "SRL",
        "SUPPRESS",
        "CVF",
    ]

    current_cui = None
    current_lat = None
    rows: list[dict[str, str]] = []

    for file in [mrconso_aa, mrconso_ab]:
        with gzip.open(file, "rt", encoding="utf-8") as fp:
            for line in fp:
                row = parse_row(line=line, columns=columns)

                if row is None:
                    continue

                cui = row.get("CUI")
                lat = row.get("LAT")
                if not cui or not lat:
                    continue

                if current_cui is None:
                    current_cui = cui
                    current_lat = lat

                if cui != current_cui or lat != current_lat:
                    assert current_lat is not None
                    entity = build_entity(
                        cui=current_cui,
                        language=current_lat,
                        rows=rows,
                        semantic=cui_to_semantic.get(current_cui),
                        description=cui_to_description.get(current_cui),
                    )
                    yield entity

                    current_cui = cui
                    current_lat = lat
                    rows = []

                rows.append(row)

    if current_cui is not None:
        assert current_lat is not None
        entity = build_entity(
            cui=current_cui,
            language=current_lat,
            rows=rows,
            semantic=cui_to_semantic.get(current_cui),
            description=cui_to_description.get(current_cui),
        )
        yield entity


class UmlsBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            return DatasetInfo(
                features=Features(UMLS_ENTITIES_FEATURES),
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
            gen_kwargs = {
                k: Path(self.config.data_dir) / v for k, v in data_files.items()
            }
        else:
            raise ValueError(f"{DATASET_NAME} is local only: specify `data_dir`")

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, **kwargs):
        if self.config.name == KbTable.ENTITIES:
            for idx, e in enumerate(
                parse_umls(
                    mrconso_aa=kwargs["mrconso_aa"],
                    mrconso_ab=kwargs["mrconso_ab"],
                    mrdef=kwargs["mrdef"],
                    mrsty=kwargs["mrsty"],
                    semgroups=kwargs["semgroups"],
                )
            ):
                e["idx"] = idx
                yield idx, e
        elif self.config.name == KbTable.HISTORY:
            for idx, e in enumerate(parse_mrcui(path=kwargs["mrcui"])):
                yield idx, e


@kb_registry.register(DATASET_NAME)
class UmlsKb(BaseKb):
    dataset_name = DATASET_NAME
    has_history = True
    builder_class = UmlsBuilder

    def __init__(self, language: str | None = "ENG", **kwargs):
        super().__init__(**kwargs)
        self.language = parse_language(language=language)

    def _postprocess(self, ds: DatasetDict) -> DatasetDict:
        entities = ds[KbTable.ENTITIES]
        if self.language is not None:
            entities = entities.filter(
                lambda x: x["language"] == self.language,
                num_proc=self.num_proc,
                desc=f"{DATASET_NAME}: filter by language ({self.language})",
            )
            entities = self.reindex(entities)

        ds[KbTable.ENTITIES] = entities
        return ds


# NOTE: Add relationships like here?
# https://github.com/cambridgeltl/sapbert/blob/main/training_data/generate_pretraining_data.ipynb
# This introduces `identifier duplicates` (different id, same synset).
# logger.info("Adding drug tradenames...")
# tradename_relations = self.get_tradename_relations(directory)
# for head, tail in tradename_relations.items():
#     if tail in cui_to_names:
#         cui_to_names[head] = cui_to_names[tail]

# def get_tradename_relations(self, directory: str) -> dict:
# """
# Load relations of type `tradename` among CUIs
# """
#
# relations: dict = {}
#
# relation_types = ["has_tradename", "tradename_of"]
#
# with open(os.path.join(directory, "MRREL.RRF")) as infile:
#     for line in infile:
#         if any(r in line for r in relation_types):
#             elems = line.split("|")
#             head, tail = elems[0], elems[4]
#             relations[head] = tail
#
# return relations
