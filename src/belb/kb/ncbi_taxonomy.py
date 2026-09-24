import json
import tarfile
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
from datasets.formatting.formatting import LazyRow

from ..config import ResourceConfig
from ..utils import NIL, get_cache_dir
from .base import (
    ENTITIES_FEATURES,
    HISTORY_FEATURES,
    BaseKb,
    KbBuilderConfig,
    KbTable,
    kb_registry,
)

DATASET_NAME = "ncbi-taxonomy"

NCBI_TAXONOMY_FEATURES = ENTITIES_FEATURES.copy()
NCBI_TAXONOMY_FEATURES.update(
    {
        "label_type": Value("string"),
        "aliases_type": Sequence(Value("string")),
        "unique_names": Sequence(Value("string")),
        "general_names": Sequence(Value("string")),
    }
)


def parse_line(line: str) -> dict:
    # See: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_readme.txt
    # names.dmp
    # ---------
    # Taxonomy names file has these fields:
    #
    # 	tax_id					-- the id of node associated with this name
    # 	name_txt				-- name itself
    # 	unique name				-- the unique variant of this name if name not unique
    # 	name class				-- (synonym, common name, ...)

    elements = [e.strip() for e in line.strip().split("|")]
    identifier = elements[0]
    name = elements[1]
    unique_name = elements[2] if elements[2] != "" else None
    alias_type = elements[3]  # synonym type
    general_name = None

    # 7:2	|	Procaryotae	|	Procaryotae <bacteria>	|	in-part	|
    # 20506:2157	|	Procaryotae	|	Procaryotae <archaea>	|	in-part	|
    if name and unique_name:
        general_name = name
        name = unique_name

    out = {
        "id": identifier,
        "text": name,
        "type": alias_type,
        "unique_name": unique_name,
        "general_name": general_name,
    }

    return out


def update_entry(entry: dict, row: dict):
    entry["id"] = row["id"]

    name_type = row["type"].replace(" ", "_")

    if name_type == "scientific_name":
        entry["label"] = row["text"]
        entry["label_type"] = name_type
    else:
        if "aliases" not in entry:
            entry["aliases"] = []
            entry["aliases_type"] = []

        entry["aliases"].append(row["text"])
        entry["aliases_type"].append(name_type)

    if row.get("unique_name"):
        if "unique_names" not in entry:
            entry["unique_names"] = []
        entry["unique_names"].append(row["unique_name"])

    if row.get("general_name"):
        if "general_names" not in entry:
            entry["general_names"] = []
        entry["general_names"].append(row["general_name"])


def stream_lines(path: str | Path, name: str) -> Iterator[str]:
    """
    Extract from archive only file w/ names
    """
    with tarfile.open(path) as archive:
        member = archive.getmember(name)
        fp = archive.extractfile(member)
        if fp is None:
            raise ValueError(f"File `{name}` not found in archive `{archive}`!")
        for line in fp.readlines():
            yield line.decode("utf-8")


def parse_names(path: str | Path) -> Iterator[dict]:
    entry: dict = {}
    current_id = None

    for line in stream_lines(path=path, name="names.dmp"):
        row = parse_line(line)

        if row["text"] in ["all", "root"]:
            continue

        if row["id"] != current_id:
            if entry:
                yield entry
            entry = {}
            current_id = row["id"]

        update_entry(entry=entry, row=row)

    if entry:
        yield entry


def parse_history(path: str | Path) -> Iterator[dict]:

    for name in ["delnodes.dmp", "merged.dmp"]:
        for line in stream_lines(path=path, name=name):
            elems = [e.strip() for e in line.split("|")]
            elems = [e for e in elems if e != ""]

            if len(elems) == 1:
                old_identifier = elems[0]
                new_identifier = NIL
            elif len(elems) == 2:
                old_identifier = elems[0]
                new_identifier = elems[1]

            yield {"obsolete": old_identifier, "update": new_identifier}


class NcbiTaxonomyBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            return DatasetInfo(
                features=Features(NCBI_TAXONOMY_FEATURES),
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

    def _split_generators(self, dl_manager: DownloadManager):

        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            base_path = Path(self.config.data_dir)
            gen_kwargs = {k: base_path / v for k, v in data_files.items()}
        else:
            downloaded = dl_manager.download(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, tar_file: Path):

        if self.config.name == KbTable.ENTITIES:
            for idx, e in enumerate(parse_names(path=tar_file)):
                e["idx"] = idx
                yield idx, e
        elif self.config.name == KbTable.HISTORY:
            for i, e in enumerate(parse_history(path=tar_file)):
                yield i, e


@kb_registry.register(DATASET_NAME)
class NcbiTaxonomyKb(BaseKb):
    dataset_name = DATASET_NAME
    has_history = True
    builder_class = NcbiTaxonomyBuilder

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def _row_get_names(row: LazyRow):

    out = {"id": row["id"], "scientific_name": row["label"]}

    aliases = row.get("aliases") or []
    aliases_type = row.get("aliases_type") or []

    common_name = None
    for alias, alias_type in zip(aliases, aliases_type):
        if alias_type == "common_name":
            common_name = alias
            break
        if common_name is None and alias_type == "genbank_common_name":
            common_name = alias
            break

    out["common_name"] = common_name

    return out


def load_taxid_names(
    resource: ResourceConfig,
    num_proc: int | None = None,
) -> dict | None:

    loader = NcbiTaxonomyKb(num_proc=num_proc)

    kb = loader.load(resource=resource)

    entities = kb["entities"]

    features = {
        "id": Value("string"),
        "scientific_name": Value("string"),
        "common_name": Value("string"),
    }

    taxid_names = entities.map(
        _row_get_names,
        features=Features(features),
        remove_columns=entities.column_names,
        desc="ncbi-taxonomy: extract `id -> names` mapping",
        num_proc=num_proc,
    )

    cache_path = get_cache_dir(taxid_names) / "taxid_names.json"

    if not Path(cache_path).exists():
        out = {
            i: {"scientific_name": sn, "common_name": cn}
            for i, sn, cn in zip(
                taxid_names["id"],
                taxid_names["scientific_name"],
                taxid_names["common_name"],
            )
        }
        with cache_path.open("w") as fp:
            json.dump(out, fp)

    with cache_path.open("r") as fp:
        out = {int(k): v for k, v in json.load(fp).items()}

    return out
