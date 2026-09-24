from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from datasets import Dataset, Features, Value
from datasets.arrow_writer import ArrowWriter
from datasets.formatting.formatting import LazyBatch, LazyRow
from loguru import logger

from ..utils import get_cache_path, get_fingerprint

# NOTE: Manual fingerprint assignment after `load_from_disk` is required.
# Even if a fingerprint is provided to the Dataset constructor or saved in state.json,
# load_from_disk often recomputes it based on the data shards on disk.
# To ensure stable caching for subsequent .map() calls, we restore the logical fingerprint.
# Try:
# from datasets import Dataset, load_from_disk
# import pyarrow as pa
# ds = Dataset(pa.table({"a": [1]}), fingerprint="my_fp")
# ds.save_to_disk("test_ds")
# ds2 = load_from_disk("test_ds")
# print(ds2._fingerprint)  # Output will be recomputed, not "my_fp"


NAMES_FEATURES = {
    "id": Value("string"),
    "entity_idx": Value("int64"),
    "name": Value("string"),  # label or alias
    "type": Value("string"),
}

ALIAS_PICKERS = ["min_len", "max_len"]


class NameViewRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, name: str):
        def decorator(cls: type["NameView"]):
            self._registry[name] = cls
            return cls

        return decorator

    def get(self, name: str) -> type["NameView"]:
        if name not in self._registry:
            # Fallback to base NameTransform if not specifically registered
            return NameView
        return self._registry[name]


nameview_registry = NameViewRegistry()


def reindex(ds: Dataset) -> Dataset:
    if "idx" in ds.column_names:
        ds = ds.remove_columns("idx")
    ds = ds.add_column("idx", range(len(ds)))
    return ds


def mark_homonyms(names: Dataset, name_column: str = "name") -> Dataset:
    # 1. If the column already exists, remove it so we can recalculate
    if "is_homonym" in names.column_names:
        names = names.remove_columns("is_homonym")

    table = names.data.table.select([name_column, "id"])
    grouped = table.group_by(name_column).aggregate([("id", "count_distinct")])
    count_column = "id_count_distinct"

    homonyms = grouped.filter(pc.greater(grouped[count_column], 1))  # type: ignore
    homonym_names = homonyms[name_column]

    # Force the value_set to be a string array
    # This prevents the "string vs null" mismatch when homonym_names is empty
    value_set = pa.array(homonym_names, type=pa.string())
    is_homonym = pc.is_in(table[name_column], value_set=value_set)  # type: ignore

    return names.add_column("is_homonym", is_homonym)


def left_join(
    left: Dataset,
    right: Dataset,
    left_on: str,
    right_on: str,
    columns: list[str] | None = None,
) -> Dataset:
    """
    Perform a left join between two datasets using DuckDB.
    """
    if columns is None:
        columns = [c for c in right.column_names if c != right_on]

    if not all(c in right.column_names for c in columns):
        raise ValueError(f"All `columns` ({columns}) must be in `right.column_names`")

    fingerprint = get_fingerprint(
        {
            "left": left._fingerprint,
            "right": right._fingerprint,
            "left_on": left_on,
            "right_on": right_on,
            "columns": sorted(columns),
        }
    )

    cache_path = get_cache_path(fingerprint=fingerprint, ds=left)

    if not Path(cache_path).exists():
        if len(left) != left.data.table.num_rows:
            left = left.flatten_indices()

        if len(right) != right.data.table.num_rows:
            right = right.flatten_indices()

        query_parts = ["SELECT\n"]
        for c in left.column_names:
            query_parts.append(f"\tl.{c},\n")
        for i, c in enumerate(columns):
            line = f"\tr.{c}"
            if i != len(columns) - 1:
                line += ",\n"
            else:
                line += "\n"
            query_parts.append(line)
        query_parts.append("FROM left_table l\n")
        query_parts.append(f"LEFT JOIN right_table r ON l.{left_on} = r.{right_on}")
        query = "".join(query_parts)

        conn = duckdb.connect()
        conn.register("left_table", left.data.table)
        conn.register("right_table", right.data.table)
        enriched_table = conn.execute(query).to_arrow_table()
        conn.close()

        with ArrowWriter(path=cache_path, fingerprint=fingerprint) as writer:
            writer.write_table(enriched_table)

        return Dataset.from_file(cache_path)

    return Dataset.from_file(cache_path)


def merge_augmented_names(names: Dataset, expanded: Dataset) -> Dataset:
    fingerprint = get_fingerprint(
        {
            "names": names._fingerprint,
            "expanded": expanded._fingerprint,
        }
    )

    cache_path = get_cache_path(fingerprint=fingerprint, ds=names)

    if not Path(cache_path).exists():
        select_columns = []
        for column in names.column_names:
            if column == "name":
                select_columns.append("COALESCE(r.name, n.name) AS name")
            else:
                select_columns.append(f"n.{column}")
        select_sql = ", ".join(select_columns)

        conn = duckdb.connect()
        conn.register("names", names.data.table)
        conn.register("expanded", expanded.data.table)
        merged_table = conn.execute(f"""
            SELECT
                {select_sql}
            FROM names n
            LEFT JOIN expanded r USING (idx)
            """).to_arrow_table()
        conn.close()

        with ArrowWriter(path=cache_path, fingerprint=fingerprint) as writer:
            writer.write_table(merged_table)

        return Dataset.from_file(cache_path)

    return Dataset.from_file(cache_path)


def is_homonym_filter(batch: LazyBatch):
    return batch["is_homonym"]


def _batch_explode_entities(
    batch: LazyBatch,
    indices: list[int],
    keep_columns: list[str],
) -> dict:
    out: dict[str, list] = {"entity_idx": [], "name": [], "type": []}
    for column in keep_columns:
        out[column] = []

    for i in range(len(indices)):
        source_row_id = indices[i]

        aliases = batch["aliases"][i] or []
        if aliases:
            aliases_type = (
                batch["aliases_type"][i]
                if "aliases_type" in batch
                else ["alias"] * len(aliases)
            )
        else:
            aliases_type = []

        names_types = [
            (batch["label"][i], "label"),
            *list(zip(aliases, aliases_type)),
        ]

        for name, name_type in names_types:
            out["entity_idx"].append(source_row_id)
            out["name"].append(name)
            out["type"].append(name_type)
            for column in keep_columns:
                out[column].append(batch[column][i])

    return dict(out)


def pick_alias(aliases: list[str], mode: str) -> str:
    if mode == "max_len":
        out = max(sorted(aliases), key=lambda x: len(x))
    elif mode == "min_len":
        out = min(sorted(aliases), key=lambda x: len(x))
    else:
        raise NotImplementedError(f"`pick_alias` with `mode={mode}` not implemented")
    return out


def get_disambiguation(
    row: LazyRow, alias_picker: str, columns: list[str]
) -> str | None:
    name_type = row["type"]

    base = None
    if name_type == "label":
        if row.get("aliases"):
            base = pick_alias(aliases=row["aliases"], mode=alias_picker)
    else:
        base = row["label"]

    values = [base] if base is not None else []
    for column in columns:
        value = row.get(column)
        if not value:
            continue
        if isinstance(value, list):
            value = ",".join(value)
        values.append(f"{column}:{value}")

    if values:
        disambiguation = ";".join(values)
        disambiguation = f"({disambiguation})"
    else:
        disambiguation = None

    return disambiguation


def _row_augment_name(
    row: LazyRow,
    alias_picker: str,
    columns: list[str],
):
    name = row["name"]

    disambiguation = get_disambiguation(
        row=row, alias_picker=alias_picker, columns=columns
    )

    if disambiguation:
        species_idx = name.find("[")
        # add before species information
        if species_idx > -1:
            species = name[species_idx:]
            augmented = f"{name[:species_idx-1]} {disambiguation} {species}"
        else:
            augmented = f"{name} {disambiguation}"
    else:
        augmented = name

    row["name"] = augmented

    return row


def _batch_add_species(batch: LazyBatch, column: str, column_fallback: str):
    names = []
    for i, name in enumerate(batch["name"]):
        species = batch[column][i]
        if species is None:
            species = batch[column_fallback][i]

        if species is not None:
            species = species if isinstance(species, str) else ",".join(species)
            name = f"{name} [{species}]"

        names.append(name)

    batch["name"] = names

    return batch


class NameView:
    def __init__(
        self,
        keep_features: dict | None = None,
        disambiguation: bool = True,
        disambiguation_columns: list[str] | None = None,
        alias_picker: str = "max_len",
        species_column: str | None = None,
        species_column_fallback: str | None = None,
    ):
        # which column in `entities` to keep in `names`
        self.keep_features = keep_features if keep_features is not None else {}
        self.keep_features["id"] = Value("string")

        # disambiguate homonyms
        self.disambiguation = disambiguation

        # which columns to use for disambiguateion (besides label/aliases)
        self.disambiguation_columns = sorted(
            set(disambiguation_columns) if disambiguation_columns else set()
        )
        self.disambiguation_columns = sorted(
            {c for c in self.disambiguation_columns if c not in ["label", "aliases"]}
        )

        # which alias to pick in case homonym is label (primary name)
        self.alias_picker = alias_picker
        if self.alias_picker not in ALIAS_PICKERS:
            raise ValueError(
                f"Invalid `alias_picker={self.alias_picker}, it must be one of {tuple(ALIAS_PICKERS)}"
            )

        self.species_column = species_column
        self.species_column_fallback = species_column_fallback

    def convert_to_names(self, entities: Dataset) -> Dataset:
        features = NAMES_FEATURES.copy()
        features.update(self.keep_features)

        names = entities.map(
            _batch_explode_entities,
            fn_kwargs={"keep_columns": list(self.keep_features)},
            batched=True,
            with_indices=True,
            features=Features(features),
            remove_columns=entities.column_names,
            desc="BELB kb: convert entities to names",
        )

        return names

    def augment_names(self, homonyms: Dataset, num_proc: int | None = None):
        expanded = homonyms.map(
            _row_augment_name,
            fn_kwargs={
                "columns": self.disambiguation_columns,
                "alias_picker": self.alias_picker,
            },
            remove_columns=[
                column
                for column in homonyms.column_names
                if column not in {"idx", "name"}
            ],
            num_proc=num_proc,
            desc="BELB kb: augment homonyms",
        )

        return expanded

    def add_species(
        self,
        names: Dataset,
        column: str,
        column_fallback: str | None = None,
        num_proc: int | None = None,
    ):
        names = names.map(
            _batch_add_species,
            fn_kwargs={"column": column, "column_fallback": column_fallback},
            batched=True,
            num_proc=num_proc,
            desc="BELB kb: add species to names",
        )
        return names

    def get_homonyms(
        self, names: Dataset, entities: Dataset, num_proc: int | None = None
    ):
        names = mark_homonyms(names)

        homonyms = names.filter(
            is_homonym_filter,
            batched=True,
            num_proc=num_proc,
            desc="BELB kb: filter homonyms",
        )

        homonyms = left_join(
            left=homonyms,
            right=entities,
            left_on="entity_idx",
            right_on="idx",
            columns=["label", "aliases"],
        )

        return homonyms

    def disambiguate(
        self,
        names: Dataset,
        entities: Dataset,
        verbose: bool = True,
        num_proc: int | None = None,
    ) -> tuple[Dataset, dict]:
        if self.species_column is not None:
            names = self.add_species(
                names=names,
                column=self.species_column,
                column_fallback=self.species_column_fallback,
                num_proc=num_proc,
            )

        homonyms = self.get_homonyms(names=names, entities=entities, num_proc=num_proc)

        tot = len(homonyms)

        expanded = self.augment_names(homonyms=homonyms)

        expanded = merge_augmented_names(names=names, expanded=expanded)

        expanded = mark_homonyms(names=expanded)

        unresolved = sum(expanded["is_homonym"])

        resolved = tot - unresolved

        if verbose:
            logger.debug(
                "BELB kb: resolved {}% of homonyms ({}/{})",
                round((resolved / tot) * 100, 2),
                resolved,
                tot,
            )

        metadata = {"total": tot, "unresolved": unresolved}

        return reindex(expanded), metadata

    def apply(
        self,
        entities: Dataset,
        num_proc: int | None = None,
    ) -> Dataset:
        required_columns = ["id", "label", "aliases"]

        if not isinstance(entities, Dataset):
            raise ValueError("`entities` must be an instance of `Dataset`")

        if not all(c in entities.column_names for c in required_columns):
            raise ValueError(
                f"`entities.column_names` must contain must `{required_columns}`"
            )

        names = self.convert_to_names(entities=entities)

        if "idx" not in names.column_names:
            names = names.add_column("idx", range(len(names)))

        if self.disambiguation:
            names, _ = self.disambiguate(
                names=names, entities=entities, num_proc=num_proc
            )
        else:
            names = mark_homonyms(names)
            num_homonyms = sum(names["is_homonym"])
            if num_homonyms > 0:
                logger.warning(
                    "`{}` with `disambiguation=False` produced {} homonyms (same name, different entity). ",
                    self.__class__.__name__,
                    num_homonyms,
                )

        return names
