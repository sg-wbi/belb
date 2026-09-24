from abc import ABC
from enum import StrEnum

import xxhash
from datasets import (
    BuilderConfig,
    Dataset,
    DatasetDict,
    GeneratorBasedBuilder,
    Sequence,
    Split,
    Value,
    load_dataset,
)
from datasets.exceptions import DatasetNotFoundError
from datasets.formatting.formatting import LazyRow
from tqdm import tqdm

from ..config import DataFile, ResourceConfig
from ..utils import HF_USER
from .transform import NameView, reindex

ENTITIES_FEATURES = {
    "idx": Value("int64"),
    "id": Value("string"),
    "label": Value("string"),  # primary name
    "aliases": Sequence(Value("string")),
}

HISTORY_FEATURES = {
    "obsolete": Value("string"),
    "update": Value("string"),
}


class KbTable(StrEnum):
    ENTITIES = "entities"
    HISTORY = "history"
    NAMES = "names"


class KbRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, name: str):
        def decorator(cls: type["BaseKb"]):
            self._registry[name] = cls
            return cls

        return decorator

    def get(self, name: str) -> type["BaseKb"]:
        if name not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise ValueError(f"KB '{name}' not found. Available: {available}")
        return self._registry[name]


kb_registry = KbRegistry()


class KbBuilderConfig(BuilderConfig):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        try:
            KbTable(self.name)
        except ValueError:
            allowed = tuple(str(t) for t in KbTable)
            raise ValueError(f"Invalid `name={self.name}`, must be one of {allowed}")


def compute_dedup_hash(example: LazyRow, dedup_columns: tuple[str, ...]):
    values = sorted(
        [
            str(example[col]) if example.get(col) is not None else ""
            for col in dedup_columns
        ]
    )
    return {"_hash": xxhash.xxh64("|".join(values).encode("utf-8")).hexdigest()}


class BaseKb(ABC):
    dataset_name: str
    has_history: bool = False
    builder_class: type[GeneratorBasedBuilder]

    def __init__(
        self,
        dedup_columns: list[str] | None = None,
        name_transform: bool | NameView | None = False,
        num_proc: int | None = None,
        **kwargs,
    ):
        self.dedup_columns = dedup_columns
        if self.dedup_columns is not None:
            self.dedup_columns = sorted(set(self.dedup_columns + ["label", "aliases"]))

        self.num_proc = num_proc

        self.name_transform = name_transform

    def load_local(
        self, data_files: list[DataFile], data_dir: str | None = None
    ) -> DatasetDict:
        tables = [KbTable.ENTITIES]
        if self.has_history:
            tables.append(KbTable.HISTORY)

        ds_dict = {}
        for table in tables:
            builder_kwargs = {
                "dataset_name": f"{HF_USER}___{self.dataset_name}",  # hack to get same naming as from downloading from hub
                "config_name": str(table),
                "data_dir": data_dir,
                "data_files": {d.name: d.uri for d in data_files},
            }
            if data_dir is not None:
                builder_kwargs["base_path"] = data_dir

            builder_kwargs = self._get_builder_kwargs(
                table=table, kwargs=builder_kwargs
            )

            builder = self.builder_class(**builder_kwargs)
            builder.download_and_prepare()
            ds = builder.as_dataset()
            ds_dict[table] = ds.pop(Split.TRAIN)
        return DatasetDict(ds_dict)

    def _get_builder_kwargs(self, table: KbTable, kwargs: dict) -> dict:
        """Hook for knowledge base-specific builder kwargs."""
        return kwargs

    def load_remote(self, hf_repo_id: str) -> DatasetDict:
        tables: dict = {}
        try:
            entities = load_dataset(hf_repo_id, name="entities", num_proc=self.num_proc)
            tables["entities"] = entities["train"]
            if self.has_history:
                history = load_dataset(
                    hf_repo_id, name="history", num_proc=self.num_proc
                )
                tables["history"] = history["train"]
        except DatasetNotFoundError:
            raise ValueError(f"HF repository `{hf_repo_id}` not found.")
        return DatasetDict(tables)

    def deduplicate(self, ds: DatasetDict) -> DatasetDict:
        """Deduplicate ENTITIES table based on 'dedup_columns'."""
        if self.dedup_columns is None:
            return ds

        entities = ds[KbTable.ENTITIES]

        invalid_columns = [
            c for c in self.dedup_columns if c not in entities.column_names
        ]
        if invalid_columns:
            raise ValueError(
                f"`dedup_columns` contains invalid columns: {invalid_columns}"
            )

        hashed_ds = entities.map(
            compute_dedup_hash,
            fn_kwargs={"dedup_columns": sorted(self.dedup_columns)},
            num_proc=self.num_proc,
            desc=f"{self.dataset_name}: computing hashes for deduplication",
        )

        hashes = hashed_ds["_hash"]
        seen = set()
        keep_indices = []
        for i, h in enumerate(tqdm(hashes, desc="BELB kb: deduplicate (keep first)")):
            if h not in seen:
                keep_indices.append(i)
                seen.add(h)

        entities = entities.select(keep_indices)
        ds[KbTable.ENTITIES] = self.reindex(entities)

        return ds

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        ds = self._postprocess(ds)
        ds = self.deduplicate(ds)
        return ds

    def _postprocess(self, ds: DatasetDict) -> DatasetDict:
        """Hook for knowledge base-specific postprocessing."""
        return ds

    def load(self, resource: ResourceConfig) -> DatasetDict:
        if resource.hf_repo_id is not None:
            ds = self.load_remote(resource.hf_repo_id)
        else:
            assert resource.data_files is not None
            ds = self.load_local(
                data_dir=resource.data_dir,
                data_files=resource.data_files,
            )

        return self.postprocess(ds)

    def reindex(self, table: Dataset) -> DatasetDict:
        return reindex(ds=table)
