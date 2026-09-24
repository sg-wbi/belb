import json
from abc import ABC
from collections import abc
from importlib.resources import files
from pathlib import Path
from typing import NamedTuple, cast

from datasets import (
    BuilderConfig,
    DatasetDict,
    GeneratorBasedBuilder,
    Sequence,
    Value,
    load_dataset,
)
from datasets.exceptions import DatasetNotFoundError
from datasets.formatting.formatting import LazyRow
from loguru import logger
from omegaconf import OmegaConf

from ..config import DataFile, ResourceConfig, TaskConfig
from ..utils import HF_USER, NIL, EntityType


class BelbCorpusConfig(BuilderConfig):
    data_files: dict[str, list[str]] | None = None


ANNOTATION_FEATURES = {
    "type": Value("string"),
    "text": Value("string"),
    "start": Value("int32"),
    "end": Value("int32"),
    "ids": Sequence(Value("string")),
}

PASSAGE_FEATUERS = {"offset": Value("int32"), "type": Value("string")}


CORPUS_FEATURES = {
    "id": Value("string"),
    "text": Value("string"),
    "annotations": [ANNOTATION_FEATURES],
    "metadata": {"passages": [PASSAGE_FEATUERS]},
}


class PatchKey(NamedTuple):
    id: str
    start: int
    end: int
    text: int


class PatchValue(NamedTuple):
    start: int
    end: int
    text: int


def _load_annotations_patches(path: Path):

    with path.open() as fp:
        patches = json.load(fp)

    lookup = {}
    for patch in patches:
        key = PatchKey(
            id=str(patch["id"]),
            start=patch["start"],
            end=patch["end"],
            text=patch["text"],
        )
        value = PatchValue(
            start=patch["start_patch"],
            end=patch["end_patch"],
            text=patch["text_patch"],
        )

        lookup[key] = value

    return lookup


def _load_id_map(spec: abc.Mapping) -> abc.Mapping:
    # NOTE: there may be two reasons why a corpus as multiple id abc.Mappings
    # 1. annotations can be linked to different KBs (e.g. MeSH, UMLS), in which case we need a `default`
    # 2. there are multiple entity types (linked to different KBs),
    # in which case we need `types` (e.g. disease -> MeSh, cell-line -> Cellosaurus)
    keys = [k for k in spec if k not in ["default", "enity_types"]]
    if len(keys) == 1:
        return spec[keys[0]]
    else:
        keys = list(spec.keys())
        if not any(k in keys for k in ["default", "entity_types"]):
            raise ValueError(
                "Found multiple id_mappings: must specify either `default` or `entity_types`"
            )
        if "default" in keys:
            return spec[spec["default"]]
        else:
            type_to_map = spec["entity_types"]
            try:
                type_to_map = {EntityType(t.upper()): m for t, m in type_to_map.items()}
            except ValueError:
                raise ValueError(
                    f"Keys in `{type_to_map}` (case-insensitive) must be entity types {tuple(str(c) for c in EntityType)} "
                )
            return {t: spec[m] for t, m in type_to_map.items()}


class BaseCorpus(ABC):
    dataset_name: str
    builder_class: type[GeneratorBasedBuilder]

    def __init__(
        self,
        name: str,
        task: TaskConfig,
        id_mapping: bool | str | abc.Mapping = True,
        annotation_patches: bool | abc.Mapping[PatchKey, PatchValue] = True,
        num_proc: int | None = None,
    ):
        self.name = name
        self.task = task
        self.num_proc = num_proc

        self.id_mapping = self.load_id_mapping(id_mapping=id_mapping)
        self.annotation_patches = self.load_annotation_patches(
            name=name, annotation_patches=annotation_patches
        )

    def _get_builder_kwargs(self, kwargs: dict) -> dict:
        """Hook for corpus-specific builder kwargs."""
        return kwargs

    def load_local(
        self, data_files: list[DataFile], data_dir: str | None = None
    ) -> DatasetDict:
        builder_kwargs = {
            "dataset_name": f"{HF_USER}___{self.dataset_name}",  # hack to get same naming as from downloading from hub
            "data_dir": data_dir,
            "data_files": {d.name: d.uri for d in data_files},
        }
        if data_dir is not None:
            builder_kwargs["base_path"] = data_dir

        builder_kwargs = self._get_builder_kwargs(kwargs=builder_kwargs)

        builder = self.builder_class(**builder_kwargs)
        builder.download_and_prepare()
        ds = builder.as_dataset()

        return ds

    def load_remote(self, hf_repo_id: str) -> DatasetDict:
        try:
            ds = load_dataset(hf_repo_id, num_proc=self.num_proc)
        except DatasetNotFoundError:
            raise ValueError(f"HF repository `{hf_repo_id}` does not exist.")
        return ds

    def _postprocess(self, ds: DatasetDict) -> DatasetDict:
        return ds

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        ds = self._postprocess(ds)

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

        ds = self.postprocess(ds)

        return self.prepare_for_task(ds)

    def load_id_mapping(
        self, id_mapping: bool | str | abc.Mapping = True
    ) -> abc.Mapping | None:
        if id_mapping is False:
            return None

        elif id_mapping is True or isinstance(id_mapping, str):
            folder = cast(Path, files("belb.assets.ids") / f"{self.name}.yaml")
            if not folder.exists():
                return None

            spec = cast(dict, OmegaConf.to_container(OmegaConf.load(folder)))

            if id_mapping is True:
                return _load_id_map(spec=spec)
            else:
                out = spec.get(id_mapping)
                if out is None:
                    raise ValueError(
                        f"`{id_mapping}` not found in pre-configured id_mapping"
                    )
                return out

        elif isinstance(id_mapping, abc.Mapping):
            return id_mapping
        else:
            raise TypeError("`id_abc.Mapping` must be bool or abc.Mapping")

    def load_annotation_patches(
        self, name: str, annotation_patches: bool | abc.Mapping[PatchKey, PatchValue]
    ) -> abc.Mapping[PatchKey, PatchValue] | None:

        if annotation_patches is False:
            return None

        elif annotation_patches is True:
            path = cast(Path, files("belb.assets.offsets") / f"{name}.json")
            if not path.exists():
                return None

            return _load_annotations_patches(path)

        elif isinstance(annotation_patches, str):
            path = cast(Path, annotation_patches)
            if not path.exists():
                raise ValueError(
                    f"File with annotation patches not found: {annotation_patches}"
                )
            return _load_annotations_patches(path)

        elif isinstance(annotation_patches, abc.Mapping):
            if not all(isinstance(k, PatchKey) for k in annotation_patches) or not all(
                isinstance(v, PatchValue) for v in annotation_patches.values()
            ):
                raise ValueError(
                    "`annotation_patches` keys and values must be instances of `PatchKey` and `PatchValue`, respectively"
                )

            return annotation_patches

        else:
            raise TypeError("`annotation_patches` must be bool, string or abc.Mapping")

    @property
    def include_composite_mentions(self):
        return self.task.composite_mentions in ["AND", "OR", True]

    def get_preprocess_message(self) -> str | None:
        """
        Get message for preprocessing steps
        """
        steps = []
        if self.annotation_patches is not None:
            steps.append("patch annotations")
        if self.id_mapping is not None:
            steps.append("update IDs")
        if self.task.inkb:
            steps.append("remove NIL (task.inkb=true)")
        if not self.include_composite_mentions:
            steps.append("remove composite mentions (task.composite_mentions=false)")
        if not self.task.endtoend:
            steps.append("remove examples w/o annotations (task.endtoend=false)")
        if self.task.document_level:
            steps.append("remove annotations w/o offsets (task.document_level=true)")

        if not steps:
            return None

        return "BELB corpus: " + "; ".join(f"{i+1}. {s}" for i, s in enumerate(steps))

    def prepare_for_task(self, ds: DatasetDict) -> DatasetDict:
        msg = self.get_preprocess_message()
        if msg:
            logger.debug(msg)

        ds = ds.map(
            process_annotations,
            fn_kwargs={
                "annotation_patches": self.annotation_patches,
                "id_mapping": self.id_mapping,
                "inkb": self.task.inkb,
                "document_level": self.task.document_level,
                "composite_mentions": self.include_composite_mentions,
            },
            num_proc=self.num_proc,
            desc="Preprocess annotations",
        )

        if not self.task.endtoend:
            ds = ds.filter(
                lambda example: len(example["annotations"]) > 0,
                num_proc=self.num_proc,
                desc="Filter examples with no annotations",
            )

        return ds

    def parse_entity_types(
        self,
        entity_types: tuple[str, ...] | None,
        supported_entity_types: list[str],
    ) -> tuple[str, ...]:
        supported_entity_types = [p.upper() for p in supported_entity_types]
        if entity_types:
            if isinstance(entity_types, abc.Sequence) and not isinstance(
                entity_types, (str, bytes)
            ):
                # NOTE: `entity_types` must be a tuple of `str`. "
                # Lists are not allowed because `datasets.Dataset.from_generator` may shard list-valued gen_kwargs across workers.
                entity_types = tuple(entity_types)
            if not isinstance(entity_types, tuple):
                raise ValueError(
                    f"`entity_types` must be a tuple of `str`, found {type(entity_types)}"
                )
            if not all(e.upper() in supported_entity_types for e in entity_types):
                raise ValueError(
                    f"Invalid `entity_types={entity_types}`. Supported (case-insensitive) values: {supported_entity_types}."
                )
            return tuple(e.upper() for e in entity_types)
        else:
            return tuple(supported_entity_types)


def apply_annotation_patches(row: LazyRow, patches: abc.Mapping) -> LazyRow:
    parsed = []
    for a in row["annotations"]:
        if patches is not None:
            key = (row["id"], a["start"], a["end"], a["text"])
            if key in patches:
                start, end, text = patches[key]
                a["start"] = start
                a["end"] = end
                a["text"] = text

        if a["start"] == -1 and a["end"] == -1:
            continue

        parsed.append(a)

    row["annotations"] = parsed
    return row


def apply_id_mapping(row: LazyRow, id_mapping: abc.Mapping) -> LazyRow:
    for a in row["annotations"]:
        a["ids"] = [id_mapping.get(i, i) for i in a["ids"]]

    return row


def apply_type_id_mapping(row: LazyRow, id_mapping: abc.Mapping) -> LazyRow:
    for a in row["annotations"]:
        a["ids"] = [id_mapping.get(a["type"].upper(), {}).get(i, i) for i in a["ids"]]

    return row


def process_annotations(
    row: LazyRow,
    annotation_patches: abc.Mapping | None = None,
    id_mapping: abc.Mapping | None = None,
    document_level: bool = False,
    inkb: bool = True,
    composite_mentions: bool = True,
) -> LazyRow:
    example = row

    if annotation_patches is not None:
        example = apply_annotation_patches(example, patches=annotation_patches)

    if id_mapping:
        example = (
            apply_type_id_mapping(example, id_mapping=id_mapping)
            if next(iter(id_mapping.keys())) in EntityType
            else apply_id_mapping(example, id_mapping=id_mapping)
        )

    if inkb:
        example = remove_nil(example)

    if not composite_mentions:
        example = remove_composite_mentions(example)

    if not document_level:
        example = remove_document_level(example)

    return example


def remove_nil(row: LazyRow) -> LazyRow:
    parsed = []
    for a in row["annotations"]:
        ids = [i for i in a["ids"] if i != NIL]
        if ids:
            a["ids"] = ids
            parsed.append(a)
    row["annotations"] = parsed
    return row


def remove_composite_mentions(row: LazyRow) -> LazyRow:
    parsed = []
    for a in row["annotations"]:
        if len(a["ids"]) > 1:
            continue
        parsed.append(a)
    row["annotations"] = parsed
    return row


def remove_document_level(row: LazyRow) -> LazyRow:
    parsed = []
    for a in row["annotations"]:
        if a["start"] == -1 and a["end"] == -1:
            continue
        parsed.append(a)
    row["annotations"] = parsed
    return row
