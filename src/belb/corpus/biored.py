from collections.abc import Iterator
from pathlib import Path

from bioc import pubtator
from datasets import (
    DatasetDict,
    DatasetInfo,
    DownloadManager,
    Features,
    GeneratorBasedBuilder,
    Split,
    SplitGenerator,
    Value,
    Version,
)

from ..utils import NIL, EntityType
from .base import (
    ANNOTATION_FEATURES,
    CORPUS_FEATURES,
    BaseCorpus,
)
from .parsing import parse_pubtator_example

DATASET_NAME = "biored"

ENTITY_TYPE_MAP = {
    "DiseaseOrPhenotypicFeature": EntityType.DISEASE,
    "ChemicalEntity": EntityType.CHEMICAL,
    "OrganismTaxon": EntityType.SPECIES,
    "GeneOrGeneProduct": EntityType.GENE,
    "SequenceVariant": EntityType.VARIANT,
    "CellLine": EntityType.CELL_LINE,
}


def parse_annotations(
    annotations: list[pubtator.PubTatorAnn],
) -> list[dict]:

    parsed = []
    for a in annotations:
        a_type = ENTITY_TYPE_MAP[a.type]

        p = {
            "type": a.type,
            "start": a.start,
            "end": a.end,
            "text": a.text,
        }

        if a.id != "-":
            if a_type != EntityType.VARIANT:
                ids = [i.strip() for i in a.id.replace("|", ",").split(",")]
            else:
                if a.id.startswith("rs"):
                    ids = [a.id]
                else:
                    ids = [NIL]
                    if "|" in a.id:
                        p["tmvar_repr"] = a.id
        else:
            ids = [NIL]

        p["ids"] = ids
        parsed.append(p)

    return parsed


def parse_split(path: Path) -> Iterator[dict]:
    with path.open() as fp:
        examples = pubtator.load(fp)
        for e in examples:
            annotations = parse_annotations(annotations=e.annotations)
            e.annotations = []
            parsed = parse_pubtator_example(e)
            parsed["annotations"] = annotations
            yield parsed


class BioredBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        annotation_features = ANNOTATION_FEATURES.copy()
        annotation_features.update(
            {
                "tmvar_repr": Value("string"),
            }
        )

        features = CORPUS_FEATURES.copy()
        features["annotations"] = [annotation_features]
        features["metadata"]["pmid"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name=DATASET_NAME,
        )

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        # data_files is dict[str, list[str]]
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            folder = dl_manager.extract(data_files["folder"])
        else:
            folder = dl_manager.download_and_extract(data_files["folder"])

        folder = Path(folder) / "BioRED"

        return [
            SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={"path": folder / "Train.PubTator"},
            ),
            SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={"path": folder / "Dev.PubTator"},
            ),
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": folder / "Test.PubTator"},
            ),
        ]

    def _generate_examples(self, path: Path):
        for idx, e in enumerate(parse_split(path)):
            yield idx, e


class BioredCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = BioredBuilder

    def __init__(
        self,
        entity_types: tuple[str | EntityType, ...] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.entity_types = self.parse_entity_types(
            entity_types=entity_types,
            supported_entity_types=list(ENTITY_TYPE_MAP.values()),
        )

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.entity_types is not None and len(self.entity_types) < len(
            ENTITY_TYPE_MAP
        ):

            def filter_annotations(example):
                example["annotations"] = [
                    a
                    for a in example["annotations"]
                    if ENTITY_TYPE_MAP[a["type"]] in self.entity_types
                ]
                return example

            ds = ds.map(
                filter_annotations,
                num_proc=self.num_proc,
                desc=f"{DATASET_NAME} filter by entity types: {self.entity_types}",
            )
        return ds
