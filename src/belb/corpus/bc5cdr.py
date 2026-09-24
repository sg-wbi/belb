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

from ..utils import ID_JOIN, NIL, EntityType
from .base import (
    CORPUS_FEATURES,
    BaseCorpus,
)
from .parsing import parse_pubtator_example

ENTITY_TYPES = [EntityType.CHEMICAL, EntityType.DISEASE]


def parse_annotations(
    annotations: list[pubtator.PubTatorAnn],
) -> list[pubtator.PubTatorAnn]:

    parsed = []

    for a in annotations:
        a.id = (a.id or NIL).replace("-1", NIL).replace("|", ID_JOIN)
        parsed.append(a)

    return parsed


def parse_split(
    path: Path,
) -> Iterator[dict]:

    with path.open() as fp:
        examples = pubtator.load(fp)

        for e in examples:
            e.annotations = parse_annotations(
                annotations=e.annotations,
            )
            yield parse_pubtator_example(e)


class Bc5cdrBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        features = CORPUS_FEATURES.copy()
        features["metadata"]["pmid"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name="bc5cdr",
        )

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            base_path = Path(self.config.data_dir)
            gen_kwargs = {k: base_path / v for k, v in data_files.items()}
        else:
            downloaded = dl_manager.download(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [
            SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={"path": gen_kwargs["train"]},
            ),
            SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={"path": gen_kwargs["validation"]},
            ),
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": gen_kwargs["test"]},
            ),
        ]

    def _generate_examples(self, path: Path):
        for idx, e in enumerate(parse_split(path)):
            yield idx, e


class Bc5cdrCorpus(BaseCorpus):
    dataset_name = "bc5cdr"
    builder_class = Bc5cdrBuilder

    def __init__(
        self,
        entity_types: tuple[str, ...] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.entity_types = self.parse_entity_types(
            entity_types=entity_types,
            supported_entity_types=ENTITY_TYPES,  # type: ignore
        )

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.entity_types is not None and len(self.entity_types) < len(ENTITY_TYPES):

            def filter_annotations(example):
                example["annotations"] = [
                    a
                    for a in example["annotations"]
                    if a["type"].upper() in self.entity_types
                ]
                return example

            ds = ds.map(
                filter_annotations,
                num_proc=self.num_proc,
                desc=f"Filtering BC5CDR by entity types: {self.entity_types}",
            )
        return ds
