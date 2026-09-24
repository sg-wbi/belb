from collections.abc import Iterator
from pathlib import Path

from bioc import pubtator
from datasets import (
    DatasetInfo,
    DownloadManager,
    Features,
    GeneratorBasedBuilder,
    Split,
    SplitGenerator,
    Value,
    Version,
)

from ..utils import ID_JOIN
from .base import (
    CORPUS_FEATURES,
    BaseCorpus,
)
from .parsing import convert_brat_to_pubtator, parse_pubtator_example

DATASET_NAME = "s1000"


def parse_annotations(
    annotations: list[pubtator.PubTatorAnn],
) -> list[pubtator.PubTatorAnn]:

    parsed = []
    for a in annotations:
        identifiers = a.id.split("|")
        if identifiers:
            a.id = ID_JOIN.join(identifiers)
            parsed.append(a)

    return parsed


def parse_split(path: Path) -> Iterator[dict]:

    for ann_file in sorted(path.glob("*.ann")):
        txt_file = ann_file.with_suffix(".txt")

        example = convert_brat_to_pubtator(ann_file=ann_file, txt_file=txt_file)
        example.annotations = parse_annotations(annotations=example.annotations)

        yield parse_pubtator_example(example)


class S1000Builder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        features = CORPUS_FEATURES.copy()
        features["metadata"]["pmid"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name=DATASET_NAME,
        )

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            folder = dl_manager.extract(data_files["folder"])
        else:
            folder = dl_manager.download_and_extract(data_files["folder"])

        folder = Path(folder) / "S1000-corpus" / "entire-corpus"

        return [
            SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={"path": folder / "train"},
            ),
            SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={"path": folder / "dev"},
            ),
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": folder / "test"},
            ),
        ]

    def _generate_examples(self, path: Path):
        for idx, e in enumerate(parse_split(path)):
            yield idx, e


class S1000Corpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = S1000Builder
