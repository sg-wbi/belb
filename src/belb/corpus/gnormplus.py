from pathlib import Path

import datasets
from bioc import pubtator
from datasets import Features, Split

from ..utils import ID_JOIN, NIL
from .base import CORPUS_FEATURES, BaseCorpus, BelbCorpusConfig
from .parsing import parse_pubtator_example

DATASET_NAME = "gnormplus"


def parse_identifier(identifier: str) -> list[str]:
    identifiers = []
    for raw_id in identifier.split(","):
        raw_id = raw_id.strip()
        if not raw_id:
            continue

        if "Tax:" in raw_id or "tax:" in raw_id:
            raw_id = raw_id.split("(")[0]

        raw_id = raw_id.strip()
        if not raw_id:
            continue

        if raw_id == "-1":
            raw_id = NIL

        identifiers.append(raw_id)

    return sorted(set(identifiers))


def parse_annotations(
    annotations: list[pubtator.PubTatorAnn],
) -> list[pubtator.PubTatorAnn]:
    parsed = []
    for a in annotations:
        if not a.id:
            a.id = NIL
            parsed.append(a)
            continue

        identifiers = parse_identifier(a.id)
        if not identifiers:
            a.id = NIL
        else:
            a.id = ID_JOIN.join(identifiers)

        parsed.append(a)

    return parsed


class GnormplusBuilder(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIG_CLASS = BelbCorpusConfig

    def _info(self):
        return datasets.DatasetInfo(
            features=Features(CORPUS_FEATURES),
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):

        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            folder = dl_manager.extract(data_files["folder"])
        else:
            folder = dl_manager.download_and_extract(data_files["folder"])

        folder = Path(folder) / "GNormPlusCorpus"

        return [
            datasets.SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={"path": folder / "BC2GNtrain.PubTator.txt"},
            ),
            datasets.SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={"path": folder / "NLMIAT.PubTator.txt"},
            ),
            datasets.SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": folder / "BC2GNtest.PubTator.txt"},
            ),
        ]

    def _generate_examples(self, path: str):
        with Path(path).open() as fp:
            examples = pubtator.load(fp)

            for i, e in enumerate(examples):
                e.annotations = parse_annotations(annotations=e.annotations)
                parsed = parse_pubtator_example(e)
                yield i, parsed


class GnormplusCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = GnormplusBuilder
