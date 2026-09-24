from pathlib import Path

import datasets
from bioc import pubtator
from datasets import Features, Split

from ..utils import ID_JOIN
from .base import CORPUS_FEATURES, BaseCorpus, BelbCorpusConfig
from .parsing import parse_pubtator_example

DATASET_NAME = "ncbi-disease"


def parse_identifier(identifier: str) -> str:
    ids = identifier.strip().split("|")
    ids = [i for ids in ids for i in ids.split("+")]
    return ID_JOIN.join([i.replace("MESH:", "") for i in ids])


class NcbiDiseaseBuilder(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIG_CLASS = BelbCorpusConfig

    def _info(self):
        return datasets.DatasetInfo(
            features=Features(CORPUS_FEATURES),
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            downloaded = dl_manager.extract(
                {k: Path(self.config.data_dir) / v for k, v in data_files.items()}
            )
            downloaded = {k: Path(v) for k, v in downloaded.items()}
        else:
            downloaded = {
                k: Path(v)
                for k, v in dl_manager.download_and_extract(data_files).items()
            }

        downloaded = {
            k: (
                v / "NCBIdevelopset_corpus.txt"
                if k == "validation"
                else v / f"NCBI{k}set_corpus.txt"
            )
            for k, v in downloaded.items()
        }

        return [
            datasets.SplitGenerator(
                name=Split.TRAIN, gen_kwargs={"path": downloaded["train"]}
            ),
            datasets.SplitGenerator(
                name=Split.VALIDATION, gen_kwargs={"path": downloaded["validation"]}
            ),
            datasets.SplitGenerator(
                name=Split.TEST, gen_kwargs={"path": downloaded["test"]}
            ),
        ]

    def _generate_examples(self, path: str):
        with Path(path).open() as fp:
            examples = pubtator.load(fp)

            for i, e in enumerate(examples):
                for a in e.annotations:
                    a.id = parse_identifier(a.id)

                yield i, parse_pubtator_example(e)


class NcbiDiseaseCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = NcbiDiseaseBuilder
