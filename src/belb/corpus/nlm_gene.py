from importlib.resources import files
from pathlib import Path

import bioc
import datasets
from datasets import Features, Split
from omegaconf import OmegaConf

from ..utils import ID_JOIN, NIL
from .base import CORPUS_FEATURES, BaseCorpus, BelbCorpusConfig
from .parsing import convert_bioc_to_pubtator, parse_pubtator_example

SPLITS = OmegaConf.load(str(files("belb.assets.splits") / "nlm-gene.yaml"))

DATASET_NAME = "nlm-gene"


def parse_identifier(identifier: str) -> list[str]:
    parsed = []
    for i in identifier.strip().replace(",", ";").replace("|", ";").split(";"):
        i = i.strip()
        if not i:
            continue
        if i == "-":
            i = NIL
        parsed.append(i)
    return sorted(set(parsed))


def parse_annotations(
    annotations: list,
) -> list:
    parsed = []
    for a in annotations:

        if a.type in ["Domain", "Other"]:
            a.id = NIL

        if a.id is None:
            a.id = NIL

        if a.id != NIL:
            identifiers = parse_identifier(identifier=a.id)
            a.id = ID_JOIN.join(identifiers)

        a.text = a.text.replace("\xa0", " ")
        parsed.append(a)
    return parsed


class NlmGeneBuilder(datasets.GeneratorBasedBuilder):
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

        folder = Path(folder) / "Corpus"

        split_to_ids = {Split.VALIDATION: SPLITS["validation"]}  # type: ignore
        for split in [Split.TRAIN, Split.TEST]:
            ids = []
            with (folder / f"Pmidlist.{str(split).capitalize()}.txt").open() as fp:
                for line in fp:
                    ids.append(int(line.strip()))
                split_to_ids[split] = ids

        return [
            datasets.SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={
                    "folder": folder / "FINAL",
                    "ids": split_to_ids[Split.TRAIN],
                },
            ),
            datasets.SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={
                    "folder": folder / "FINAL",
                    "ids": split_to_ids[Split.VALIDATION],
                },
            ),
            datasets.SplitGenerator(
                name=Split.TEST,
                gen_kwargs={
                    "folder": folder / "FINAL",
                    "ids": split_to_ids[Split.TEST],
                },
            ),
        ]

    def _generate_examples(self, folder: Path, ids: list[int]):
        for i in ids:
            collection = bioc.load(folder / f"{i}.BioC.XML")
            for doc in collection.documents:
                e = convert_bioc_to_pubtator(
                    example=doc, identifier_key="NCBI Gene identifier"
                )
                e.title = e.title.replace("\xa0", " ")
                e.abstract = e.abstract.replace("\xa0", " ")
                e.annotations = parse_annotations(annotations=e.annotations)
                yield str(i), parse_pubtator_example(e)


class NlmGeneCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = NlmGeneBuilder
