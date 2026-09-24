from collections.abc import Iterator
from importlib.resources import files
from pathlib import Path

import pandas as pd
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
from omegaconf import OmegaConf

from ..utils import EntityType
from .base import CORPUS_FEATURES, BaseCorpus

DATASET_NAME = "s800"

SPLITS = OmegaConf.load(str(files("belb.assets.splits") / "s800.yaml"))


def load_id_to_annotations(path: Path) -> dict[str, list[pubtator.PubTatorAnn]]:

    columns = ["identifier", "id:pmid", "start", "end", "text"]
    df = pd.read_csv(path, sep="\t", names=columns)  # type: ignore
    df["identifier"] = df["identifier"].astype(str)

    id_to_annotations: dict[str, list[pubtator.PubTatorAnn]] = {}

    for row in df.to_dict("records"):
        key = row["id:pmid"]

        if key not in id_to_annotations:
            id_to_annotations[key] = []

        id_to_annotations[key].append(
            pubtator.PubTatorAnn(
                pmid=key.split(":")[1],
                start=row["start"],
                end=row["end"] + 1,  # need to to this to all to get the offsets working
                text=row["text"],
                type=EntityType.SPECIES,
                id=row["identifier"],
            )
        )

    return id_to_annotations


def parse_split(
    folder: Path,
    ids: list[str],
    id_to_annotations: dict[str, list[pubtator.PubTatorAnn]],
) -> Iterator[dict]:

    for i in ids:
        filename, pmid = i.split(":")

        with (folder / "abstracts" / f"{filename}.txt").open() as fp:
            text = fp.read()

        offset = text.index("\n", text.index("\n") + 1)

        metadata = {
            "pmid": pmid,
            "passages": [
                {"offset": 0, "type": "title"},
                {"offset": offset + 1, "type": "abstract"},
            ],
        }

        annotations = id_to_annotations.get(i, [])

        parsed_annotations = [
            {
                "text": a.text,
                "start": a.start,
                "end": a.end,
                "type": a.type,
                "ids": a.id.split("|"),
            }
            for a in annotations
        ]

        yield {
            "id": filename,
            "text": text,
            "annotations": parsed_annotations,
            "metadata": metadata,
        }


class S800Builder(GeneratorBasedBuilder):
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

        folder = Path(folder)

        id_to_annotations = load_id_to_annotations(folder / "S800.tsv")

        split_to_ids = {
            Split.TRAIN: SPLITS["train"],  # type: ignore
            Split.VALIDATION: SPLITS["validation"],  # type: ignore
            Split.TEST: SPLITS["test"],  # type: ignore
        }

        return [
            SplitGenerator(
                name=split,
                gen_kwargs={
                    "folder": folder,
                    "ids": ids,
                    "id_to_annotations": id_to_annotations,
                },
            )
            for split, ids in split_to_ids.items()
        ]

    def _generate_examples(
        self,
        folder: Path,
        ids: list[str],
        id_to_annotations: dict[str, list[pubtator.PubTatorAnn]],
    ):
        for idx, example in enumerate(parse_split(folder, ids, id_to_annotations)):
            yield idx, example


class S800Corpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = S800Builder
