import re
from collections.abc import Iterator
from dataclasses import dataclass
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

from ..utils import NIL, EntityType
from .base import CORPUS_FEATURES, BaseCorpus

DATASET_NAME = "linnaeus"

SPLITS = OmegaConf.load(str(files("belb.assets.splits") / "linnaeus.yaml"))


@dataclass
class PassageChunk:
    pid: int
    offset: int
    text: str


@dataclass
class Passage:
    pid: int
    offset: int
    text: str
    type: str

    def to_dict(self) -> dict:
        return {
            "id": self.pid,
            "offset": self.offset,
            "text": self.text,
            "type": self.type,
        }


def split_text_into_chunks(text: str) -> list[PassageChunk]:
    chunks: list[PassageChunk] = []
    start = 0
    for idx, match in enumerate(re.finditer(re.escape("\n"), text)):
        end = match.start()
        chunks.append(
            PassageChunk(
                pid=idx,
                offset=start,
                text=text[start:end],
            )
        )
        start = end

    return chunks


def build_passages(text: str) -> list[Passage]:
    chunks = split_text_into_chunks(text)
    if not chunks:
        return []

    buckets: list[list[PassageChunk]] = []
    bucket: list[PassageChunk] = []
    for chunk in reversed(chunks):
        if len(chunk.text) > 100:
            buckets.append(bucket)
            bucket = []
        bucket.append(chunk)
    if bucket:
        buckets.append(bucket)

    buckets = [sorted(b, key=lambda c: c.pid) for b in reversed(buckets)]

    title_chunk = buckets[0].pop(0)
    passages = [
        Passage(
            pid=0,
            offset=title_chunk.offset,
            text=title_chunk.text,
            type="title",
        )
    ]
    if not buckets[0]:
        buckets.pop(0)

    for pid, bucket in enumerate(buckets, start=1):
        if pid == len(buckets):
            if all(c.text in ["\n", "", "\n\n"] for c in bucket):
                break

        text_block = "".join(c.text for c in bucket)
        offset = bucket[0].offset
        passage_type = "abstract" if pid == 1 else "passage"
        passages.append(
            Passage(pid=pid, offset=offset, text=text_block, type=passage_type)
        )

    for passage in passages:
        if passage.type != "title" and passage.text:
            passage.text = passage.text[1:]

    return passages


def load_passages(path: Path) -> list[dict]:
    with path.open() as fp:
        text = fp.read()

    passages = build_passages(text)

    return [p.to_dict() for p in passages]


def load_id_to_annotations(path: Path) -> dict[str, list[pubtator.PubTatorAnn]]:

    df = pd.read_csv(path, sep="\t")

    df.rename({df.columns[0]: "identifier"}, axis=1, inplace=True)

    df["identifier"] = df["identifier"].apply(lambda x: x.replace("species:ncbi:", ""))

    id_to_annotations: dict = {}
    for row in df.to_dict("records"):
        key = row["document"].replace("pmcA", "")
        if key not in id_to_annotations:
            id_to_annotations[key] = []

        # species:ncbi:0  pmcA1312363     9263    9284    T.aestivum var Fortal   4
        # species:ncbi:0  pmcA1474674     7819    7845    Synechococcus strain Tx-20
        # species:ncbi:0  pmcA1474674     14179   14198   Synechococcus TX-20     4
        # species:ncbi:0  pmcA1851977     17206   17215   SIVmac239
        # species:ncbi:0  pmcA1891629     8906    8928    Rhipicephalus camicasi
        # species:ncbi:0  pmcA1891629     9288    9298    Haemogogus
        # species:ncbi:0  pmcA2562362     1713    1718    mules
        # species:ncbi:0  pmcA2562362     1841    1846    mules
        id_to_annotations[key].append(
            pubtator.PubTatorAnn(
                pmid=key,
                start=row["start"],
                end=row["end"],
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
        passages = load_passages(path=folder / "txt" / f"pmcA{i}.txt")

        annotations = id_to_annotations.get(i, [])

        for a in annotations:
            if str(a.id) == "0":
                a.id = NIL

        parsed = [
            {
                "text": a.text,
                "start": a.start,
                "end": a.end,
                "type": a.type,
                "ids": [a.id],
            }
            for a in annotations
        ]

        text = " ".join(p["text"] for p in passages)

        metadata = {
            "pmcid": i,
            "passages": [{"offset": p["offset"], "type": p["type"]} for p in passages],
        }

        yield {"id": i, "text": text, "annotations": parsed, "metadata": metadata}


class LinnaeusBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        features = CORPUS_FEATURES.copy()
        features["metadata"]["pmcid"] = Value("string")

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

        folder = Path(folder) / "manual-corpus-species-1.1"

        id_to_annotations = load_id_to_annotations(folder / "tags.tsv")

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


class LinnaeusCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = LinnaeusBuilder
