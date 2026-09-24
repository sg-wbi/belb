from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path

import pandas as pd
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
from loguru import logger

from .base import ANNOTATION_FEATURES, CORPUS_FEATURES, BaseCorpus

DATASET_NAME = "snp"


def parse_split(annotations_path: Path) -> Iterator[dict]:

    # Due to license restriction of PubMed(R)® this corpus contains only annotations.
    # To facilitate a reproduction of the original corpus, we include the exact position in the text, as well as the matching string.
    # The articles are composed of <Title><Whitespace><Whitespace><Abstract>
    # For detailed description of the corpus and its annotations see README.txt.
    logger.warning(
        "`snp` corpus provides only annotations: use the PubMed API to retrieve corresponding text "
        "(articles are composed of <Title><Whitespace><Whitespace><Abstract>)"
    )

    annotations = pd.read_csv(
        annotations_path,
        sep="\t",
        names=["pmid", "text", "norm", "start", "end", "id", "type"],
    )

    annotations["pmid"] = annotations["pmid"].astype(str)

    pmid_to_annotations: dict = {}

    for row in annotations.to_dict("records"):
        pmid = row["pmid"]

        if pmid not in pmid_to_annotations:
            pmid_to_annotations[pmid] = []

        a = {
            "start": row["start"],
            "end": row["end"],
            "text": row["text"].replace("'", ""),
            "type": row["type"],
            "ids": row["id"].replace("rs", "").split(),
            "metadata": {"norm": row["norm"]},
        }

        pmid_to_annotations[pmid].append(a)

    for pmid, annotations in pmid_to_annotations.items():
        yield {"id": pmid, "annotations": annotations, "metadata": {"pmid": pmid}}


def resolve_annotations_path(path: Path) -> Path:
    if path.is_file():
        return path

    candidate = path / "annotations.txt"
    if candidate.exists():
        return candidate

    for subdir in sorted(p for p in path.iterdir() if p.is_dir()):
        candidate = subdir / "annotations.txt"
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"Could not locate `annotations.txt` in `{path}`")


class SnpBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        annotation_features = ANNOTATION_FEATURES.copy()
        annotation_features.update({"norm": Value("string")})

        features = deepcopy(CORPUS_FEATURES)
        features["annotations"] = [annotation_features]
        features.pop("text")
        features["metadata"].pop("passages")
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

        annotations_path = resolve_annotations_path(Path(folder))

        return [
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"annotations_path": annotations_path},
            )
        ]

    def _generate_examples(self, annotations_path: Path):
        for idx, example in enumerate(parse_split(annotations_path=annotations_path)):
            yield idx, example


class SnpCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = SnpBuilder


# SHIFT_FORWARD_BY_ONE = [
#     15645182,
#     16368448,
#     16453988,
#     16497333,
#     16652158,
#     16691626,
#     16723442,
#     17019603,
#     17216208,
#     17219016,
#     17299513,
#     17445871,
#     17535992,
#     17566096,
#     17630229,
#     17632285,
#     17656372,
#     17657167,
#     17674045,
#     17701054,
#     17701750,
#     17704904,
#     17852831,
#     17873324,
#     17877509,
#     17894153,
#     17917281,
#     18061132,
#     18092344,
#     18160840,
#     18162085,
#     18163425,
#     18193244,
#     18222353,
#     18239646,
#     16865358,
# ]
#
#
# SHIFT_FORWARD_BY_THREE = [16351803, 16854283]
#
#
# def handle_errors_annotation_offsets(self, eid: str, a: Annotation, p: Passage):
#     """Fix annotation offsets"""
#
#     if int(eid) in SHIFT_FORWARD_BY_ONE:
#         a.start += 1
#         a.end += 1
#
#     elif int(eid) in SHIFT_FORWARD_BY_THREE:
#         a.start += 3
#         a.end += 3
#
#     elif eid == "17894849" and (a.start, a.end) == (5, 10):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17455201" and (a.start, a.end) == (130, 134):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17187763" and (a.start, a.end) in [(31, 40), (45, 54)]:
#         a.start += 1
#         a.end += 1
#
#     elif eid == "16865697" and (a.start, a.end) == (67, 78):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17000021" and (a.start, a.end) == (26, 35):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17022693" and (a.start, a.end) in [(68, 73), (79, 84)]:
#         a.start += 1
#         a.end += 1
#
#     elif eid == "16338218" and (a.start, a.end) == (18, 25):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "16625213" and (a.start, a.end) == (24, 29):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17187763" and (a.start, a.end) == (24, 29):
#         a.start += 1
#         a.end += 1
#
#     elif eid == "17390150" and (a.start, a.end) == (24, 33):
#         a.start += 1
#         a.end += 1
