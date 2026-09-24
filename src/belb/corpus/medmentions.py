import gzip
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

from ..config import DataFile
from .base import CORPUS_FEATURES, BaseCorpus
from .parsing import parse_pubtator_example

DATASET_NAME = "medmentions"

SUBSETS = ["st21pv", "full"]


def clean_text(text: str) -> str:
    """
    27928244 : This study evaluated the theory of mind (ToM) in adolescents diagnosed with bipolar disorder \u200e\u200e(BD)
    See here: https://stackoverflow.com/questions/51813129/decoding-u200e-to-string
    """
    return text.replace("\u200e", " ")


def parse_subset(subset: str | None = None) -> str:
    if subset is None:
        subset = "st21pv"

    if subset not in SUBSETS:
        raise ValueError(f"Invalid subset `{subset}`. Must be one of {SUBSETS}")

    return subset


def parse_split(examples: Iterator[pubtator.PubTator]) -> Iterator[dict]:

    for e in examples:
        e.title = clean_text(e.title)
        e.abstract = clean_text(e.abstract)

        for a in e.annotations:
            a.text = clean_text(a.text)
            a.id = a.id.replace("UMLS:", "")

        yield parse_pubtator_example(e)


def resolve_root(path: Path) -> Path:
    if (path / "st21pv" / "data" / "corpus_pubtator.txt.gz").exists():
        return path

    expected = path / "MedMentions-master"
    if (expected / "st21pv" / "data" / "corpus_pubtator.txt.gz").exists():
        return expected

    for subdir in sorted(p for p in path.iterdir() if p.is_dir()):
        candidate = subdir / "st21pv" / "data" / "corpus_pubtator.txt.gz"
        if candidate.exists():
            return subdir

    raise FileNotFoundError(f"Could not locate MedMentions root in `{path}`")


class MedMentionsBuilder(GeneratorBasedBuilder):
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

        folder = resolve_root(Path(folder))
        subset = parse_subset(getattr(self.config, "name", None))
        corpus_path = folder / subset / "data" / "corpus_pubtator.txt.gz"

        return [
            SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={
                    "corpus_path": corpus_path,
                    "ids_path": folder
                    / "full"
                    / "data"
                    / "corpus_pubtator_pmids_trng.txt",
                },
            ),
            SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={
                    "corpus_path": corpus_path,
                    "ids_path": folder
                    / "full"
                    / "data"
                    / "corpus_pubtator_pmids_dev.txt",
                },
            ),
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={
                    "corpus_path": corpus_path,
                    "ids_path": folder
                    / "full"
                    / "data"
                    / "corpus_pubtator_pmids_test.txt",
                },
            ),
        ]

    def _generate_examples(self, corpus_path: Path, ids_path: Path):
        with ids_path.open() as fp:
            ids = {line.strip() for line in fp.readlines()}

        with gzip.open(corpus_path, "rt") as fp:
            examples = (e for e in pubtator.load(fp) if e.pmid in ids)
            yield from enumerate(parse_split(examples=examples))


class MedMentionsCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = MedMentionsBuilder

    def __init__(self, subset: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.subset = parse_subset(subset=subset)

    def _get_builder_kwargs(self, kwargs: dict) -> dict:
        """Hook for corpus-specific builder kwargs."""

        kwargs["name"] = self.subset

        return kwargs
