import xml.etree.ElementTree as ET
from pathlib import Path

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
from .base import ANNOTATION_FEATURES, CORPUS_FEATURES, BaseCorpus

SUPPORTED_ENTITY_TYPES = [EntityType.VARIANT, EntityType.GENE]


def safe_find(elem: ET.Element, value: str):
    out = elem.find(value)
    if out is None:
        raise ValueError(f"Element {elem} has no value: `{value}`!")
    return out


def extract_text_and_annotations(annotated_elem: ET.Element) -> tuple[str, list]:
    annotations = []
    for a in annotated_elem:
        annotation: dict = {"type": a.tag, "text": a.text}
        annotation.update(
            {k.replace("v_", "").replace("g_", ""): v for k, v in a.attrib.items()}
        )
        annotations.append(annotation)

    annotations_text = [a["text"] for a in annotations]
    annotation_idx = 0
    text = ""
    for text_chunk in annotated_elem.itertext():
        if text_chunk in annotations_text[annotation_idx:]:
            annotation = annotations[annotation_idx]
            annotation["start"] = len(text)
            annotation["end"] = annotation["start"] + len(text_chunk)
            annotation_idx += 1
        text += text_chunk

    assert annotation_idx == len(
        annotations
    ), "Not all annotations were found when computing offsets"

    return text, annotations


def parse_example(example: ET.Element) -> dict:
    pmid_elem = safe_find(elem=example, value="Pmid")
    pmid = pmid_elem.text

    title_elem = safe_find(elem=example, value="Title")
    title, title_annotations = extract_text_and_annotations(title_elem)

    abstract_elem = safe_find(elem=example, value="Abstract")
    abstract, abstract_annotations = extract_text_and_annotations(abstract_elem)

    for a in abstract_annotations:
        a["start"] += len(title) + 1
        a["end"] += len(title) + 1

    annotations = title_annotations + abstract_annotations

    for a in annotations:
        a["ids"] = [NIL if i == "No" else i for i in a.pop("id").split(",")]
        a["metadata"] = {}
        a["metadata"]["lex"] = a.pop("lex")
        if "norm" in a:
            a["metadata"]["norm"] = a.pop("norm")

    return {
        "id": pmid,
        "text": " ".join([title, abstract]),
        "annotations": annotations,
        "metadata": {
            "pmid": pmid,
            "passages": [
                {"offset": 0, "type": "title"},
                {"offset": len(title) + 1, "type": "abstract"},
            ],
        },
    }


class OsirisBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        annotation_features = ANNOTATION_FEATURES.copy()
        annotation_features.update({"lex": Value("string"), "norm": Value("string")})

        features = CORPUS_FEATURES.copy()
        features["annotations"] = [annotation_features]
        features["metadata"]["pmid"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name="osiris",
        )

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            base_path = Path(self.config.data_dir)
            path = base_path / data_files["file"]
        else:
            downloaded = dl_manager.download(data_files)
            path = Path(downloaded["file"])

        return [
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": path},
            )
        ]

    def _generate_examples(self, path: Path):
        for idx, e in enumerate(ET.parse(path).getroot()):
            yield idx, parse_example(example=e)


class OsirisCorpus(BaseCorpus):
    dataset_name = "osiris"
    builder_class = OsirisBuilder

    def __init__(self, entity_types: tuple[str, ...] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.entity_types = self.parse_entity_types(
            entity_types=entity_types,
            supported_entity_types=SUPPORTED_ENTITY_TYPES,  # type: ignore
        )

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.entity_types is not None and len(self.entity_types) < len(SUPPORTED_ENTITY_TYPES):

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
                desc=f"Filtering Osiris by entity types: {self.entity_types}",
            )
        return ds
