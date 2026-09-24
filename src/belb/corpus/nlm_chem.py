from pathlib import Path

import bioc
import datasets
from datasets import Features, Split

from ..utils import NIL
from .base import CORPUS_FEATURES, BaseCorpus, BelbCorpusConfig

DATASET_NAME = "nlm-chem"


def parse_annotations(
    annotations: list[bioc.BioCAnnotation],
) -> list[dict]:
    parsed = []

    for a in annotations:
        annotation = {
            "type": a.infons["type"],
        }

        if annotation["type"] in ["MeSH_Indexing_Chemical", "OTHER"]:
            annotation.update({"start": -1, "end": -1, "text": ""})
        else:
            annotation.update(
                {
                    "start": a.total_span.offset,
                    "end": a.total_span.end,
                    "text": a.text,
                }
            )

        identifier = a.infons.get("identifier", NIL)
        if identifier == "-":
            identifier = NIL

        if identifier != NIL:
            identifiers = [i.strip() for i in identifier.split(",")]
        else:
            identifiers = [identifier]

        identifiers = [i.replace("MESH:", "") for i in identifiers if i != "-"]

        annotation["ids"] = identifiers

        parsed.append(annotation)

    return parsed


class NlmChemBuilder(datasets.GeneratorBasedBuilder):
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

        folder = Path(folder)

        return [
            datasets.SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={"path": folder / "BC7T2-NLMChem-corpus-train.BioC.xml"},
            ),
            datasets.SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={"path": folder / "BC7T2-NLMChem-corpus-dev.BioC.xml"},
            ),
            datasets.SplitGenerator(
                name=Split.TEST,
                gen_kwargs={"path": folder / "BC7T2-NLMChem-corpus-test.BioC.xml"},
            ),
        ]

    def _generate_examples(self, path: str):
        with Path(path).open() as fp:
            collection = bioc.load(fp)

            for example in collection.documents:
                metadata = {"pmcid": example.id, "passages": []}
                text = ""
                annotations = []

                for p in example.passages:
                    # Add padding to match absolute BioC offsets
                    if p.offset > len(text):
                        text += " " * (p.offset - len(text))

                    metadata["passages"].append(
                        {"type": p.infons.get("type", "passage"), "offset": p.offset}
                    )

                    text += p.text

                    annotations.extend(
                        parse_annotations(
                            annotations=p.annotations,
                        )
                    )

                yield example.id, {
                    "id": example.id,
                    "text": text,
                    "annotations": annotations,
                    "metadata": metadata,
                }


class NlmChemCorpus(BaseCorpus):
    dataset_name = DATASET_NAME
    builder_class = NlmChemBuilder
