from importlib.resources import files
from pathlib import Path

import bioc
import pandas as pd
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
from omegaconf import OmegaConf

from ..utils import NIL, EntityType
from .base import CORPUS_FEATURES, BaseCorpus

DATASET_NAME = "bioid"

SPLITS = OmegaConf.load(str(files("belb.assets.splits") / "bioid.yaml"))

PREFIX_TO_TYPE = {
    "Cellosaurus": EntityType.CELL_LINE,
    "CHEBI": EntityType.CHEMICAL,
    "PubChem": EntityType.CHEMICAL,
    "NCBI gene": EntityType.GENE,
    "Uniprot": EntityType.GENE,
    "protein": EntityType.GENE,
    "gene": EntityType.GENE,
    "NCBI taxon": EntityType.SPECIES,
    "organism": EntityType.SPECIES,
    "CL": "CELL",
    "cell": "CELL",
    "BAO": "BIOASSAY",
    "Corum": "PROTEIN-COMPLEX",
    "GO": "BIOLOGICAL-PROCESS/MOLECULAR-FUNCTION/CELLULAR-COMPONENT",
    "Rfam": "RNA-FAMILIES",
    "Uberon": "ANATOMICAL-PART",
    "molecule": "MOLECULE",
    "subcellular": "SUBCELLULAR",
    "tissue": "TISSUE",
}

SUPPORTED_ENTITY_TYPES = sorted(set(PREFIX_TO_TYPE.values()))


def clean_text(text: str) -> str:
    return text.replace("\u2010", "-").replace("\u2212", "-")


def load_id_to_annotations(file: Path) -> dict:
    nil_prefix = {"molecule", "subcellular", "tissue", "cellorganismgene", "protein"}

    df = pd.read_csv(file, low_memory=False)
    df["text"] = df["text"].apply(lambda x: clean_text(x))
    df["obj"] = df["obj"].apply(lambda x: x.replace("CVCL", "Cellosaurus:CVCL"))
    df["obj"] = df["obj"].apply(lambda x: x.replace("Uberon:UBERON", "Uberon"))

    annotations: dict = {}
    for row in df.to_dict("records"):
        pmcid = str(row["don_article"])
        figure = row["figure"]
        key = f"{pmcid} {figure}"

        # 26019:sdPaper1495,10.15252/embr.201540933,4818770,Figure_1-A,6,13,64,72,8,8,,ES cells,,CVCL_6412|CL:0002322,,yes,
        if row["obj"] == "Cellosaurus:CVCL_6412|CL:0002322":
            row["obj"] = row["obj"].replace("|CL:0002322", "")

        # 5125832,Figure_3-G,aspartate aminotransferase,Uniprot:P05201|Uniprot:P05202
        prefixes_ids: list[str] = row["obj"].split("|")

        prefixes, ids = zip(*[p.split(":") for p in prefixes_ids])
        types = set(PREFIX_TO_TYPE[p] for p in prefixes)

        assert len(types) == 1

        ids_parsed = []
        for p, i in zip(prefixes, ids):
            if p in nil_prefix:
                ids_parsed.append(NIL)
            else:
                ids_parsed.append(i)

        a = {
            "start": row["first left"],
            "end": row["last right"],
            "text": row["text"],
            "type": next(iter(types)),
            "ids": ids_parsed,
        }

        if key not in annotations:
            annotations[key] = []
        annotations[key].append(a)

    return annotations


class BioIdBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        features = CORPUS_FEATURES.copy()
        features["metadata"]["pmcid"] = Value("string")
        features["metadata"]["figure"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name=DATASET_NAME,
        )

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        path = Path(dl_manager.extract(data_files["folder"])) / "BioIDtraining_2"

        id_to_annotations = load_id_to_annotations(path / "annotations.csv")

        return [
            SplitGenerator(
                name=Split.TRAIN,
                gen_kwargs={
                    "folder": path,
                    "ids": SPLITS["train"],  # type: ignore
                    "id_to_annotations": id_to_annotations,
                },
            ),
            SplitGenerator(
                name=Split.VALIDATION,
                gen_kwargs={
                    "folder": path,
                    "ids": SPLITS["validation"],  # type: ignore
                    "id_to_annotations": id_to_annotations,
                },
            ),
            SplitGenerator(
                name=Split.TEST,
                gen_kwargs={
                    "folder": path,
                    "ids": SPLITS["test"],  # type: ignore
                    "id_to_annotations": id_to_annotations,
                },
            ),
        ]

    def _generate_examples(self, folder: Path, ids: list[str], id_to_annotations: dict):

        idx = 0
        for i in ids:
            path = folder / "caption_bioc" / f"{i}.xml"
            collection = bioc.load(path)
            for d in collection.documents:
                pmcid = d.infons["pmc_id"]
                figure = d.infons["figure"]
                text = clean_text(d.passages[0].text)

                yield idx, {
                    "id": d.id,
                    "text": text,
                    "annotations": id_to_annotations.get(d.id, []),
                    "metadata": {
                        "passages": [{"offset": 0, "type": "figure"}],
                        "pmcid": pmcid,
                        "figure": figure,
                    },
                }
                idx += 1


class BioIdCorpus(BaseCorpus):
    dataset_name = "bioid"
    builder_class = BioIdBuilder

    def __init__(self, entity_types: tuple[str, ...] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.entity_types = self.parse_entity_types(
            entity_types=entity_types,
            supported_entity_types=SUPPORTED_ENTITY_TYPES,
        )

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.entity_types is not None and len(self.entity_types) < len(
            SUPPORTED_ENTITY_TYPES
        ):

            def filter_annotations(example):
                example["annotations"] = [
                    a for a in example["annotations"] if a["type"] in self.entity_types
                ]
                return example

            ds = ds.map(
                filter_annotations,
                num_proc=self.num_proc,
                desc=f"Filtering BioID by entity types: {self.entity_types}",
            )
        return ds
