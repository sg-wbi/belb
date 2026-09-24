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

from ..utils import NIL, EntityType
from .base import ANNOTATION_FEATURES, CORPUS_FEATURES, BaseCorpus
from .parsing import parse_pubtator_example

SUPPORTED_ENTITY_TYPES = [EntityType.VARIANT, EntityType.GENE, EntityType.SPECIES]

VARIANT_ENTITY_TYPES = {
    "DNAMutation",
    "ProteinMutation",
    "SNP",
    "AcidChange",
    "ProteinAllele",
    "DNAAllele",
}


def parse_annotations(
    annotations: list[pubtator.PubTatorAnn],
) -> list[dict]:

    parsed = []
    for a in annotations:
        metadata = None
        if a.type in VARIANT_ENTITY_TYPES:
            ids = []
            metadata = {}
            for f in a.id.split(";"):
                if f.startswith("CorrespondingGene"):
                    metadata["corresponding_gene"] = f.split(":")[1]
                elif f.startswith("VariantGroup"):
                    metadata["variant_group"] = f.split(":")[1]
                elif f.startswith("RS#"):
                    ids.append(f.split(":")[1])
                elif f.startswith("rs"):
                    ids.append(f.replace("rs", ""))
                else:
                    metadata["tmvar_repr"] = f
            if not ids:
                ids = [NIL]
        else:
            ids = a.id.replace(";", ",").split(",")
            ids = [NIL if i == "-" else i for i in ids]

        p = {"type": a.type, "start": a.start, "end": a.end, "text": a.text, "ids": ids}
        if metadata:
            p["metadata"] = metadata

        parsed.append(p)

    return parsed


class Tmvar3Builder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")

    def _info(self):
        annotation_features = ANNOTATION_FEATURES.copy()
        annotation_features.update(
            {
                "corresponding_gene": Value("string"),
                "variant_group": Value("string"),
                "tmvar_repr": Value("string"),
            }
        )

        features = CORPUS_FEATURES.copy()
        features["annotations"] = [annotation_features]
        features["metadata"]["pmid"] = Value("string")

        return DatasetInfo(
            features=Features(features),
            dataset_name="tmvar3",
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
        with path.open() as fp:
            examples = pubtator.load(fp)
            for idx, e in enumerate(examples):
                annotations = parse_annotations(annotations=e.annotations)
                e.annotations = []
                parsed = parse_pubtator_example(e)
                parsed["annotations"] = annotations
                yield idx, parsed


class Tmvar3Corpus(BaseCorpus):
    dataset_name = "tmvar3"
    builder_class = Tmvar3Builder

    def __init__(self, entity_types: tuple[str, ...] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.entity_types = self.parse_entity_types(
            entity_types=entity_types,
            supported_entity_types=SUPPORTED_ENTITY_TYPES,  # type: ignore
        )

    def postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.entity_types is not None and len(self.entity_types) < len(SUPPORTED_ENTITY_TYPES):

            def filter_annotations(example):
                filtered = []
                for a in example["annotations"]:
                    a_type = (
                        EntityType.VARIANT
                        if a["type"] in VARIANT_ENTITY_TYPES
                        else a["type"].upper()
                    )
                    if a_type in self.entity_types:
                        filtered.append(a)
                example["annotations"] = filtered
                return example

            ds = ds.map(
                filter_annotations,
                num_proc=self.num_proc,
                desc=f"Filtering tmVar3 by entity types: {self.entity_types}",
            )
        return ds
