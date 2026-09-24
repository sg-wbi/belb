import json
from collections.abc import Iterator
from pathlib import Path

from datasets import (
    DatasetInfo,
    DownloadManager,
    Features,
    GeneratorBasedBuilder,
    Sequence,
    Split,
    SplitGenerator,
    Value,
    Version,
)

from .base import ENTITIES_FEATURES, BaseKb, KbBuilderConfig, KbTable, kb_registry

DATASET_NAME = "mondo"


def parse_mondo(path: Path) -> Iterator[dict]:

    with open(path) as fp:
        mondo = json.load(fp)

    for node in mondo["graphs"][0]["nodes"]:
        identifier = node["id"].split("/")[-1]
        if identifier.startswith("MONDO_"):
            if node.get("type") == "CLASS":
                if node.get("meta", {}).get("deprecated"):
                    continue

                name = node["lbl"]

                entry = {
                    "id": identifier,
                    "label": name,
                    "description": None,
                    "xrefs": [],
                }

                meta = node.get("meta")
                if meta is not None:
                    aliases = [a.get("val") for a in meta.get("synonyms", [])]
                    aliases = [a for a in aliases if a != name]
                    if aliases:
                        entry["aliases"] = aliases

                    description = meta.get("definition", {}).get("val")
                    if description:
                        entry["description"] = description

                    xrefs = [x["val"] for x in meta.get("xrefs", [])]
                    if xrefs:
                        entry["xrefs"] = xrefs

                yield entry


class MondoBuilder(GeneratorBasedBuilder):

    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            features = ENTITIES_FEATURES.copy()
            features.update(
                {
                    "description": Value("string"),
                    "xrefs": Sequence(Value("string")),
                }
            )
            return DatasetInfo(
                features=Features(features),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.ENTITIES),
            )
        else:
            raise ValueError(f"Invalid `table={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:

        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        if self.config.data_dir:
            gen_kwargs = {"file": Path(self.config.data_dir) / data_files["file"]}
        else:
            downloaded = dl_manager.download_and_extract(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, file: Path):
        for idx, e in enumerate(parse_mondo(path=file)):
            e["idx"] = idx
            yield (idx, e)


@kb_registry.register(DATASET_NAME)
class MondoKb(BaseKb):
    dataset_name = DATASET_NAME
    builder_class = MondoBuilder

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
