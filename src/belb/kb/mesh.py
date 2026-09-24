import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path

from datasets import (
    DatasetDict,
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

DATASET_NAME = "mesh"

BRANCHES = (
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "V",
    "Z",
)


def get_aliases(elem: ET.Element, label: str):
    # Collect Entry Terms (synonyms)
    aliases = [
        term.text
        for term in elem.findall(".//Concept/TermList/Term/String")
        if term.text
    ]

    return [a for a in aliases if a != label]


def parse_mesh_desc(desc_file: str | Path) -> Iterator[dict]:

    for event, elem in ET.iterparse(str(desc_file), events=("end",)):
        if elem.tag == "DescriptorRecord":
            ui = elem.findtext(".//DescriptorUI")

            # Get all tree numbers
            label = elem.findtext(".//DescriptorName/String")
            if label is None:
                raise ValueError(f"{ui} has no label")

            entry: dict = {
                "id": ui,
                "label": label,
                "tree_numbers": [
                    tn.text for tn in elem.findall(".//TreeNumber") if tn.text
                ],
            }

            aliases = get_aliases(elem=elem, label=label)
            if aliases:
                entry["aliases"] = aliases

            # Collect Scope Notes
            scope_notes = [
                note.text for note in elem.findall(".//Concept/ScopeNote") if note.text
            ]
            if scope_notes:
                entry["description"] = " ".join(scope_notes).strip()

            qualifiers = elem.findall("AllowableQualifiersList/AllowableQualifier")
            if qualifiers:
                entry["qualifiers"] = []
                for q in qualifiers:
                    qui = q.find("QualifierReferredTo/QualifierUI")
                    name = q.find("QualifierReferredTo/QualifierName/String")
                    # q.find("Abbreviation").text
                    entry["qualifiers"].append(
                        {
                            "qui": qui.text if qui is not None else None,
                            "name": name.text if name is not None else None,
                        }
                    )

            pas = elem.findall("PharmacologicalActionList/PharmacologicalAction")
            if pas:
                entry["pharmacological_actions"] = []
                for pa in pas:
                    dui = pa.find("DescriptorReferredTo/DescriptorUI")
                    name = pa.find("DescriptorReferredTo/DescriptorName/String")
                    entry["pharmacological_actions"].append(
                        {
                            "dui": dui.text if dui is not None else None,
                            "name": name.text if name is not None else None,
                        }
                    )

            yield entry


def parse_mesh_supp(
    supp_file: str | Path, id_to_tns: dict[str, list[str]]
) -> Iterator[dict]:
    for event, elem in ET.iterparse(str(supp_file), events=("end",)):
        if elem.tag == "SupplementalRecord":

            ui = elem.findtext(".//SupplementalRecordUI")
            label = elem.findtext(".//SupplementalRecordName/String")
            if label is None:
                raise ValueError(f"{ui} has no label")

            entry: dict = {
                "id": ui,
                "label": label,
            }

            mapped_descriptors = []
            for h in elem.findall(".//{*}HeadingMappedTo"):
                ref = h.find(".//{*}DescriptorReferredTo")
                if ref is not None:
                    dui = ref.findtext("{*}DescriptorUI")
                    if dui:
                        mapped_descriptors.append(dui.lstrip("*"))

            entry["mapped_to_ids"] = mapped_descriptors
            entry["tree_numbers"] = [
                tn
                for dui in mapped_descriptors
                for tns in id_to_tns.get(dui, [])
                for tn in tns
            ]

            aliases = get_aliases(elem=elem, label=label)
            if aliases:
                entry["aliases"] = aliases

            notes = [note.text for note in elem.findall(".//Note") if note.text]
            if notes:
                entry["description"] = " ".join(notes)

            yield entry


class MeshBuilder(GeneratorBasedBuilder):

    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            features = ENTITIES_FEATURES.copy()
            features.update(
                {
                    "description": Value("string"),
                    "mapped_to_ids": Sequence(Value("string")),
                    "tree_numbers": Sequence(Value("string")),
                    "qualifiers": [
                        {
                            "qui": Value("string"),
                            "name": Value("string"),
                        }
                    ],
                    "pharmacological_actions": [
                        {
                            "id": Value("string"),
                            "name": Value("string"),
                        }
                    ],
                }
            )
            return DatasetInfo(
                features=Features(features),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.ENTITIES),
            )

        else:
            raise ValueError(f"Invalid `name={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:

        # dict[str,str] is converted into dic[str, list[str]]
        data_files = {k: v[0] for k, v in self.config.data_files.items()}

        # local
        if self.config.data_dir:
            gen_kwargs = {
                "desc_file": Path(self.config.data_dir) / data_files["desc_file"]
            }
            if "supp_file" in data_files:
                gen_kwargs["supp_file"] = (
                    Path(self.config.data_dir) / data_files["supp_file"]
                )
        else:
            downloaded = dl_manager.download_and_extract(data_files)
            gen_kwargs = {k: Path(v) for k, v in downloaded.items()}

        return [SplitGenerator(name=Split.TRAIN, gen_kwargs=gen_kwargs)]

    def _generate_examples(self, desc_file: Path, supp_file: Path | None = None):
        id_to_tns = {}
        idx = 0
        for e in parse_mesh_desc(desc_file=desc_file):
            id_to_tns[e["id"]] = e["tree_numbers"]
            e["idx"] = idx
            yield (idx, e)
            idx += 1

        if supp_file is not None:
            for e in parse_mesh_supp(supp_file=supp_file, id_to_tns=id_to_tns):
                e["idx"] = idx
                yield (e["id"], e)
                idx += 1


@kb_registry.register(DATASET_NAME)
class MeshKb(BaseKb):
    dataset_name = DATASET_NAME
    builder_class = MeshBuilder

    def __init__(
        self,
        branches: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if branches is not None:
            if not all(s.startswith((BRANCHES)) for s in branches):
                raise ValueError(
                    f"All items in `branches` must start with one of {tuple(BRANCHES)}"
                )
        self.branches = branches

    def _postprocess(self, ds: DatasetDict) -> DatasetDict:
        if self.branches is not None:
            ds[KbTable.ENTITIES] = ds[KbTable.ENTITIES].filter(
                lambda e: any(
                    t.startswith(tuple(self.branches)) for t in e["tree_numbers"]
                ),
                num_proc=self.num_proc,
                desc=f"mesh: select entities under branches: {tuple(self.branches)}",
            )
            ds[KbTable.ENTITIES] = self.reindex(ds[KbTable.ENTITIES])
        return ds
