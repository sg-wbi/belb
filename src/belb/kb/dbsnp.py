import bz2
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

from ..utils import NIL
from .base import (
    ENTITIES_FEATURES,
    HISTORY_FEATURES,
    BaseKb,
    KbBuilderConfig,
    KbTable,
    kb_registry,
)

DATASET_NAME = "dbsnp"


def stream_lines(files: list[Path]) -> Iterator[str]:
    for file in files:
        if not file.exists():
            continue

        with bz2.open(file) as fp:
            for line in fp:
                yield line.decode("utf-8")


def parse_line(line: str) -> list[dict]:
    """
    Extract rsid, HGVS notations and related gene from json line in dbSNP dump
    """

    row = json.loads(line)

    rsid = str(row["refsnp_id"])

    primary_data = row.get("primary_snapshot_data")
    if primary_data is None:
        return []

    assembly_annotations = [
        aa
        for allele_annotations in primary_data.get("allele_annotations", [])
        for aa in allele_annotations.get("assembly_annotation", [])
    ]

    genes = [gene for aa in assembly_annotations for gene in aa.get("genes", [])]
    gene_ids = list(set(gene["id"] for gene in genes))
    gene_ids = [-1] if len(gene_ids) == 0 else gene_ids

    refseq_hgvs = [
        a.get("hgvs")
        for pwa in primary_data.get("placements_with_allele", [])
        for a in pwa.get("alleles", [])
        if a.get("allele", {}).get("spdi", {}).get("deleted_sequence", "G")
        != a.get("allele", {}).get("spdi", {}).get("inserted_sequence", "A")
    ]

    refseq_hgvs = [
        r
        for r in refseq_hgvs
        if (
            r is not None
            and r != "not_yet_implemented"
            and not r.startswith(("XM_", "XR_", "XP_", "GPC_", "YP_"))
        )
    ]

    entries: list[dict] = []
    for gene_id in gene_ids:
        entry = {
            "id": rsid,
            "label": f"rs{rsid}",
            "aliases": refseq_hgvs,
            "gene_id": str(gene_id) if gene_id != -1 else None,
            "references": row.get("citations", []),
        }
        entries.append(entry)

    return entries


def parse_merged(row: dict) -> Iterator[dict]:
    merged_into = row.get("merged_snapshot_data", {}).get("merged_into", [])

    if len(merged_into) == 0:
        new = [NIL]
    else:
        new = merged_into

    for m in row.get("dbsnp1_merges", []):
        for n in new:
            yield {
                "obsolete": str(m["merged_rsid"]),
                "update": str(n),
            }


def parse_dbsnp(files: list[Path]) -> Iterator[dict]:
    for line in stream_lines(files):
        yield from parse_line(line)


def parse_history(files: list[Path]) -> Iterator[dict]:
    for file in files:
        for line in stream_lines([file]):
            row = json.loads(line)
            if "merged" in file.name:
                yield from parse_merged(row)
            else:
                yield {"obsolete": str(row["refsnp_id"]), "update": NIL}


class DbsnpBuilder(GeneratorBasedBuilder):
    VERSION = Version("0.0.0")
    BUILDER_CONFIG_CLASS = KbBuilderConfig

    def _info(self):
        if self.config.name == KbTable.ENTITIES:
            features = ENTITIES_FEATURES.copy()
            features.update(
                {
                    "references": Sequence(Value("string")),
                    "gene_id": Value("int32"),
                }
            )
            return DatasetInfo(
                features=Features(features),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.ENTITIES),
            )
        elif self.config.name == KbTable.HISTORY:
            return DatasetInfo(
                features=Features(HISTORY_FEATURES),
                dataset_name=DATASET_NAME,
                config_name=str(KbTable.HISTORY),
            )
        else:
            raise ValueError(f"Invalid `table={self.config.name}`")

    def _split_generators(self, dl_manager: DownloadManager) -> list[SplitGenerator]:
        if not self.config.data_dir:
            raise ValueError(f"{DATASET_NAME} is local only: specify `data_dir`")

        return [
            SplitGenerator(
                name=Split.TRAIN, gen_kwargs={"files": self.config.data_files}
            )
        ]

    def _generate_examples(self, files: dict):
        if self.config.name == KbTable.ENTITIES:
            for idx, e in enumerate(parse_dbsnp([Path(f) for f in files["chr"]])):
                e["idx"] = idx
                yield idx, e
        elif self.config.name == KbTable.HISTORY:
            history_files = [files["unsupported"], files["merged"], files["withdrawn"]]
            for idx, e in enumerate(parse_history([Path(f) for f in history_files])):
                yield idx, e


@kb_registry.register(DATASET_NAME)
class DbsnpKb(BaseKb):
    dataset_name = DATASET_NAME
    has_history = True
    builder_class = DbsnpBuilder

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
