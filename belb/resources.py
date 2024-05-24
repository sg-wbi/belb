#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List all resources included in BELB
"""

from dataclasses import dataclass, field, fields
from typing import Optional

from belb.utils import download

CHROMOSOMES = list(str(i) for i in range(1, 23)) + ["X", "Y", "MT"]
DBSNP_FILES = ["refsnp-unsupported.json.bz2", "refsnp-withdrawn.json.bz2"] + [
    f"refsnp-chr{i}.json.bz2" for i in CHROMOSOMES
]


@dataclass(frozen=True)
class Resource:
    """
    Container for resource downloads
    """

    name: str
    base_url: Optional[str] = None
    files: list[str] = field(default_factory=list)
    extract: bool = False

    def download(self, directory: str, extract: Optional[bool] = None):
        """
        Fetch corpus files
        """
        extract = self.extract if extract is None else extract

        if self.base_url is not None and self.files is not None:
            for file in self.files:
                download(
                    base_url=self.base_url,
                    file=file,
                    directory=directory,
                    extract=extract,
                )


class IterableDataclass:
    """
    Make dataclass iterable
    """

    def __iter__(self):
        return iter([getattr(self, f.name) for f in fields(self)])

    def __len__(self):
        return len(list(iter(self)))


# ORDER MATTERS!!!
@dataclass(frozen=True)
class KbsContainer(IterableDataclass):
    """
    Container for all kbs resources
    """

    CTD_DISEASES: Resource = Resource(
        name="ctd_diseases",
        base_url="https://ctdbase.org/reports/",
        files=["CTD_diseases.tsv.gz"],
    )
    CTD_CHEMICALS: Resource = Resource(
        name="ctd_chemicals",
        base_url="http://ctdbase.org/reports/",
        files=["CTD_chemicals.tsv.gz"],
    )
    NCBI_TAXONOMY: Resource = Resource(
        name="ncbi_taxonomy",
        base_url="https://ftp.ncbi.nlm.nih.gov/pub/taxonomy",
        files=["taxdump.tar.gz"],
    )
    CELLOSAURUS: Resource = Resource(
        name="cellosaurus",
        base_url="https://ftp.expasy.org/databases/cellosaurus/",
        files=["cellosaurus.txt", "cellosaurus_deleted_ACs.txt"],
    )
    NCBI_GENE: Resource = Resource(
        name="ncbi_gene",
        base_url="https://ftp.ncbi.nih.gov/gene/DATA/",
        files=["gene_info.gz", "gene_history.gz"],
    )
    DBSNP: Resource = Resource(
        name="dbsnp",
        base_url="ftp://ftp.ncbi.nlm.nih.gov/snp/redesign/latest_release/JSON/",
        files=DBSNP_FILES,
    )
    UMLS: Resource = Resource(name="umls")


Kbs = KbsContainer()


@dataclass(frozen=True)
class CorporaContainer(IterableDataclass):
    """
    Container for all corpora
    """

    GNORMPLUS: Resource = Resource(
        name="gnormplus",
        base_url="https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/tmTools/download/GNormPlus",
        files=["GNormPlusCorpus.zip"],
        extract=True,
    )
    NLM_GENE: Resource = Resource(
        name="nlm_gene",
        base_url="https://zenodo.org/record/5089049/files/",
        files=["NLM-Gene-Corpus.zip"],
        extract=True,
    )
    NCBI_DISEASE: Resource = Resource(
        name="ncbi_disease",
        base_url="https://www.ncbi.nlm.nih.gov/CBBresearch/Dogan/DISEASE/",
        files=[
            "NCBItrainset_corpus.zip",
            "NCBIdevelopset_corpus.zip",
            "NCBItestset_corpus.zip",
        ],
        extract=True,
    )
    BC5CDR: Resource = Resource(
        name="bc5cdr",
        base_url="https://biocreative.bioinformatics.udel.edu/media/store/files/2016/",
        files=["CDR_Data.zip"],
        extract=True,
    )
    NLM_CHEM: Resource = Resource(
        name="nlm_chem",
        base_url="https://ftp.ncbi.nlm.nih.gov/pub/lu/BC7-NLM-Chem-track",
        files=["BC7T2-NLMChem-corpus_v2.BioC.xml.gz"],
        extract=True,
    )
    LINNAEUS: Resource = Resource(
        name="linnaeus",
        base_url="https://sourceforge.net/projects/linnaeus/files/Corpora/",
        files=["manual-corpus-species-1.1.tar.gz"],
        extract=True,
    )
    S800: Resource = Resource(
        name="s800",
        base_url="https://species.jensenlab.org/files/",
        files=["S800-1.0.tar.gz"],
        extract=True,
    )
    # S1000: Resource = Resource(
    #     name="s1000",
    #     base_url="https://jensenlab.org/assets/s1000/",
    #     files=["S1000-corpus.tar.gz"],
    #     extract=True,
    # )
    BIOID: Resource = Resource(
        name="bioid",
        base_url="https://biocreative.bioinformatics.udel.edu/media/store/files/2017/",
        files=["BioIDtraining_2.tar.gz"],
        extract=True,
    )
    OSIRIS: Resource = Resource(
        name="osiris",
        base_url="https://raw.githubusercontent.com/rockt/SETH/master/resources/OSIRIS",
        files=["corpus.xml"],
        extract=False,
    )
    SNP: Resource = Resource(
        name="snp",
        base_url="https://www.scai.fraunhofer.de/content/dam/scai/de/downloads/bioinformatik/",
        files=["normalization-variation-corpus.gz"],
        extract=True,
    )
    TMVAR: Resource = Resource(
        name="tmvar",
        base_url="https://ftp.ncbi.nlm.nih.gov/pub/lu/tmVar3",
        files=["tmVar3Corpus.txt"],
        extract=False,
    )
    MEDMENTIONS: Resource = Resource(
        name="medmentions",
        base_url="https://github.com/chanzuckerberg/MedMentions/archive/refs/heads/",
        files=["master.zip"],
        extract=True,
    )


Corpora = CorporaContainer()
