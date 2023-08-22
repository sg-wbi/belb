#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Available corpora with corresponding default configurations
"""
from ..preprocessing.data import Entities
from ..resources import Corpora
from .bc5cdr import bc5cdr
from .bc5cdr.bc5cdr import Bc5CdrCorpusConfig, Bc5CdrCorpusParser
from .bioid import bioid
from .bioid.bioid import BioIdCorpusConfig, BioIdCorpusParser
from .corpus import BelbCorpus
from .gnormplus import gnormplus
from .gnormplus.gnormplus import GNormPlusCorpusConfig, GNormPlusCorpusParser
from .linnaeus import linnaeus
from .linnaeus.linnaeus import LinnaeusCorpusConfig, LinnaeusCorpusParser
from .medmentions import medmentions
from .medmentions.medmentions import (MedMentionsCorpusConfig,
                                      MedMentionsCorpusParser)
from .ncbi_disease import ncbi_disease
from .ncbi_disease.ncbi_disease import (NcbiDiseaseCorpusConfig,
                                        NcbiDiseaseCorpusParser)
from .nlm_chem import nlm_chem
from .nlm_chem.nlm_chem import NlmChemCorpusConfig, NlmChemCorpusParser
from .nlm_gene import nlm_gene
from .nlm_gene.nlm_gene import NlmGeneCorpusConfig, NLMGeneCorpusParser
from .osiris import osiris
from .osiris.osiris import OsirisCorpusConfig, OsirisCorpusParser
from .parser import BaseBelbCorpusConfig, BaseBelbCorpusParser
from .s800 import s800
from .s800.s800 import S800CorpusConfig, S800CorpusParser
from .snp import snp
from .snp.snp import SnpCorpusConfig, SnpCorpusParser
from .tmvar import tmvar
from .tmvar.tmvar import TmVarCorpusConfig, TmVarCorpusParser

ENTITY_TO_CORPORA_NAMES: dict = {
    Entities.GENE: [Corpora.GNORMPLUS.name, Corpora.NLM_GENE.name],
    Entities.SPECIES: [Corpora.S800.name, Corpora.LINNAEUS.name],
    Entities.VARIANT: [Corpora.SNP.name, Corpora.OSIRIS.name, Corpora.TMVAR.name],
    Entities.CELL_LINE: [Corpora.BIOID.name],
    Entities.CHEMICAL: [Corpora.BC5CDR.name, Corpora.NLM_CHEM.name],
    Entities.DISEASE: [Corpora.BC5CDR.name, Corpora.NCBI_DISEASE.name],
    Entities.UMLS: [Corpora.MEDMENTIONS.name],
}

NAME_TO_CORPUS_CONFIG = {
    Corpora.GNORMPLUS.name: GNormPlusCorpusConfig,
    Corpora.NLM_GENE.name: NlmGeneCorpusConfig,
    Corpora.NCBI_DISEASE.name: NcbiDiseaseCorpusConfig,
    Corpora.BC5CDR.name: Bc5CdrCorpusConfig,
    Corpora.NLM_CHEM.name: NlmChemCorpusConfig,
    Corpora.BIOID.name: BioIdCorpusConfig,
    Corpora.LINNAEUS.name: LinnaeusCorpusConfig,
    Corpora.S800.name: S800CorpusConfig,
    Corpora.SNP.name: SnpCorpusConfig,
    Corpora.OSIRIS.name: OsirisCorpusConfig,
    Corpora.TMVAR.name: TmVarCorpusConfig,
    Corpora.MEDMENTIONS.name: MedMentionsCorpusConfig,
}

NAME_TO_CORPUS_PARSER = {
    Corpora.GNORMPLUS.name: GNormPlusCorpusParser,
    Corpora.NLM_GENE.name: NLMGeneCorpusParser,
    Corpora.NCBI_DISEASE.name: NcbiDiseaseCorpusParser,
    Corpora.BC5CDR.name: Bc5CdrCorpusParser,
    Corpora.NLM_CHEM.name: NlmChemCorpusParser,
    Corpora.BIOID.name: BioIdCorpusParser,
    Corpora.LINNAEUS.name: LinnaeusCorpusParser,
    Corpora.S800.name: S800CorpusParser,
    Corpora.SNP.name: SnpCorpusParser,
    Corpora.OSIRIS.name: OsirisCorpusParser,
    Corpora.TMVAR.name: TmVarCorpusParser,
    Corpora.MEDMENTIONS.name: MedMentionsCorpusParser,
}

NAME_TO_CORPUS_MODULE = {
    Corpora.GNORMPLUS.name: gnormplus,
    Corpora.NLM_GENE.name: nlm_gene,
    Corpora.NCBI_DISEASE.name: ncbi_disease,
    Corpora.BC5CDR.name: bc5cdr,
    Corpora.NLM_CHEM.name: nlm_chem,
    Corpora.BIOID.name: bioid,
    Corpora.LINNAEUS.name: linnaeus,
    Corpora.S800.name: s800,
    Corpora.SNP.name: snp,
    Corpora.OSIRIS.name: osiris,
    Corpora.TMVAR.name: tmvar,
    Corpora.MEDMENTIONS.name: medmentions,
}


class AutoCorpusParser:
    """
    Import util
    """

    @staticmethod
    def from_name(name: str, **kwargs) -> BaseBelbCorpusParser:
        """
        Get corpus parser from name
        """
        if name not in NAME_TO_CORPUS_PARSER:
            raise ValueError(
                f"Unknown corpus `{name}`: must be one of {list(NAME_TO_CORPUS_PARSER)}"
            )
        return NAME_TO_CORPUS_PARSER[name](**kwargs)


class AutoBelbCorpusConfig:
    """
    Import util
    """

    @staticmethod
    def from_name(name: str, **kwargs) -> BaseBelbCorpusConfig:
        """
        Get corpus config from name
        """
        if name not in NAME_TO_CORPUS_CONFIG:
            raise ValueError(
                f"Unknown corpus `{name}`: must be one of {list(NAME_TO_CORPUS_CONFIG)}"
            )
        return NAME_TO_CORPUS_CONFIG[name](**kwargs)


class AutoBelbCorpus:
    """
    Wrapper to get a corpus from name
    """

    @staticmethod
    def from_name(
        name: str,
        directory: str,
        **kwargs,
    ):
        """
        Get corpus from name w/ default attributes
        """

        config = AutoBelbCorpusConfig.from_name(name=name, **kwargs)

        return BelbCorpus(directory=directory, config=config)
