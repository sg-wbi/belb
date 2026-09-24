from collections.abc import Mapping

from datasets import DatasetDict

from ..config import BelbConfig
from .base import CORPUS_FEATURES
from .bc5cdr import Bc5cdrCorpus
from .bioid import BioIdCorpus
from .biored import BioredCorpus
from .gnormplus import GnormplusCorpus
from .linnaeus import LinnaeusCorpus
from .medmentions import MedMentionsCorpus
from .ncbi_disease import NcbiDiseaseCorpus
from .nlm_chem import NlmChemCorpus
from .nlm_gene import NlmGeneCorpus
from .osiris import OsirisCorpus
from .s800 import S800Corpus
from .s1000 import S1000Corpus
from .snp import SnpCorpus
from .tmvar3 import Tmvar3Corpus

LOADERS = {
    "bc5cdr": Bc5cdrCorpus,
    "ncbi-disease": NcbiDiseaseCorpus,
    "nlm-chem": NlmChemCorpus,
    "nlm-gene": NlmGeneCorpus,
    "gnormplus": GnormplusCorpus,
    "linnaeus": LinnaeusCorpus,
    "s800": S800Corpus,
    "s1000": S1000Corpus,
    "biored": BioredCorpus,
    "medmentions": MedMentionsCorpus,
    "tmvar3": Tmvar3Corpus,
    "osiris": OsirisCorpus,
    "snp": SnpCorpus,
    "bioid": BioIdCorpus,
}


def load_corpus(
    name: str,
    config: BelbConfig,
    id_mapping: bool | str | Mapping = True,  # TODO: allow to pass file path
    annotation_patches: bool | str | Mapping = True,
    # any corpus-specific argument (e.g., entity type, subset,  ...)
    corpus_kwargs: dict | None = None,
    num_proc: int | None = None,
) -> DatasetDict:
    """
    id_mapping:
        bool: False = no mapping, True =  hard-coded default from options in `assets/id_mapping/{name}.yaml`
        str: option in `assets/id_mapping/{name}.yaml`
        dict: your own mapping

    For instance for `ncbi-disease` True = `mesh-disease`. But you can pass also id_mapping=ctd-diseases
    """
    try:
        resource = config.corpora[name]
    except KeyError:
        raise ValueError(f"Corpus `{name}` is not defined in configuration file.")

    corpus_kwargs = corpus_kwargs if corpus_kwargs is not None else {}
    corpus_kwargs["name"] = name

    for key, value in [
        ("num_proc", num_proc),
        ("id_mapping", id_mapping),
        ("annotation_patches", annotation_patches),
        ("task", config.task),
    ]:
        if key not in corpus_kwargs:
            corpus_kwargs[key] = value

    loader_class = LOADERS[name]

    loader = loader_class(**corpus_kwargs)

    return loader.load(resource)


__all__ = ["CORPUS_FEATURES"]
