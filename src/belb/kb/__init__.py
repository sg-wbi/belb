from datasets import Dataset, DatasetDict

from ..config import BelbConfig, BelbConfigError
from .base import ENTITIES_FEATURES, HISTORY_FEATURES, KbTable, kb_registry
from .cellosaurus import CellosaurusKb
from .ctd_chemicals import CtdChemicalsKb
from .ctd_diseases import CtdDiseasesKb
from .dbsnp import DbsnpKb
from .mesh import MeshKb
from .mondo import MondoKb
from .ncbi_gene import NcbiGeneKb
from .ncbi_taxonomy import NcbiTaxonomyKb, load_taxid_names
from .transform import NameView, nameview_registry
from .umls import UmlsKb


def load_kb(
    name: str,
    config: BelbConfig,
    names: bool = False,
    kb_kwargs: dict | None = None,  # any kb-specific argument (e.g., subset)
    names_kwargs: dict | None = None,
    num_proc: int | None = None,
) -> Dataset | DatasetDict:
    try:
        resource = config.kbs[name]
    except KeyError:
        raise ValueError(f"KB `{name}` is not defined in configuration file.")

    kb_kwargs = kb_kwargs if kb_kwargs is not None else {}
    if "num_proc" not in kb_kwargs:
        kb_kwargs["num_proc"] = num_proc

    if (
        name == "ncbi-gene"
        and resource.hf_repo_id is None
        and "taxid_names" not in kb_kwargs
    ):
        if "ncbi-taxonomy" not in config.kbs:
            raise BelbConfigError(
                f"`ncbi-taxonomy` must be specified in `kbs.yaml` if loading local `{name}` "
                "(required to map the taxonomy ids to names)."
            )
        taxid_names = load_taxid_names(
            resource=config.kbs["ncbi-taxonomy"], num_proc=num_proc
        )
        kb_kwargs["taxid_names"] = taxid_names

    kb_class = kb_registry.get(name)
    loader = kb_class(**kb_kwargs)
    kb = loader.load(resource=resource)

    if names:
        names_kwargs = names_kwargs if names_kwargs is not None else {}
        nameview_class = nameview_registry.get(name)
        nameview = nameview_class(**names_kwargs)
        kb[KbTable.NAMES] = nameview.apply(
            entities=kb[KbTable.ENTITIES], num_proc=num_proc
        )

    return kb


__all__ = [
    "load_kb",
    "load_taxid_names",
    "kb_registry",
    "KbTable",
    "CellosaurusKb",
    "UmlsKb",
    "MeshKb",
    "CtdChemicalsKb",
    "CtdDiseasesKb",
    "MondoKb",
    "NcbiGeneKb",
    "NcbiTaxonomyKb",
    "DbsnpKb",
    "NameView",
    "nameview_registry",
    "ENTITIES_FEATURES",
    "HISTORY_FEATURES",
]
