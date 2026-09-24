import pytest

from belb import load_pair
from belb.config import BelbConfig
from belb.qaqc import qaqc_ids

# Programmatic exclusions with reasons

SKIP = {
    # "ncbi-disease.ctd-diseases": "KB has no versioning system and cannot be redistributed",
    # "bc5cdr-disease.ctd-diseases": "KB has no versioning system and cannot be redistributed",
    # "bc5cdr-chemical.ctd-chemicals": "KB has no versioning system and cannot be redistributed",
    # "nlm-chem.ctd-chemicals": "KB has no versioning system and cannot be redistributed",
    "medmentions-st21pv.umls": "Tested with full corpus",
    "gnormplus.ncbi-gene": "Tested with KB subset (faster)",
    "nlm-gene.ncbi-gene": "Tested with KB subset (faster)",
}


def pytest_generate_tests(metafunc):
    if "pairing_name" in metafunc.fixturenames:
        from belb.config import load_config

        config_dir = metafunc.config.getoption("--config-dir")
        default = metafunc.config.getoption("--default")
        config = load_config(default=default, config_dir=config_dir)
        params = []
        for pairing in config.pairs:
            marks = []
            if pairing.name in SKIP:
                marks.append(pytest.mark.skip(reason=SKIP[pairing.name]))
            params.append(pytest.param(pairing.name, id=pairing.name, marks=marks))
        metafunc.parametrize("pairing_name", params)


def test_corpus_ids_match_kb(benchmark_config: BelbConfig, pairing_name: str):
    benchmark_config.task.inkb = True

    pair = load_pair(name=pairing_name, config=benchmark_config)

    missing_ids = qaqc_ids(pair.corpus, pair.kb)

    assert not missing_ids, (
        f"Found {len(missing_ids)} IDs in {pairing_name} "
        f"not present in KB. "
        f"Sample: {list(missing_ids)[:10]}"
    )
