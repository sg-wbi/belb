from dataclasses import dataclass, field

import pytest

from belb.config import BelbConfig
from belb.corpus import LOADERS, load_corpus
from belb.qaqc import qaqc_offsets

# Programmatic exclusions with reasons
EXCLUSIONS = {
    "snp": "Corpus does not provide text",
}


@dataclass(frozen=True)
class CorpusTestConfig:
    name: str
    corpus_kwargs: dict[str, str] = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Generates a clean pytest ID."""
        if not self.corpus_kwargs:
            return self.name
        suffix = "-".join(f"{k}={v}" for k, v in self.corpus_kwargs.items())
        return f"{self.name}-{suffix}"


def pytest_generate_tests(metafunc):
    if "tc" in metafunc.fixturenames:
        from belb.config import load_config

        config_dir = metafunc.config.getoption("--config-dir")
        default = metafunc.config.getoption("--default")
        config = load_config(default=default, config_dir=config_dir)

        available_corpora = set(config.corpora) & set(LOADERS)
        params = []
        for name in available_corpora:
            tc = CorpusTestConfig(name=name)
            marks = []
            if name in EXCLUSIONS:
                marks.append(pytest.mark.skip(reason=EXCLUSIONS[name]))
            params.append(pytest.param(tc, id=tc.id, marks=marks))
        metafunc.parametrize("tc", params)


def test_corpus_offsets(benchmark_config: BelbConfig, tc: CorpusTestConfig):

    # keep all annotations
    benchmark_config.task.inkb = False

    # Load the corpus with generic custom attributes
    corpus = load_corpus(
        name=tc.name, config=benchmark_config, corpus_kwargs=tc.corpus_kwargs
    )

    errors_df = qaqc_offsets(corpus)

    # Assert
    assert errors_df.empty, (
        f"Offset mismatches in {tc.name} (kwargs={tc.corpus_kwargs}):\n"
        f"{errors_df.to_dict('records')}"
    )
