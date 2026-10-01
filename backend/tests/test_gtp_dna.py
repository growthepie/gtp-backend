from tests import setup_test_imports

setup_test_imports()

from src.gtp_dna import get_gtp_dna_dispatch_token, get_gtp_dna_token


def test_get_gtp_dna_token_prefers_generic_github_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "generic-token")
    monkeypatch.setenv("GITHUB_GROWTHEPAI_TOKEN", "bot-token")

    assert get_gtp_dna_token() == "generic-token"


def test_get_gtp_dna_dispatch_token_prefers_growthepai_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "generic-token")
    monkeypatch.setenv("GITHUB_GROWTHEPAI_TOKEN", "bot-token")

    assert get_gtp_dna_dispatch_token() == "bot-token"


def test_get_gtp_dna_dispatch_token_falls_back_to_generic_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "generic-token")
    monkeypatch.delenv("GITHUB_GROWTHEPAI_TOKEN", raising=False)

    assert get_gtp_dna_dispatch_token() == "generic-token"
