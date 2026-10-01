"""Access the gtp-dna repository through the GitHub API."""

import io
import os
import zipfile

import requests
from dotenv import load_dotenv


REPO_API_URL = "https://api.github.com/repos/growthepie/gtp-dna"
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


def get_gtp_dna_token():
    return os.getenv("GITHUB_TOKEN") or os.getenv("GITHUB_GROWTHEPAI_TOKEN")


def get_gtp_dna_dispatch_token():
    return os.getenv("GITHUB_GROWTHEPAI_TOKEN") or os.getenv("GITHUB_TOKEN")


def _get(url, accept, timeout):
    headers = {"Accept": accept, "X-GitHub-Api-Version": "2022-11-28"}
    token = get_gtp_dna_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code in (401, 403, 404):
        raise RuntimeError(
            "Cannot access growthepie/gtp-dna. Set GITHUB_GROWTHEPAI_TOKEN "
            "or GITHUB_TOKEN to a GitHub token with Contents read access to the repository."
        )
    response.raise_for_status()
    return response


def get_gtp_dna_file(path, ref="main"):
    response = _get(
        f"{REPO_API_URL}/contents/{path}?ref={ref}",
        "application/vnd.github.raw+json",
        timeout=30,
    )
    return response.content.decode("utf-8")


def get_gtp_dna_archive(ref="main"):
    response = _get(
        f"{REPO_API_URL}/zipball/{ref}",
        "application/vnd.github+json",
        timeout=60,
    )
    return zipfile.ZipFile(io.BytesIO(response.content))
