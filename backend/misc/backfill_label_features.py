"""
Backfill script — re-enrich + re-classify a fixed address list and write
results to contract_label_features. Does NOT attest anything to OLI.

Usage:
    python backend/misc/backfill_label_features.py

The address list is the batch attested on 2026-05-11 that missed the
contract_label_features write due to the public.contract_label_features bug.
"""
import sys, os, asyncio, json, types
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
_BACKEND = Path(__file__).parent.parent
sys.path.insert(0, str(_BACKEND))
os.chdir(_BACKEND)

from dotenv import load_dotenv
load_dotenv(dotenv_path=_BACKEND / ".env", override=False)
load_dotenv(dotenv_path=_BACKEND.parent / ".env", override=False)

import logging
# Force the AutoLabeler logger to stdout before the module is imported,
# so basicConfig's no-op (root already has handlers from alembic) doesn't swallow output.
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
_al_logger = logging.getLogger("AutoLabeler")
_al_logger.addHandler(_handler)
_al_logger.setLevel(logging.INFO)
_al_logger.propagate = False

from labeling.automated_labeler import run_pipeline

# ── addresses attested 2026-05-11 (automated labeler, missed features write) ──
CONTRACTS = [
    {"address": "0x233c5370ccfb3cd7409d9a3fb98ab94de94cb4cd", "origin_key": "scroll"},
    {"address": "0x00000000000fb5c9adea0298d729a0cb3823cc07", "origin_key": "polygon_pos"},
    {"address": "0x2a6c43e0dbdcde23d40c82f45682bc6d8a6db219", "origin_key": "scroll"},
    {"address": "0x6a5b3ab3274b738eab25205af6e2d4dd77812924", "origin_key": "scroll"},
    {"address": "0x7f4434e1ab330d8565ca689e9774bf5d82db65df", "origin_key": "scroll"},
    {"address": "0x66a2edaa2b12a23957b1e0537d1fc1d85762597d", "origin_key": "scroll"},
    {"address": "0x84ba896235059fe27727eaa2695a9f99220d9a7e", "origin_key": "polygon_pos"},
    {"address": "0x0533ecf7179c2ee47c2122273d389614564a62a1", "origin_key": "optimism"},
    {"address": "0x606f31ae97ac61be5938e418c7b33869a2dbbaef", "origin_key": "optimism"},
    {"address": "0x646d30f447c926b23dfd8adc87eb152f640aec03", "origin_key": "polygon_pos"},
    {"address": "0x800ca870416cdfef77991036b8e1f2e51623996e", "origin_key": "scroll"},
    {"address": "0x7818b23ba4dc500fdafef8e545372da4f32f8d7b", "origin_key": "optimism"},
    {"address": "0x8932fe7726c1ee743f662f485c3e5a5d1d595f71", "origin_key": "polygon_pos"},
    {"address": "0x43208f448de982a2d8a2df8f8e78574b98f2aa74", "origin_key": "polygon_pos"},
    {"address": "0x709944a48caf83535e43471680fda4905fb3920a", "origin_key": "scroll"},
    {"address": "0x834484f59983ccb3f2635d88f4e7ed378814e798", "origin_key": "optimism"},
    {"address": "0x88926382559e24d1153d9f492554064c9f052a22", "origin_key": "scroll"},
    {"address": "0xada100874d00e3331d00f2007a9c336a65009718", "origin_key": "polygon_pos"},
    {"address": "0xc5fd68d2b8c79679f8a88f1896d47cc2aaa6726f", "origin_key": "optimism"},
    {"address": "0xc0603cbfc330e988734cfffd107709a5408cd98c", "origin_key": "scroll"},
    {"address": "0xe4303a411691088023e403ebaefdb01ec7e1d0f9", "origin_key": "polygon_pos"},
    {"address": "0x1b5ab7c503c2b1d94e7c42b212b4f944f7c77fce", "origin_key": "megaeth"},
    {"address": "0x1bf6ef01addb0181634370314ac6ee843d4a1c5e", "origin_key": "megaeth"},
    {"address": "0x06a645079cd4f3bb38ffad47f92180b8041145e3", "origin_key": "ethereum"},
    {"address": "0x6e43f31b2c160a3672c681114696667ef219d4c3", "origin_key": "megaeth"},
    {"address": "0x7aa4ac1399e2af80fb19c6d383e50232ccb57ad1", "origin_key": "ethereum"},
    {"address": "0x07e04e47ca503eb97665d10a2a8e76c2681a02ad", "origin_key": "megaeth"},
    {"address": "0x9c65d15a671d814ef7be25418fd46139e7366c07", "origin_key": "ethereum"},
    {"address": "0x9ed578831ddf3246731ef74e992163631cd010b0", "origin_key": "ethereum"},
    {"address": "0x183afbbca127cbef02426abed18d983c85dce0ab", "origin_key": "megaeth"},
    {"address": "0x829f4b62eebe12af653b4dd4ffc480966f7d7f09", "origin_key": "ethereum"},
    {"address": "0x955a4addc17114c36726c12af9c73e23e497c2bd", "origin_key": "megaeth"},
    {"address": "0x3436cf21128b3f94c7b06e14f064baf9654eb7c6", "origin_key": "ethereum"},
    {"address": "0x726618909158fd57a6148fe10c1823e72e55de05", "origin_key": "optimism"},
    {"address": "0xb92fe925dc43a0ecde6c8b1a2709c170ec4fff4f", "origin_key": "ethereum"},
    {"address": "0xc2f34f8849a8607fd73e06d6849bda07c2b7de38", "origin_key": "megaeth"},
    {"address": "0xdc347a0e1a8d214dea7ac7bc76c01de7a93cfd6f", "origin_key": "optimism"},
    {"address": "0xf9a8eadfe12a90cb375eee524f03d66ecace5daa", "origin_key": "optimism"},
    {"address": "0xf9f676066eb7baeeed93e859bc26a41663f277a8", "origin_key": "megaeth"},
    {"address": "0xf827725498e6fcf62d331566965f5254bcda081f", "origin_key": "ethereum"},
    {"address": "0xfe7a22b80216e366e17f2c8a4ec29382272fbb4b", "origin_key": "celo"},
    {"address": "0x3a10593bbe34ab4841e4350eececd616717e1707", "origin_key": "base"},
    {"address": "0x49b5a631f54927c0007232844f06fe18cbf69786", "origin_key": "ethereum"},
    {"address": "0x59258be246e366a16dd580694ae225c41a5a976b", "origin_key": "base"},
    {"address": "0x88818ac1b667914e502e873fccb29bbbd1a40164", "origin_key": "base"},
    {"address": "0x0474941aad87433a55eec78109b41c6aa08f8f2c", "origin_key": "base"},
    {"address": "0x520961c720a5e35d2ee0659d8a4681727f34987a", "origin_key": "base"},
    {"address": "0xc2e097a4aea85c7f1ceee0161a56bbdd1ca3c840", "origin_key": "celo"},
    {"address": "0xc2068e03ca948f54348899eeda1417a901d76285", "origin_key": "celo"},
    {"address": "0xc6992dbd4f87402743bc4e2e8b28c370115f0deb", "origin_key": "base"},
    {"address": "0xe4c343161f6bc95fd9d54bec21c0f52f10335210", "origin_key": "base"},
    {"address": "0x1bd3b99200cc2dfa459a8e16d38a56e75879a97d", "origin_key": "arbitrum"},
    {"address": "0x3db351db37ed8357a7626d39d6d1e6768881b05a", "origin_key": "arbitrum"},
    {"address": "0x20c6c627eb775a169981060806e98cc962ce8803", "origin_key": "arbitrum"},
    {"address": "0x71e688f6cc7aad7a00cc9579eaf1a870a0c5b155", "origin_key": "arbitrum"},
    {"address": "0x78b69899c8cd252126cbb1a50171ec37286c3877", "origin_key": "arbitrum"},
    {"address": "0x00134515341014a3bb6b33fe8f8d67f30b92d316", "origin_key": "arbitrum"},
    {"address": "0xa57d8e6646e063ffd6eae579d4f327b689da5dc3", "origin_key": "base"},
    {"address": "0xb7c05d62cae59479bcf1facaa0bdf7245454e4bd", "origin_key": "arbitrum"},
    {"address": "0xe855d4921ba6d09a29b5924325c402c51b486155", "origin_key": "arbitrum"},
    {"address": "0xeb8396b3053b75f9fbf7dd90d758d05195c3da75", "origin_key": "base"},
    {"address": "0xe8d294f3fff2a5cb34d15ecdef34a53b01f5a462", "origin_key": "arbitrum"},
    {"address": "0x0000efc4ec03a7c47d3a38a9be7ff1d52dd01b99", "origin_key": "base"},
    {"address": "0xbf7fe109f674ab1341d69e67a262e50ce87eb7c1", "origin_key": "ethereum"},
    {"address": "0x07720554047a0c30b29a09b23db59e5ad2f328af", "origin_key": "megaeth"},
]

# ── write input JSON for run_pipeline ─────────────────────────────────────────
_INPUT_JSON = "/tmp/backfill_label_features_input.json"

def _build_args():
    args = types.SimpleNamespace()
    args.input_json       = _INPUT_JSON
    args.attest           = False          # no OLI attestation
    args.concurrency      = 5
    args.output_dir       = "/tmp/backfill_label_features_output"
    args.days             = 7
    args.chains           = None
    args.max_contracts    = None
    args.min_txcount      = 0
    args.use_v2_fetch     = True
    args.per_chain_limit  = 0
    args.confidence_threshold = 0.3
    return args


if __name__ == "__main__":
    Path("/tmp/backfill_label_features_output").mkdir(exist_ok=True)
    with open(_INPUT_JSON, "w") as f:
        json.dump(CONTRACTS, f)
    print(f"Input written: {len(CONTRACTS)} contracts → {_INPUT_JSON}", flush=True)
    try:
        asyncio.run(run_pipeline(_build_args()))
    except Exception as e:
        import traceback
        print(f"ERROR: {e}")
        traceback.print_exc()
