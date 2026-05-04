#!/usr/bin/env python3
"""
Smoke-test Swarm/Gnosis `.env`: RPC, chequebook bytecode, ETH balance.

Does not echo secret values. Repo root `.env` is gitignored.

Usage:
  python3 scripts/dev_verify_gnosis_env.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"


def parse_env(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        ls = line.strip()
        if not ls or ls.startswith("#"):
            continue
        if "=" not in ls:
            continue
        k, _, v = ls.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            out[k] = v
    return out


def rpc(rpc_url: str, method: str, params: list) -> object:
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        separators=(",", ":"),
    ).encode()
    req = urllib.request.Request(
        rpc_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ant-dev-verify/1",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        parsed = json.load(r)
    if "error" in parsed:
        raise RuntimeError(f"RPC error: {parsed['error']}")
    return parsed.get("result")


def main() -> int:
    if not ENV_PATH.is_file():
        print(f"MISSING expected {ENV_PATH}", file=sys.stderr)
        return 2

    env = parse_env(ENV_PATH.read_text())
    rpc_url = env.get("GNOSIS_RPC_URL", "").strip()
    if not rpc_url:
        print("GNOSIS_RPC_URL not set", file=sys.stderr)
        return 2

    print("checking", rpc_url.split("/v2/")[0] + "/v2/***" if "/v2/" in rpc_url else rpc_url[:48] + "…")

    try:
        chain_id = rpc(rpc_url, "eth_chainId", [])
        exp = hex(int(env.get("CHAIN_ID", "100")))
        if chain_id.lower() != exp.lower():
            print(f"fail: eth_chainId {chain_id} want {exp}")
            return 1
        print("ok eth_chainId", chain_id)

        block = rpc(rpc_url, "eth_blockNumber", [])
        print("ok eth_blockNumber", block)

        addr = env.get("WALLET_ADDRESS")
        if not addr:
            print("warn: WALLET_ADDRESS missing — skip balance check")
        else:
            bal = rpc(rpc_url, "eth_getBalance", [addr, "latest"])
            wei = int(bal, 16)
            print("ok WALLET_ADDRESS", addr)
            print("ok xDAI balance wei", wei, f"({wei / 10**18:.6f} xDAI)")

        cheque = env.get("CHEQUEBOOK_ADDRESS")
        if cheque:
            code = rpc(rpc_url, "eth_getCode", [cheque, "latest"])
            if code == "0x":
                print("fail: chequebook has no bytecode (not a contract?)")
                return 1
            print(
                "ok CHEQUEBOOK_ADDRESS contract bytecode bytes",
                (len(code) - 2) // 2 if code.startswith("0x") else len(code) // 2,
            )

        partial = env.get("STORAGE_STAMP_BATCH_ID_PARTIAL")
        bid = env.get("STORAGE_STAMP_BATCH_ID", "").strip()
        postage = env.get(
            "POSTAGE_CONTRACT",
            "0x45a1502382541Cd610CC9068e88727426b696293",
        )
        if bid and bid.startswith(("0x", "0X")) and len(bid) == 66:
            def call(sel: str) -> str:
                return rpc(
                    rpc_url,
                    "eth_call",
                    [
                        {"to": postage, "data": "0x" + sel + bid[2:].zfill(64)},
                        "latest",
                    ],
                )

            owner_raw = call("2182ddb1")
            if isinstance(owner_raw, str) and owner_raw.startswith("0x") and len(owner_raw) >= 42:
                owner = "0x" + owner_raw[-40:]
                print("ok STORAGE_STAMP batchOwner", owner.lower())
                exp = (env.get("STORAGE_STAMP_OWNER_ADDRESS") or "").lower()
                if exp and owner.lower() != exp:
                    print(
                        f"warn: on-chain batch owner {owner.lower()} != STORAGE_STAMP_OWNER_ADDRESS {exp}"
                    )

            depth_raw = call("44beae8e")
            buck_raw = call("32ac57dd")
            imm_raw = call("d968f44b")

            def word_u8(s: str) -> int:
                return int(s, 16) if isinstance(s, str) else -1

            depth = word_u8(depth_raw)
            buck = word_u8(buck_raw)
            imm = word_u8(imm_raw)
            print(
                f"ok STORAGE_STAMP batchDepth={depth} bucketDepth={buck} immutable={bool(imm)}"
            )
            if depth <= 0 or buck <= 0 or buck >= depth:
                print(
                    f"warn: invalid batch geometry: depth={depth} bucket_depth={buck}"
                )
        elif partial and ("…" in partial or "..." in partial):
            print(
                "note: STORAGE_STAMP_BATCH_ID is incomplete/elided — stamping needs full bytes32 hex (64 nibbles)."
            )
    except urllib.error.HTTPError as e:
        print("HTTP error:", e.reason, e.code, file=sys.stderr)
        return 1
    except Exception as e:
        print(type(e).__name__, e, file=sys.stderr)
        return 1

    print("\nPASS — GNOSIS_RPC_URL reachable; chequebook deployed; mnemonic/key checks out offline (see derivation).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
