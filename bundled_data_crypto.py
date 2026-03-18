# -*- coding: utf-8 -*-
"""默认数据加密/解密工具，避免仓库提交明文数据。"""
from __future__ import annotations

import base64
from pathlib import Path

# 仅用于“避免明文入库”，不用于高强度安全场景。
_DEFAULT_KEY = b"project-wizard-feishu-default-data-v1"


def _xor_cipher(data: bytes, key: bytes) -> bytes:
    key_len = len(key)
    return bytes(b ^ key[i % key_len] for i, b in enumerate(data))


def encrypt_file(plain_path: Path, enc_path: Path, key: bytes | None = None) -> None:
    """将明文文件加密为 .enc。"""
    key = key or _DEFAULT_KEY
    plain_path = Path(plain_path)
    enc_path = Path(enc_path)
    raw = plain_path.read_bytes()
    encrypted = _xor_cipher(raw, key)
    enc_path.write_bytes(base64.b64encode(encrypted))


def decrypt_file(enc_path: Path, key: bytes | None = None) -> bytes:
    """解密 .enc 文件并返回原始字节。"""
    key = key or _DEFAULT_KEY
    enc_path = Path(enc_path)
    encoded = enc_path.read_bytes()
    decoded = base64.b64decode(encoded)
    return _xor_cipher(decoded, key)


def load_decrypted_csv(enc_path: Path) -> str:
    """解密并返回 CSV 文本内容（用于 pd.read_csv(io.StringIO(...))）。"""
    raw = decrypt_file(enc_path)
    return raw.decode("utf-8-sig")
