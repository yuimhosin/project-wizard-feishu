# -*- coding: utf-8 -*-
"""内嵌默认数据的加密/解密，避免明文存储在 git 中。"""
import base64
from pathlib import Path

# 固定密钥（仅用于防止仓库中明文暴露，非高安全场景）
_DEFAULT_KEY = b"app203-bundled-sample-v1"


def _xor_cipher(data: bytes, key: bytes) -> bytes:
    """简单 XOR 加解密。"""
    key_len = len(key)
    return bytes(b ^ key[i % key_len] for i, b in enumerate(data))


def encrypt_file(plain_path: Path, enc_path: Path, key: bytes = None) -> None:
    """将明文文件加密为 .enc 文件。"""
    key = key or _DEFAULT_KEY
    plain_path = Path(plain_path)
    enc_path = Path(enc_path)
    data = plain_path.read_bytes()
    encrypted = _xor_cipher(data, key)
    enc_path.write_bytes(base64.b64encode(encrypted))


def decrypt_file(enc_path: Path, key: bytes = None) -> bytes:
    """从 .enc 文件解密得到原始字节。"""
    key = key or _DEFAULT_KEY
    enc_path = Path(enc_path)
    data = enc_path.read_bytes()
    decoded = base64.b64decode(data)
    return _xor_cipher(decoded, key)


def load_decrypted_csv(enc_path: Path) -> str:
    """解密并返回 CSV 文本内容（用于 pd.read_csv(io.StringIO(...))）。"""
    raw = decrypt_file(enc_path)
    return raw.decode("utf-8-sig")
