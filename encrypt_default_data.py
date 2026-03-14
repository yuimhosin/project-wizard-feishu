# -*- coding: utf-8 -*-
"""将 改良改造报表-V4-sample.csv 加密为 .enc，供 git 提交。"""
from pathlib import Path

from bundled_data_crypto import encrypt_file

ROOT = Path(__file__).resolve().parent
PLAIN = ROOT / "改良改造报表-V4-sample.csv"
ENC = ROOT / "改良改造报表-V4-sample.csv.enc"

if __name__ == "__main__":
    if not PLAIN.exists():
        print(f"未找到 {PLAIN}，跳过加密。")
        exit(0)
    encrypt_file(PLAIN, ENC)
    print(f"已加密: {PLAIN} -> {ENC}")
