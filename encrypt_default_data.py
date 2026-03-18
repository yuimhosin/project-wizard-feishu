# -*- coding: utf-8 -*-
"""将默认数据 CSV 加密为 .enc，供仓库提交。"""
from pathlib import Path

from bundled_data_crypto import encrypt_file

ROOT = Path(__file__).resolve().parent
PLAIN = ROOT / "改良改造报表-V4.csv"
ENC_MAIN = ROOT / "改良改造报表-V4.csv.enc"
ENC_SAMPLE = ROOT / "改良改造报表-V4-sample.csv.enc"


if __name__ == "__main__":
    if not PLAIN.exists():
        print(f"未找到明文文件：{PLAIN}")
        raise SystemExit(1)
    encrypt_file(PLAIN, ENC_MAIN)
    encrypt_file(PLAIN, ENC_SAMPLE)
    print(f"已加密：{PLAIN.name} -> {ENC_MAIN.name}")
    print(f"已加密：{PLAIN.name} -> {ENC_SAMPLE.name}")
