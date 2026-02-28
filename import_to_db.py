# -*- coding: utf-8 -*-
"""将改良改造报表 CSV 导入 SQLite 数据库。"""
import os
import sys
from pathlib import Path

# 添加当前目录以便导入
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from data_loader import load_single_csv

DB_PATH = os.getenv("APP203_DB_PATH", "app203_projects.db")


def save_to_db(df: pd.DataFrame):
    """将 DataFrame 全量写入 SQLite。"""
    if df is None or df.empty:
        return False
    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("projects", conn, if_exists="replace", index=False)
    return True


def main():
    # 默认：先查项目目录，再查上级 text2sql 目录
    base = Path(__file__).resolve().parent
    candidates = [
        base / "改良改造报表-V4.csv",
        base.parent / "改良改造报表-V4.csv",
    ]
    csv_path = None
    for p in candidates:
        if p.exists():
            csv_path = p
            break
    if not csv_path:
        if len(sys.argv) > 1:
            csv_path = Path(sys.argv[1])
            if not csv_path.exists():
                print(f"错误：文件不存在 {csv_path}")
                sys.exit(1)
        else:
            print("错误：未找到 改良改造报表-V4.csv")
            print("用法: python import_to_db.py [CSV文件路径]")
            sys.exit(1)

    print(f"正在加载: {csv_path}")
    df = load_single_csv(str(csv_path))
    if df.empty:
        print("错误：加载后无数据")
        sys.exit(1)
    print(f"已加载 {len(df)} 条记录")
    save_to_db(df)
    print(f"已写入数据库: {DB_PATH}")
    print("完成。")


if __name__ == "__main__":
    main()
