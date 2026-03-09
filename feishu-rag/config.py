# -*- coding: utf-8 -*-
"""飞书 RAG 系统配置"""
import os
from pathlib import Path

# 加载 .env
try:
    from dotenv import load_dotenv
    _env = Path(__file__).resolve().parent / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

# Streamlit：将 secrets 注入为环境变量（补充 .env 中缺失或空的值）
def _inject_secrets_from_dict(d: dict):
    for k, v in d.items():
        if isinstance(v, str) and (k not in os.environ or not (os.environ.get(k) or "").strip()):
            os.environ[k] = v
        elif isinstance(v, dict):
            _inject_secrets_from_dict(v)  # 嵌套 section

try:
    import streamlit as st
    if hasattr(st, "secrets") and st.secrets:
        _inject_secrets_from_dict(dict(st.secrets))
except Exception:
    pass

# 本地运行：若 feishu-rag 无 .streamlit，尝试加载父目录的 secrets.toml
if not (os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENAI_API_KEY")):
    import re
    for _secrets_path in [
        Path(__file__).resolve().parent / ".streamlit" / "secrets.toml",
        Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml",
    ]:
        if _secrets_path.exists():
            try:
                text = _secrets_path.read_text(encoding="utf-8")
                for m in re.finditer(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]*)"', text, re.M):
                    k, v = m.group(1), m.group(2)
                    if k not in os.environ or not (os.environ.get(k) or "").strip():
                        os.environ[k] = v
            except Exception:
                pass
            break

# 飞书应用
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

# 文档分享授权码（可选）：当文档为保密/仅链接可访问时，可填写分享时获得的授权码 pt-xxx
FEISHU_PERMISSION_TOKEN = os.getenv("FEISHU_PERMISSION_TOKEN", "")

# 文档 ID 列表（逗号分隔，支持 doc_token、document_id、wiki、bitable 链接）
def _parse_doc_ids(raw: str) -> list:
    import re
    ids = []
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        # wiki 链接
        if "wiki/" in x or "feishu.cn/wiki" in x:
            m = re.search(r"wiki/([A-Za-z0-9]+)", x)
            if m:
                ids.append(("wiki", m.group(1)))
            else:
                ids.append(("doc", x))
        # 多维表格 base/ 链接
        elif "base/" in x or "feishu.cn/base" in x:
            m = re.search(r"base/([A-Za-z0-9]+)", x)
            table_m = re.search(r"[?&]table=([A-Za-z0-9]+)", x)
            if m:
                app_token = m.group(1)
                table_id = table_m.group(1) if table_m else ""
                ids.append(("bitable", (app_token, table_id)))
            else:
                ids.append(("doc", x))
        else:
            ids.append(("doc", x))
    return ids

FEISHU_DOC_IDS = _parse_doc_ids(os.getenv("FEISHU_DOC_IDS", ""))

# 向量库路径
FEISHU_RAG_ROOT = Path(__file__).resolve().parent
VECTOR_DB_PATH = os.getenv("FEISHU_VECTOR_DB_PATH", str(FEISHU_RAG_ROOT / "vector_db"))

# 同步间隔（秒）
SYNC_INTERVAL = int(os.getenv("FEISHU_SYNC_INTERVAL", "300"))

# 嵌入模型：使用本地 HuggingFace（BGE），无需 API
EMBEDDING_MODEL = os.getenv("FEISHU_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5")

# LLM 配置：DeepSeek（OpenAI 兼容）
LLM_API_KEY = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
LLM_API_BASE = os.getenv("DEEPSEEK_API_BASE") or os.getenv("OPENAI_BASE_URL") or "https://api.deepseek.com"
if LLM_API_BASE and not LLM_API_BASE.rstrip("/").endswith("/v1"):
    LLM_API_BASE = LLM_API_BASE.rstrip("/") + "/v1"
LLM_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# RAG 参数
CHUNK_SIZE = int(os.getenv("FEISHU_CHUNK_SIZE", "500"))
CHUNK_OVERLAP = int(os.getenv("FEISHU_CHUNK_OVERLAP", "50"))
TOP_K = int(os.getenv("FEISHU_RAG_TOP_K", "100"))  # 默认检索 100 个块
TOP_K_LIST = int(os.getenv("FEISHU_RAG_TOP_K_LIST", "100"))  # 查询所有/列表时用更大值
