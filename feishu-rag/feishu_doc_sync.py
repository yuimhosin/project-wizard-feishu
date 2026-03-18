# -*- coding: utf-8 -*-
"""
飞书云文档同步：拉取文档内容，检测变更，触发 RAG 向量库更新
"""
import json
import hashlib
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from config import FEISHU_DOC_IDS, FEISHU_DOC_URLS, VECTOR_DB_PATH, SYNC_INTERVAL
from feishu_api_client import get_doc_raw_content, get_doc_info, list_wiki_space_docs, get_bitable_raw_content


META_FILE = Path(VECTOR_DB_PATH) / "doc_meta.json"


def _load_meta() -> dict:
    """加载文档元信息（doc_id -> {revision_id, content_hash}）"""
    if not META_FILE.exists():
        return {}
    try:
        with open(META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_meta(meta: dict):
    Path(VECTOR_DB_PATH).mkdir(parents=True, exist_ok=True)
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def sync_documents(on_update: Optional[Callable[[str, str, str], None]] = None) -> dict:
    """
    同步所有配置的文档。
    on_update(doc_id, content, title) 在文档有更新时被调用。
    返回统计：{"synced": n, "updated": n, "failed": n}
    """
    meta = _load_meta()
    stats = {"synced": 0, "updated": 0, "failed": 0}

    for item in FEISHU_DOC_IDS:
        if not item or len(item) != 2:
            continue
        source, doc_id = item
        stats["synced"] += 1

        if source == "bitable":
            app_token, table_id = doc_id if isinstance(doc_id, tuple) else ("", "")
            meta_key = f"bitable:{app_token}:{table_id}"
            content, revision = get_bitable_raw_content(app_token, table_id)
            if content is None:
                stats["failed"] += 1
                continue
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            old = meta.get(meta_key, {})
            if revision != old.get("revision_id", "") or content_hash != old.get("content_hash", ""):
                stats["updated"] += 1
                title = f"多维表格_{table_id[:12]}"
                meta[meta_key] = {"revision_id": revision, "content_hash": content_hash, "title": title}
                if on_update:
                    try:
                        on_update(meta_key, content, title)
                    except Exception:
                        pass
        elif source == "wiki":
            # 先尝试作为单节点获取
            content, revision = get_doc_raw_content(doc_id, source=source)
            if content is None:
                # 可能是知识库空间 ID，尝试列出所有文档
                docs = list_wiki_space_docs(doc_id)
                if not docs:
                    stats["failed"] += 1
                    continue
                for node_token, obj_token, title in docs:
                    meta_key = f"wiki:{doc_id}:{node_token}"
                    content, revision = get_doc_raw_content(obj_token, source="doc")
                    if content is None:
                        continue
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    old = meta.get(meta_key, {})
                    if revision != old.get("revision_id", "") or content_hash != old.get("content_hash", ""):
                        stats["updated"] += 1
                        meta[meta_key] = {"revision_id": revision, "content_hash": content_hash, "title": title}
                        if on_update:
                            try:
                                on_update(meta_key, content, title)
                            except Exception:
                                pass
            else:
                meta_key = f"{source}:{doc_id}"
                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                old = meta.get(meta_key, {})
                old_rev = old.get("revision_id", "")
                old_hash = old.get("content_hash", "")
                if revision != old_rev or content_hash != old_hash:
                    stats["updated"] += 1
                    info = get_doc_info(doc_id, source=source)
                    title = info.get("title", doc_id) if isinstance(info, dict) else doc_id
                    meta[meta_key] = {"revision_id": revision, "content_hash": content_hash, "title": title}
                    if on_update:
                        try:
                            on_update(meta_key, content, title)
                        except Exception:
                            pass
        else:
            meta_key = f"{source}:{doc_id}"
            content, revision = get_doc_raw_content(doc_id, source=source)
            if content is None:
                stats["failed"] += 1
                continue

            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            old = meta.get(meta_key, {})
            old_rev = old.get("revision_id", "")
            old_hash = old.get("content_hash", "")

            if revision != old_rev or content_hash != old_hash:
                stats["updated"] += 1
                info = get_doc_info(doc_id, source=source)
                title = info.get("title", doc_id) if isinstance(info, dict) else doc_id
                meta[meta_key] = {"revision_id": revision, "content_hash": content_hash, "title": title}
                if on_update:
                    try:
                        on_update(meta_key, content, title)
                    except Exception:
                        pass

    _save_meta(meta)
    return stats


def get_doc_list_with_urls() -> list:
    """
    返回 [(title, url), ...]，用于「表格清单」等统一入口。
    从 doc_meta 取标题，从 FEISHU_DOC_URLS 取链接。
    """
    meta = _load_meta()
    result = []
    for i, item in enumerate(FEISHU_DOC_IDS):
        if not item or len(item) != 2:
            continue
        source, doc_id = item
        url = FEISHU_DOC_URLS[i] if i < len(FEISHU_DOC_URLS) else ""
        title = ""
        if source == "bitable":
            app_token, table_id = doc_id if isinstance(doc_id, tuple) else ("", "")
            meta_key = f"bitable:{app_token}:{table_id}"
            title = (meta.get(meta_key) or {}).get("title", f"多维表格_{(table_id or app_token)[:12]}")
        elif source == "wiki":
            meta_key = f"wiki:{doc_id}"
            if meta_key in meta:
                title = meta[meta_key].get("title", "知识库")
            else:
                for k, v in meta.items():
                    if k.startswith(f"wiki:{doc_id}:"):
                        title = v.get("title", "知识库")
                        break
                if not title:
                    title = "知识库"
        else:
            meta_key = f"{source}:{doc_id}"
            title = (meta.get(meta_key) or {}).get("title", "文档")
        result.append((title, url))
    return result


def run_sync_loop(on_update: Optional[Callable[[str, str, str], None]] = None, interval: int = None):
    """后台线程：定期同步文档"""
    iv = interval or SYNC_INTERVAL

    def _loop():
        while True:
            try:
                sync_documents(on_update=on_update)
            except Exception:
                pass
            time.sleep(iv)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t
