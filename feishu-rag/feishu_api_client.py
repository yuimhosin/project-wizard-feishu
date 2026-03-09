# -*- coding: utf-8 -*-
"""
飞书 API 客户端：获取 tenant_access_token、拉取云文档内容
"""
import os
import time
import json
import urllib.request
import urllib.error
from typing import Optional

# 配置
FEISHU_API_BASE = "https://open.feishu.cn/open-apis"


def _get_config():
    from config import FEISHU_APP_ID, FEISHU_APP_SECRET
    return FEISHU_APP_ID, FEISHU_APP_SECRET


_token_cache = {"token": None, "expires_at": 0}


def get_tenant_access_token() -> Optional[str]:
    """获取 tenant_access_token，带缓存（2小时有效期，提前5分钟刷新）"""
    app_id, app_secret = _get_config()
    if not app_id or not app_secret:
        return None

    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 300:
        return _token_cache["token"]

    url = f"{FEISHU_API_BASE}/auth/v3/tenant_access_token/internal"
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                token = data.get("tenant_access_token")
                expire = data.get("expire", 7200)
                _token_cache["token"] = token
                _token_cache["expires_at"] = now + expire
                return token
    except Exception:
        pass
    return None


def _get_wiki_obj_token(node_token: str) -> Optional[str]:
    """通过 wiki 节点 token 获取对应的文档 obj_token"""
    token = get_tenant_access_token()
    if not token:
        return None
    url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node?token={node_token}"
    perm_token = os.getenv("FEISHU_PERMISSION_TOKEN", "")
    if perm_token:
        url += f"&permission_token={perm_token}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                node = data.get("data", {}).get("node", {})
                return node.get("obj_token")
    except urllib.error.HTTPError as e:
        if e.code == 400:
            # 可能是 space_id 而非 node_token，由调用方尝试 list_wiki_space_docs
            return None
        raise
    except Exception:
        pass
    return None


def list_wiki_space_docs(space_id: str) -> list:
    """
    获取知识库空间内所有文档节点。
    当 wiki 链接指向整个知识库（space_id）时使用。
    返回 [(node_token, obj_token, title), ...]
    """
    token = get_tenant_access_token()
    if not token:
        return []
    result = []
    page_token = None

    def _fetch_nodes(parent_token: str = None):
        nonlocal page_token
        url = f"{FEISHU_API_BASE}/wiki/v2/spaces/{space_id}/nodes"
        params = []
        if parent_token:
            params.append(f"parent_node_token={parent_token}")
        if page_token:
            params.append(f"page_token={page_token}")
        perm_token = os.getenv("FEISHU_PERMISSION_TOKEN", "")
        if perm_token:
            params.append(f"permission_token={perm_token}")
        if params:
            url += "?" + "&".join(params)
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") != 0:
                    return []
                d = data.get("data", {})
                items = d.get("items", [])
                page_token = d.get("page_token")
                return items
        except Exception:
            return []

    # 先获取根节点下的子节点
    nodes = _fetch_nodes()
    while nodes:
        for node in nodes:
            node_token = node.get("node_token", "")
            obj_token = node.get("obj_token", "")
            obj_type = node.get("obj_type", "")
            title = node.get("title", "")
            if obj_type in ("doc", "docx") and obj_token:
                result.append((node_token, obj_token, title))
            elif obj_type == "folder" and node_token:
                # 递归获取文件夹下的文档
                sub = _fetch_nodes(parent_token=node_token)
                nodes.extend(sub)
        if page_token:
            nodes = _fetch_nodes()
        else:
            break

    return result


def subscribe_drive_file(file_token: str, file_type: str = "bitable") -> bool:
    """
    订阅云文档事件，变更时飞书会推送到配置的 Request URL。
    file_type: doc|docx|sheet|bitable
    注意：需在飞书开放平台事件订阅中已添加 drive.file.bitable_record_changed_v1 等事件
    """
    token = get_tenant_access_token()
    if not token:
        return False
    url = f"{FEISHU_API_BASE}/drive/v1/files/{file_token}/subscribe?file_type={file_type}"
    req = urllib.request.Request(
        url,
        method="POST",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("code") == 0
    except Exception:
        return False


def get_bitable_raw_content(app_token: str, table_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    获取多维表格内容，转为纯文本。
    返回 (content, revision_id)，失败返回 (None, None)。revision_id 用空字符串表示。
    """
    token = get_tenant_access_token()
    if not token:
        return None, None
    if not table_id:
        return None, None

    all_texts = []
    page_token = None

    while True:
        params = ["page_size=500"]
        if page_token:
            params.append(f"page_token={page_token}")
        url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records?{'&'.join(params)}"

        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") != 0:
                    return None, None
                d = data.get("data", {})
                items = d.get("items", [])
                for rec in items:
                    fields = rec.get("fields", {})
                    parts = []
                    for k, v in fields.items():
                        if v is None:
                            continue
                        if isinstance(v, (list, dict)):
                            v = json.dumps(v, ensure_ascii=False)
                        parts.append(f"{k}: {v}")
                    if parts:
                        all_texts.append("\n".join(parts))
                page_token = d.get("page_token")
                if not d.get("has_more", False) or not page_token:
                    break
        except Exception:
            return None, None

    content = "\n\n---\n\n".join(all_texts) if all_texts else ""
    return content, ""


def _flatten_field_value(v) -> str:
    """将飞书字段值转为可分析文本"""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, dict):
                # 用户/人员: 取 name 或 text
                parts.append(item.get("name") or item.get("text") or str(item))
            else:
                parts.append(str(item))
        return "; ".join(parts) if parts else ""
    if isinstance(v, dict):
        return v.get("text") or v.get("name") or str(v)
    return str(v)


def get_bitable_records(app_token: str, table_id: str) -> list[dict]:
    """
    获取多维表格记录（结构化），用于统计分析。
    返回 [{"上报机构": "xx", "事件名称": "xx", ...}, ...]
    """
    token = get_tenant_access_token()
    if not token:
        return []
    if not table_id:
        return []

    all_records = []
    page_token = None

    while True:
        params = ["page_size=500"]
        if page_token:
            params.append(f"page_token={page_token}")
        url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records?{'&'.join(params)}"

        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") != 0:
                    return []
                d = data.get("data", {})
                items = d.get("items", [])
                for rec in items:
                    fields = rec.get("fields", {})
                    flat = {}
                    for k, v in fields.items():
                        if k in ("file_token", "tmp_url", "avatar_url"):
                            continue
                        flat[k] = _flatten_field_value(v)
                    all_records.append(flat)
                page_token = d.get("page_token")
                if not d.get("has_more", False) or not page_token:
                    break
        except Exception:
            return []

    return all_records


def _is_docx(doc_id: str) -> bool:
    """根据 ID 格式判断是否为 docx（docx 通常更长且格式不同）"""
    # doc_token 通常较短；document_id 为 docx 格式
    return len(doc_id) > 20 or "docx" in doc_id.lower()


def get_doc_raw_content(doc_id: str, source: str = "doc") -> tuple[Optional[str], Optional[str]]:
    """
    获取文档纯文本内容。
    返回 (content, revision_id)，失败返回 (None, None)
    """
    token = get_tenant_access_token()
    if not token:
        return None, None

    # wiki 节点需先获取 obj_token
    if source == "wiki":
        obj_token = _get_wiki_obj_token(doc_id)
        if not obj_token:
            return None, None
        doc_id = obj_token

    if _is_docx(doc_id):
        # 新版 docx API
        url = f"{FEISHU_API_BASE}/docx/v1/documents/{doc_id}/raw_content"
    else:
        # 旧版 doc API
        url = f"{FEISHU_API_BASE}/doc/v2/{doc_id}/raw_content"

    # 若配置了分享授权码（保密文档），尝试附加到请求
    perm_token = os.getenv("FEISHU_PERMISSION_TOKEN", "")
    if perm_token:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}permission_token={perm_token}"

    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                content = data.get("data", {}).get("content", "")
                revision_id = data.get("data", {}).get("revision_id") or data.get("data", {}).get("document_revision_id")
                return content or "", str(revision_id) if revision_id else ""
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None
        raise
    except Exception:
        return None, None


def get_doc_info(doc_id: str, source: str = "doc") -> Optional[dict]:
    """获取文档基本信息（含 revision_id），用于增量同步"""
    token = get_tenant_access_token()
    if not token:
        return None

    if source == "wiki":
        obj_token = _get_wiki_obj_token(doc_id)
        if not obj_token:
            return {"revision_id": "", "title": doc_id}
        token_auth = get_tenant_access_token()
        if not token_auth:
            return {"revision_id": "", "title": doc_id}
        url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node?token={doc_id}"
        req = urllib.request.Request(url, method="GET", headers={"Authorization": f"Bearer {token_auth}"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") == 0:
                    node = data.get("data", {}).get("node", {})
                    return {"revision_id": "", "title": node.get("title", doc_id)}
        except Exception:
            pass
        return {"revision_id": "", "title": doc_id}

    if _is_docx(doc_id):
        url = f"{FEISHU_API_BASE}/docx/v1/documents/{doc_id}"
    else:
        # 旧版 doc 无单独 info 接口，直接返回默认
        return {"revision_id": "", "title": doc_id}

    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                d = data.get("data", {})
                return {
                    "document_id": d.get("document_id"),
                    "revision_id": str(d.get("revision_id", "")),
                    "title": d.get("title", ""),
                }
    except Exception:
        pass
    return None
