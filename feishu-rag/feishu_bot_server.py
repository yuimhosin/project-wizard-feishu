# -*- coding: utf-8 -*-
"""
飞书机器人服务：接收消息事件，调用 RAG 回答，并回复用户
需配置企业自建应用 + 事件订阅
"""
import json
import hmac
import hashlib
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from config import FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_DOC_IDS
from feishu_api_client import get_tenant_access_token, subscribe_drive_file
from feishu_doc_sync import sync_documents, run_sync_loop
from rag_engine import RAGEngine


# 飞书验证 token（事件订阅配置时填写）
FEISHU_VERIFY_TOKEN = os.getenv("FEISHU_VERIFY_TOKEN", "")
FEISHU_ENCRYPT_KEY = os.getenv("FEISHU_ENCRYPT_KEY", "")

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"


def _decrypt_event(encrypt: str) -> dict:
    """解密飞书事件（若开启了加密）"""
    if not FEISHU_ENCRYPT_KEY or not encrypt:
        return {}
    try:
        import base64
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend
        key = hashlib.sha256(FEISHU_ENCRYPT_KEY.encode()).digest()
        raw = base64.b64decode(encrypt)
        iv = raw[:16]
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded = decryptor.update(raw[16:]) + decryptor.finalize()
        pad = padded[-1]
        if isinstance(pad, str):
            pad = ord(pad)
        data = padded[:-pad].decode("utf-8")
        return json.loads(data)
    except Exception:
        return {}


def _send_message(chat_id: str, msg_type: str, content: dict, reply_to_msg_id: str = None) -> bool:
    """发送消息到飞书。有 reply_to_msg_id 时使用回复接口，否则使用发送接口"""
    token = get_tenant_access_token()
    if not token:
        return False
    import urllib.request

    if reply_to_msg_id:
        # 回复消息：POST /im/v1/messages/{message_id}/reply
        url = f"{FEISHU_API_BASE}/im/v1/messages/{reply_to_msg_id}/reply"
        body = {"msg_type": msg_type, "content": json.dumps(content)}
    else:
        # 发送新消息
        url = f"{FEISHU_API_BASE}/im/v1/messages"
        params = "?receive_id_type=chat_id" if chat_id.startswith("oc_") else "?receive_id_type=user_id"
        url += params
        body = {"receive_id": chat_id, "msg_type": msg_type, "content": json.dumps(content)}

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


def _extract_question(text: str, bot_name: str = "") -> str:
    """从 @机器人 消息中提取问题文本"""
    # 移除 @机器人 等 at 标签，格式如 <at user_id="xxx">名字</at>
    import re
    text = re.sub(r"<at[^>]*>.*?</at>", "", text, flags=re.DOTALL)
    return text.strip()


def _get_rag() -> RAGEngine:
    return RAGEngine.get_cached()


def _on_doc_update(doc_id: str, content: str, title: str):
    """文档更新时，清洗后更新向量库"""
    from clean_timestamps import clean_content
    _get_rag().add_document(doc_id, clean_content(content), title)


class FeishuEventHandler(BaseHTTPRequestHandler):
    """处理飞书事件回调"""

    def do_GET(self):
        """URL 校验：飞书可能发 GET 带 ?challenge=xxx，或先探测"""
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        challenge = qs.get("challenge", [""])[0]
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        if challenge:
            self.wfile.write(json.dumps({"challenge": challenge}).encode("utf-8"))
        else:
            self.wfile.write(json.dumps({"msg": "ok"}).encode("utf-8"))

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/feishu/event" and parsed.path != "/":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        raw = json.loads(body.decode("utf-8")) if body else {}
        print(f"[FEISHU] POST /feishu/event body_len={len(body)} keys={list(raw.keys())} type={raw.get('type')}", flush=True)

        # 1. 校验（若配置了）
        if FEISHU_VERIFY_TOKEN and raw.get("token") != FEISHU_VERIFY_TOKEN:
            self.send_response(403)
            self.end_headers()
            return

        # 2. 解密（若加密）
        if raw.get("encrypt"):
            if not FEISHU_ENCRYPT_KEY:
                print("[FEISHU] 收到加密请求，但未配置 FEISHU_ENCRYPT_KEY。请在飞书开放平台复制「Encrypt Key」到 .env", flush=True)
            decrypted = _decrypt_event(raw["encrypt"])
            if decrypted:
                raw = decrypted
            else:
                print("[FEISHU] 解密失败，请检查 FEISHU_ENCRYPT_KEY 是否与飞书控制台一致", flush=True)

        # 3. 立即返回 200，url_verification 必须返回 challenge
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        if raw.get("type") == "url_verification":
            challenge = raw.get("challenge", "")
            self.wfile.write(json.dumps({"challenge": challenge}).encode("utf-8"))
            print(f"[FEISHU] url_verification 已返回 challenge (len={len(challenge)})", flush=True)
        else:
            self.wfile.write(b"{}")

        # 4. 异步处理事件
        def _handle():
            try:
                typ = raw.get("type")
                evt = raw.get("header", {}).get("event_type") or raw.get("event", {}).get("type")
                print(f"[FEISHU] POST received type={typ} event_type={evt} keys={list(raw.keys())}", flush=True)
                if typ != "url_verification" and evt and "im.message" not in str(evt):
                    print(f"[FEISHU] 非消息事件，完整 header/event 前 500 字符: {str(raw)[:500]}", flush=True)
                _handle_event(raw)
            except Exception as e:
                import traceback
                print(f"[FEISHU] error: {e}\n{traceback.format_exc()}")

        threading.Thread(target=_handle, daemon=True).start()

    def log_message(self, format, *args):
        if os.getenv("FEISHU_DEBUG"):
            super().log_message(format, *args)


def _handle_drive_event(raw: dict):
    """处理云文档变更事件：触发 RAG 同步"""
    event_type = raw.get("header", {}).get("event_type") or raw.get("event", {}).get("type") or ""
    if "drive.file.bitable_record_changed" in event_type or "drive.file.edit" in event_type:
        print(f"[FEISHU] 收到文档变更事件 {event_type}，触发同步", flush=True)
        try:
            sync_documents(on_update=_on_doc_update)
            print("[FEISHU] 文档同步完成", flush=True)
        except Exception as e:
            import traceback
            print(f"[FEISHU] 文档同步失败: {e}\n{traceback.format_exc()}", flush=True)


def _handle_event(raw: dict):
    """处理事件：接收消息 -> RAG 回答；云文档变更 -> 触发同步"""
    typ = raw.get("type")
    if typ == "url_verification":
        return

    # 支持 schema 1.0/2.0，以及 v1/v2 事件
    event_type = (
        raw.get("header", {}).get("event_type")
        or raw.get("event", {}).get("type")
        or raw.get("event", {}).get("event", {}).get("type")
        or ""
    )

    # 云文档变更：多维表格/文档编辑 -> 实时同步
    if "drive.file" in event_type:
        _handle_drive_event(raw)
        return

    if "im.message.receive" not in event_type:
        print(f"[FEISHU] skip event_type={event_type}")
        return

    event = raw.get("event", {})
    msg = event.get("message", {})
    msg_type = msg.get("message_type")
    if msg_type != "text":
        return

    content_raw = msg.get("content", "{}")
    try:
        content = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
    except Exception:
        content = {}
    text = content.get("text", "").strip()
    if not text:
        print(f"[FEISHU] empty text, content={str(content_raw)[:200]}")
        return

    question = _extract_question(text)
    if not question or len(question) < 2:
        print(f"[FEISHU] question too short: '{question}'")
        return

    chat_id = msg.get("chat_id")
    message_id = msg.get("message_id")
    if not chat_id:
        print("[FEISHU] no chat_id in message")
        return

    print(f"[FEISHU] processing question='{question[:50]}...'")
    if os.getenv("FEISHU_AGENTIC_RAG"):
        from agentic_rag import query as agentic_query
        answer = agentic_query(question)
    else:
        answer = _get_rag().query(question)
    ok = _send_message(chat_id, "text", {"text": answer}, reply_to_msg_id=message_id)
    print(f"[FEISHU] reply sent ok={ok}")


def _subscribe_drive_files():
    """订阅配置的云文档事件，变更时飞书推送触发实时同步"""
    if not os.getenv("FEISHU_DRIVE_SUBSCRIBE", "1").strip() in ("1", "true", "yes"):
        return
    for item in FEISHU_DOC_IDS:
        if not item or len(item) != 2:
            continue
        source, doc_id = item
        if source == "bitable":
            app_token, _ = doc_id if isinstance(doc_id, tuple) else ("", "")
            if app_token and subscribe_drive_file(app_token, "bitable"):
                print(f"[FEISHU] 已订阅多维表格 {app_token[:12]}... 事件", flush=True)
            elif app_token:
                print(f"[FEISHU] 订阅多维表格失败（需应用为文档拥有者/管理员）", flush=True)
        elif source in ("doc", "docx"):
            if isinstance(doc_id, str) and doc_id and subscribe_drive_file(doc_id, source):
                print(f"[FEISHU] 已订阅文档 {doc_id[:12]}... 事件", flush=True)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="飞书 RAG 机器人服务")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--no-sync-loop", action="store_true", help="不启动后台文档同步")
    parser.add_argument("--no-drive-subscribe", action="store_true", help="不订阅云文档事件")
    args = parser.parse_args()

    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        print("请配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return

    if not FEISHU_DOC_IDS:
        print("请配置 FEISHU_DOC_IDS（逗号分隔的文档 ID）")
        return

    # 首次全量同步
    print("正在同步飞书文档...")
    sync_documents(on_update=_on_doc_update)
    print("文档同步完成")

    # 订阅云文档事件（变更时实时同步）
    if not args.no_drive_subscribe:
        _subscribe_drive_files()

    if not args.no_sync_loop:
        run_sync_loop(on_update=_on_doc_update)
        print("后台同步已启动")

    server = HTTPServer((args.host, args.port), FeishuEventHandler)
    print(f"飞书 RAG 服务已启动: http://{args.host}:{args.port}", flush=True)
    print("事件订阅 URL: https://你的ngrok地址/feishu/event", flush=True)
    print("收到消息时本窗口会打印 [FEISHU] 日志，请留意", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
