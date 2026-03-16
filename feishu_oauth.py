# -*- coding: utf-8 -*-
"""
飞书 OAuth 网页登录 / 工作台免登：授权、兑换 token、获取用户信息
- 工作台免登：从飞书工作台打开应用时自动带 code，无需点击登录
- 配置：安全设置 -> 重定向URL；网页应用 -> 桌面端主页 = 本模块生成的授权链接
"""
import os
import json
import time
import urllib.request
import urllib.parse
from typing import Optional
from pathlib import Path

# 加载 .env
try:
    from dotenv import load_dotenv
    for _p in [Path(__file__).resolve().parent / ".env", Path(__file__).resolve().parent.parent / ".env"]:
        if _p.exists():
            load_dotenv(_p)
            break
except ImportError:
    pass

FEISHU_API = "https://open.feishu.cn/open-apis"
FEISHU_AUTH_URL = "https://open.feishu.cn/open-apis/authen/v1/authorize"

_app_token_cache = {"token": None, "expires_at": 0}


def _get_app_id_secret():
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    return app_id, app_secret


def get_app_access_token() -> Optional[str]:
    """获取 app_access_token（用于 OAuth 兑换 user_access_token）"""
    app_id, app_secret = _get_app_id_secret()
    if not app_id or not app_secret:
        return None
    now = time.time()
    if _app_token_cache["token"] and _app_token_cache["expires_at"] > now + 300:
        return _app_token_cache["token"]
    url = f"{FEISHU_API}/auth/v3/app_access_token/internal"
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                token = data.get("app_access_token")
                expire = data.get("expire", 7200)
                _app_token_cache["token"] = token
                _app_token_cache["expires_at"] = now + expire
                return token
    except Exception:
        pass
    return None


def build_authorize_url(redirect_uri: str, state: str = "") -> str:
    """构建飞书授权链接。redirect_uri 需与飞书开放平台配置一致。"""
    app_id, _ = _get_app_id_secret()
    if not app_id:
        return ""
    params = {
        "app_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": "contact:user.base:readonly",  # 获取用户基本信息
        "state": state or "default",
    }
    return f"{FEISHU_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_user(code: str) -> Optional[dict]:
    """
    用授权码兑换 user_access_token 并获取用户信息。
    返回 {"open_id": str, "name": str, "avatar_url": str, ...} 或 None
    """
    app_token = get_app_access_token()
    if not app_token:
        return None
    url = f"{FEISHU_API}/authen/v1/access_token"
    body = json.dumps({"grant_type": "authorization_code", "code": code}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {app_token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            d = data.get("data", {})
            access_token = d.get("access_token")
            if not access_token:
                return None
            # 获取用户信息
            info = _get_user_info(access_token)
            if info:
                return info
            # 若 user_info 失败，至少返回 open_id（部分接口会在 data 中返回）
            return {"open_id": d.get("open_id", ""), "name": "", "avatar_url": ""}
    except Exception:
        return None


def get_workbench_authorize_url(redirect_uri: str) -> str:
    """
    获取工作台免登用的授权链接，用于飞书开放平台「网页应用」->「桌面端主页」。
    桌面端主页填此 URL，用户从工作台点击应用时会跳转飞书授权并带回 code。
    """
    return build_authorize_url(redirect_uri)


def _get_user_info(user_access_token: str) -> Optional[dict]:
    """用 user_access_token 获取用户信息"""
    url = f"{FEISHU_API}/authen/v1/user_info"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {user_access_token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            d = data.get("data", {})
            return {
                "open_id": d.get("open_id", ""),
                "name": d.get("name", ""),
                "avatar_url": d.get("avatar_url", ""),
                "union_id": d.get("union_id", ""),
                "user_id": d.get("user_id", ""),
            }
    except Exception:
        return None


if __name__ == "__main__":
    import sys
    uri = sys.argv[1] if len(sys.argv) > 1 else os.getenv("FEISHU_REDIRECT_URI", "http://localhost:8501/")
    url = build_authorize_url(uri)
    if url:
        print("工作台免登 - 桌面端主页填写以下链接：")
        print(url)
    else:
        print("请先配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
