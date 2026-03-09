# -*- coding: utf-8 -*-
"""
清洗 doc_contents.json：
1. 时间戳转为可读格式 YYYY/MM/DD HH:MM
2. 删除图片/文件等冗余数据（file_token, tmp_url, avatar_url 等）
3. 删除图片 URL、base64 图片、多余换行符和空白
"""
import json
import re
from datetime import datetime
from pathlib import Path

VECTOR_DB = Path(__file__).resolve().parent / "vector_db"
DOC_CONTENTS = VECTOR_DB / "doc_contents.json"

# 需要删除的 JSON 对象中的特征键（含这些键的对象整块删除）
_REMOVE_KEYS = ("file_token", "tmp_url", "avatar_url")

# 图片 URL 正则（匹配后整行或整段替换为 [图片已省略]）
_IMG_URL_PATTERN = re.compile(
    r"https?://[^\s\"'<>]+\.(?:png|jpg|jpeg|gif|webp|bmp|svg)(?:\?[^\s\"'<>]*)?",
    re.IGNORECASE,
)
# Base64 图片
_BASE64_IMG_PATTERN = re.compile(r"data:image/[a-zA-Z0-9+]+;base64,[A-Za-z0-9+/=]{50,}")
# 飞书开放平台图片/文件链接
_FEISHU_FILE_PATTERN = re.compile(
    r"https?://(?:open\.)?feishu\.cn/(?:open-apis/)?[^\s\"'<>]*(?:file|image|avatar)[^\s\"'<>]*",
    re.IGNORECASE,
)
# 图片/视频文件名（含飞书内部引用）
_IMG_FILENAME_PATTERN = re.compile(
    r"(?:^|[\s;,])([\w\-.]*\.(?:png|jpg|jpeg|gif|webp|bmp|svg|mp4|mov)\b|(?:img|file)_v3_[\w\-]+(?:\.(?:jpg|jpeg|png|gif|mp4))?)",
    re.IGNORECASE,
)


def _remove_file_avatar_objects(content: str) -> str:
    """
    删除 content 中包含 file_token、tmp_url、avatar_url 的 JSON 对象，
    替换为 [已省略]
    """
    result = []
    i = 0
    while i < len(content):
        if content[i] == "{":
            start = i
            depth = 1
            i += 1
            while i < len(content) and depth > 0:
                if content[i] == "{":
                    depth += 1
                elif content[i] == "}":
                    depth -= 1
                i += 1
            chunk = content[start:i]
            if any(f'"{k}"' in chunk for k in _REMOVE_KEYS):
                result.append("[已省略]")
            else:
                result.append(chunk)
        elif content[i] == "[":
            # 处理数组：若内部有需删除的对象，整段替换
            start = i
            depth = 1
            i += 1
            while i < len(content) and depth > 0:
                if content[i] == "[":
                    depth += 1
                elif content[i] == "]":
                    depth -= 1
                i += 1
            chunk = content[start:i]
            if any(f'"{k}"' in chunk for k in _REMOVE_KEYS):
                result.append("[已省略]")
            else:
                result.append(chunk)
        else:
            result.append(content[i])
            i += 1
    return "".join(result)


def ts_to_readable(ts_str: str) -> str:
    """将毫秒时间戳转为 YYYY/MM/DD HH:MM"""
    try:
        ts = int(ts_str)
        if ts < 1e10:  # 可能是秒级
            ts = ts * 1000
        elif ts > 1e15:  # 超出合理范围
            return ts_str
        dt = datetime.fromtimestamp(ts / 1000)
        return dt.strftime("%Y/%m/%d %H:%M")
    except (ValueError, OSError):
        return ts_str


def _remove_images_and_whitespace(content: str) -> str:
    """删除图片 URL、base64 图片、文件名、飞书文件引用、多余换行和空白"""
    # 删除图片 URL
    content = _IMG_URL_PATTERN.sub("[图片已省略]", content)
    content = _FEISHU_FILE_PATTERN.sub("[图片已省略]", content)
    # 删除 base64 图片
    content = _BASE64_IMG_PATTERN.sub("[图片已省略]", content)
    # 删除图片/视频文件名及飞书内部引用（img_v3_xxx, file_v3_xxx）
    content = _IMG_FILENAME_PATTERN.sub(" [图片已省略]", content)
    # 合并连续多个 [图片已省略] 为单个；整行仅图片时简化为 [已省略]
    content = re.sub(r"(\[图片已省略\]\s*)+", "[图片已省略] ", content)
    content = re.sub(r"([^\n:]+):\s*\[图片已省略\]\s*", r"\1: [已省略]\n", content)
    # 删除仅含 [图片已省略] 的整行（避免留下空行噪音）
    content = re.sub(r"\n\s*\[图片已省略\]\s*\n", "\n", content)
    content = re.sub(r"^\s*\[图片已省略\]\s*\n", "", content)
    # 将 Tab 转为空格，连续空格/制表符压缩为单个空格
    content = re.sub(r"[ \t]+", " ", content)
    # 删除行首行尾空白
    content = "\n".join(line.strip() for line in content.splitlines())
    # 连续空行压缩为单个换行
    content = re.sub(r"\n{2,}", "\n", content)
    return content.strip()


def clean_content(content: str) -> str:
    """
    同步知识库时自动调用的清洗函数：
    1. 删除图片/文件等冗余数据（file_token, tmp_url, avatar_url）
    2. 删除图片 URL、base64 图片
    3. 清洗时间戳：转为可读格式 YYYY/MM/DD HH:MM
    4. 删除多余换行符、空白、分隔符
    """
    if not content or not content.strip():
        return content

    # 先删除 file/avatar 对象
    content = _remove_file_avatar_objects(content)

    # 删除图片和多余空白
    content = _remove_images_and_whitespace(content)

    # 删除记录分隔符（---）及多余换行
    content = re.sub(r"\n-{2,}\n", "\n", content)
    content = re.sub(r"\n{2,}", "\n", content)

    # 清洗时间戳：所有 12-13 位数字转为可读格式 YYYY/MM/DD HH:MM
    def repl(m):
        field, num = m.group(1), m.group(2)
        if num.isdigit() and 12 <= len(num) <= 13:
            return f"{field}: {ts_to_readable(num)}"
        return m.group(0)

    pattern = r"([^\n:]+): (\d{12,13})(?=\s|$|\n)"
    content = re.sub(pattern, repl, content)

    return content.strip()


def clean_all_doc_contents() -> int:
    """
    批量清洗 doc_contents.json 中所有文档，并删除向量索引以触发重建。
    同步知识库后自动调用。返回清洗的文档数量。
    """
    if not DOC_CONTENTS.exists():
        return 0
    try:
        with open(DOC_CONTENTS, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    for doc_id, item in data.items():
        content = item.get("content", "")
        if content:
            item["content"] = clean_content(content)

    with open(DOC_CONTENTS, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    faiss_index = VECTOR_DB / "faiss_index"
    if faiss_index.exists():
        import shutil
        shutil.rmtree(faiss_index)
    return len(data)


if __name__ == "__main__":
    if not DOC_CONTENTS.exists():
        print("doc_contents.json 不存在，请先同步知识库")
        exit(1)
    with open(DOC_CONTENTS, "r", encoding="utf-8") as f:
        data = json.load(f)

    for doc_id, item in data.items():
        content = item.get("content", "")
        if content:
            item["content"] = clean_content(content)

    with open(DOC_CONTENTS, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    faiss_index = VECTOR_DB / "faiss_index"
    if faiss_index.exists():
        import shutil
        shutil.rmtree(faiss_index)
        print("已删除旧向量索引，RAG 将使用清洗后内容重建")

    print("清洗完成（时间戳 + 图片/文件/头像/换行符），已保存到", DOC_CONTENTS)
