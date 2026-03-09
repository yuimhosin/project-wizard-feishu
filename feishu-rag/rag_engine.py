# -*- coding: utf-8 -*-
"""
RAG engine: local HuggingFace embedding + FAISS + DeepSeek.
Reference: wikiqa_rag
"""
import json
from pathlib import Path
from typing import List, Optional

from config import (
    CHUNK_SIZE,
    CHUNK_OVERLAP,
    TOP_K,
    TOP_K_LIST,
    VECTOR_DB_PATH,
    LLM_API_KEY,
    LLM_API_BASE,
    LLM_MODEL,
)


def _chunk_text(text: str, chunk_size: int = None, overlap: int = None) -> List[str]:
    """Chunk by character count with overlap"""
    cs = chunk_size or CHUNK_SIZE
    ov = overlap or CHUNK_OVERLAP
    if not text or not text.strip():
        return []
    text = text.strip()
    chunks = []
    start = 0
    while start < len(text):
        end = start + cs
        chunk = text[start:end]
        if not chunk.strip():
            start = end - ov
            continue
        chunks.append(chunk)
        start = end - ov
    return chunks


def _get_llm_client():
    """DeepSeek client (OpenAI compatible)"""
    try:
        from openai import OpenAI
        return OpenAI(api_key=LLM_API_KEY, base_url=LLM_API_BASE)
    except ImportError:
        return None


_rag_singleton = None


class RAGEngine:
    """RAG engine: FAISS + local embedding + DeepSeek. Rebuild index on doc update."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or VECTOR_DB_PATH
        self._db = None
        self._contents_file = Path(self.db_path) / "doc_contents.json"

    @classmethod
    def get_cached(cls, db_path: str = None):
        """获取缓存的 RAGEngine 实例，避免重复加载 FAISS"""
        global _rag_singleton
        if _rag_singleton is None:
            _rag_singleton = cls(db_path)
        return _rag_singleton

    def _get_embeddings(self):
        from model_manager import get_embedding_model
        return get_embedding_model()

    def _load_contents(self) -> dict:
        """Load stored doc contents {doc_id: {content, title}}"""
        if not self._contents_file.exists():
            return {}
        try:
            with open(self._contents_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_contents(self, contents: dict):
        Path(self.db_path).mkdir(parents=True, exist_ok=True)
        with open(self._contents_file, "w", encoding="utf-8") as f:
            json.dump(contents, f, ensure_ascii=False, indent=2)

    def _rebuild_db(self, contents: dict):
        """Rebuild FAISS from all stored contents"""
        from langchain_core.documents import Document
        from langchain_community.vectorstores import FAISS

        all_docs = []
        for doc_id, data in contents.items():
            content = data.get("content", "")
            title = data.get("title", "")
            if not content.strip():
                continue
            chunks = _chunk_text(content)
            for c in chunks:
                all_docs.append(Document(page_content=c, metadata={"doc_id": doc_id, "title": title}))

        if not all_docs:
            all_docs = [Document(page_content=" ", metadata={"doc_id": "__empty__", "title": ""})]

        self._db = FAISS.from_documents(all_docs, self._get_embeddings())
        self._save_db()

    def _save_db(self):
        if self._db is None:
            return
        Path(self.db_path).mkdir(parents=True, exist_ok=True)
        index_path = Path(self.db_path) / "faiss_index"
        self._db.save_local(str(index_path))

    def _get_db(self):
        if self._db is not None:
            return self._db
        from langchain_community.vectorstores import FAISS

        index_path = Path(self.db_path) / "faiss_index"
        if index_path.exists():
            self._db = FAISS.load_local(
                str(index_path),
                self._get_embeddings(),
                allow_dangerous_deserialization=True,
            )
        else:
            contents = self._load_contents()
            if contents:
                self._rebuild_db(contents)
            else:
                from langchain_core.documents import Document
                self._db = FAISS.from_documents(
                    [Document(page_content=" ", metadata={"doc_id": "__init__", "title": ""})],
                    self._get_embeddings(),
                )
                self._save_db()
        return self._db

    def add_document(self, doc_id: str, content: str, title: str = ""):
        """Store doc and rebuild index"""
        contents = self._load_contents()
        contents[doc_id] = {"content": content, "title": title}
        self._save_contents(contents)
        self._rebuild_db(contents)
        self._save_db()

    def delete_document(self, doc_id: str):
        """Remove doc and rebuild index"""
        contents = self._load_contents()
        contents.pop(doc_id, None)
        self._save_contents(contents)
        self._rebuild_db(contents)
        self._save_db()

    def invalidate_index(self):
        """清除缓存的向量索引，下次查询时从 doc_contents 重建"""
        self._db = None

    def search(self, query: str, top_k: int = None) -> List[dict]:
        """Retrieve relevant chunks"""
        k = top_k or TOP_K
        if not query or not query.strip():
            return []
        try:
            db = self._get_db()
            docs = db.similarity_search(query.strip(), k=k * 2)
            out = []
            for d in docs:
                if d.metadata.get("doc_id") in ("__init__", "__empty__"):
                    continue
                if not (d.page_content or "").strip():
                    continue
                out.append({"content": d.page_content, "metadata": d.metadata, "distance": 0})
                if len(out) >= k:
                    break
            return out
        except Exception:
            return []

    def query(self, question: str, top_k: int = None) -> str:
        """RAG: retrieve + DeepSeek generate。统计分析类问题走 bitable 直接统计"""
        q = question.strip()
        # 统计分析类、具体事件关键词：直接从多维表格拉取并筛选，不走向量检索
        if any(kw in q for kw in (
            "统计", "分析", "哪个机构", "上报最积极", "机构排名", "上报数量",
            "电梯", "漏水", "渗漏", "特种设备", "困人", "管道", "消防", "人身安全", "基础设施"
        )):
            try:
                from stats_analysis import get_records, format_stats_report
                recs = get_records()
                return format_stats_report(recs, q)
            except Exception as e:
                return f"统计分析失败：{e}"

        # 查询所有/列表类问题用更大 top_k
        if top_k is None:
            if any(kw in q for kw in ("所有", "全部", "查询", "列出", "有哪些", "多少")):
                top_k = TOP_K_LIST
            else:
                top_k = TOP_K
        chunks = self.search(question, top_k=top_k)
        if not chunks:
            return "暂无相关文档内容，请确认知识库已同步或尝试其他问题。"

        context = "\n\n---\n\n".join([c["content"] for c in chunks])
        prompt = f"""你是一个基于企业知识库的问答助手。请根据以下参考内容回答用户问题。如果参考内容中没有相关信息，请如实说明。

【参考内容】
{context}

【用户问题】
{question}

【回答要求】
- 仅基于参考内容回答，不要编造
- 若用户查询列表/所有/全部，请尽可能完整列出参考内容中的所有匹配项，不要只列举部分
- 回答简洁清晰
- 若无法从参考内容得出答案，请说明"""

        client = _get_llm_client()
        if not client or not LLM_API_KEY:
            return "LLM 未配置，无法生成回答。"

        try:
            r = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            return (r.choices[0].message.content or "").strip()
        except Exception as e:
            return f"生成回答时出错：{e}"
