# -*- coding: utf-8 -*-
"""
飞书 RAG 可视化界面 - Streamlit
支持在线同步：启动时若 vector_db 为空则自动从飞书拉取，无需提交数据库
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import streamlit as st

# set_page_config 必须是第一个 Streamlit 命令，需在 config 之前
st.set_page_config(
    page_title="飞书知识库问答",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 加载 config（会读取 secrets）
from config import FEISHU_DOC_IDS, VECTOR_DB_PATH

# 自定义样式：左侧设置栏 + 主内容区全宽，避免挤在中间
st.markdown("""
<style>
    /* 取消整体 max-width 限制，内容区全宽 */
    .stApp { max-width: 100%; padding: 0 2rem 2rem 2rem; }
    /* 侧边栏固定宽度，设置更清晰 */
    [data-testid="stSidebar"] { min-width: 280px; }
    /* 主内容区占满剩余空间，不再挤在中间 */
    .block-container { max-width: 100%; padding-left: 2rem; padding-right: 2rem; }
    .chat-user { 
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
        color: white; 
        padding: 12px 16px; 
        border-radius: 12px; 
        margin: 8px 0;
        margin-left: 20%;
    }
    .chat-bot { 
        background: #f0f2f6; 
        padding: 12px 16px; 
        border-radius: 12px; 
        margin: 8px 0;
        margin-right: 20%;
        border-left: 4px solid #667eea;
    }
    .quick-btn { margin: 4px; }
</style>
""", unsafe_allow_html=True)


def _need_sync() -> bool:
    """检查是否需要同步：vector_db 为空或不存在"""
    from pathlib import Path
    db_path = Path(VECTOR_DB_PATH)
    contents_file = db_path / "doc_contents.json"
    if not contents_file.exists():
        return True
    try:
        import json
        with open(contents_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return not data or len(data) == 0
    except Exception:
        return True


def _run_sync() -> dict:
    """从飞书在线同步文档到本地 vector_db，同步后自动清洗，返回统计"""
    from feishu_doc_sync import sync_documents
    from rag_engine import RAGEngine
    from clean_timestamps import clean_content, clean_all_doc_contents
    rag = RAGEngine.get_cached()

    def on_update(doc_id: str, content: str, title: str):
        rag.add_document(doc_id, clean_content(content), title)

    stats = sync_documents(on_update=on_update)
    # 同步后批量清洗：去除图片、换行符等，确保历史数据一致
    n = clean_all_doc_contents()
    if n > 0:
        rag.invalidate_index()
    return stats


def get_answer(question: str, use_agentic: bool, use_fast_path: bool = True) -> str:
    """根据模式获取回答"""
    if use_agentic:
        from agentic_rag import query
        return query(question, use_fast_path=use_fast_path)
    else:
        from rag_engine import RAGEngine
        rag = RAGEngine.get_cached()
        return rag.query(question)


def main():
    st.title("📚 飞书知识库问答")
    st.caption("基于多维表格的 RAG 检索与 Agentic 智能问答")

    # 侧边栏
    with st.sidebar:
        st.header("⚙️ 设置")

        # 在线同步：无数据库时自动同步，或手动触发
        if not FEISHU_DOC_IDS:
            st.warning("未配置 FEISHU_DOC_IDS，请在 Secrets 中填写文档链接")
        else:
            need_auto = _need_sync() and "sync_done" not in st.session_state
            if need_auto:
                with st.spinner("正在从飞书同步文档..."):
                    try:
                        stats = _run_sync()
                        st.session_state.sync_done = True
                        st.success(f"同步完成：更新 {stats.get('updated', 0)} 篇")
                    except Exception as e:
                        st.error(f"同步失败：{e}")
                        st.session_state.sync_done = True
                    st.rerun()
            if st.button("🔄 同步知识库", help="从飞书重新拉取文档"):
                with st.spinner("同步中..."):
                    try:
                        stats = _run_sync()
                        st.success(f"同步完成：更新 {stats.get('updated', 0)} 篇")
                    except Exception as e:
                        st.error(f"同步失败：{e}")
                st.rerun()

        st.divider()
        use_agentic = st.radio(
            "模式",
            ["Agentic RAG", "经典 RAG"],
            index=0,
            help="Agentic 可自动选择检索/统计/时间等工具；经典仅向量检索",
        )
        use_agentic = use_agentic == "Agentic RAG"
        use_fast_path = st.checkbox(
            "快捷路径（跳过 LLM 分析）",
            value=False,
            help="开启后统计/机构排名等直接返回数据，不经过 LLM 分析总结",
        ) if use_agentic else False

        st.divider()
        st.markdown("**快捷问题**")
        quick_questions = [
            "现在几点了？",
            "哪个机构上报最积极？",
            "查询 2026/01 的所有事件",
            "按月份统计上报数量",
        ]
        for q in quick_questions:
            if st.button(q, key=q, use_container_width=True):
                st.session_state.quick_q = q
                st.rerun()  # 触发处理快捷问题

    # 初始化对话历史
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # 快捷问题
    if "quick_q" in st.session_state:
        q = st.session_state.quick_q
        del st.session_state.quick_q
        if q:
            with st.spinner("思考中..."):
                ans = get_answer(q, use_agentic, use_fast_path=use_fast_path)
            st.session_state.messages.append({"role": "user", "content": q})
            st.session_state.messages.append({"role": "assistant", "content": ans})
            st.rerun()  # 刷新显示新对话

    # 显示对话历史
    for msg in st.session_state.messages:
        role = msg["role"]
        content = msg["content"]
        with st.chat_message(role):
            st.markdown(content)

    # 输入框
    if prompt := st.chat_input("输入你的问题..."):
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            with st.spinner("思考中..."):
                answer = get_answer(prompt, use_agentic, use_fast_path=use_fast_path)
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})

    # 清空对话
    if st.session_state.messages:
        st.sidebar.divider()
        if st.sidebar.button("🗑️ 清空对话"):
            st.session_state.messages = []
            st.rerun()


if __name__ == "__main__":
    main()
