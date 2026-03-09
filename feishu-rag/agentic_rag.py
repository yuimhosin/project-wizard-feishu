# -*- coding: utf-8 -*-
"""
Agentic RAG: 智能体驱动的检索增强生成
- Agent 根据问题自动选择工具：RAG 检索 / 统计分析
- 支持多轮决策、查询重写（可选）
"""
import sys
from pathlib import Path
from typing import Literal

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import LLM_API_KEY, LLM_API_BASE, LLM_MODEL, TOP_K, TOP_K_LIST

_agent_graph = None


def _get_rag_engine():
    from rag_engine import RAGEngine
    return RAGEngine.get_cached()


def _rag_search(query: str) -> str:
    """向量检索：从知识库中搜索与问题相关的文档片段。适用于：具体事件查询、某时间/机构/类型的事件列表。"""
    rag = _get_rag_engine()
    k = TOP_K_LIST if any(kw in query for kw in ("所有", "全部", "有哪些", "多少", "列出")) else TOP_K
    chunks = rag.search(query, top_k=k)
    if not chunks:
        return "未找到相关文档内容。"
    return "\n\n---\n\n".join([c["content"] for c in chunks])


def _stats_analysis(question: str) -> str:
    """统计分析：直接从多维表格拉取全量数据做聚合统计。适用于：机构排名、上报数量、按时间/分类统计。"""
    from stats_analysis import get_records, format_stats_report
    recs = get_records()
    return format_stats_report(recs, question)


def _get_current_time(timezone: str = "Asia/Shanghai") -> str:
    """获取当前时间。东八区显式用上海时区（避免服务器 UTC 导致显示国际时间），其他时区联网获取"""
    from datetime import datetime
    if timezone in ("Asia/Shanghai", "Asia/Hong_Kong", ""):
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
        except ImportError:
            from datetime import timezone, timedelta
            return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    try:
        import urllib.request
        import json
        url = f"https://worldtimeapi.org/api/timezone/{timezone}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
        dt = data.get("datetime", "")
        if dt:
            return dt[:19].replace("T", " ")
    except Exception:
        pass
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    except ImportError:
        from datetime import timezone, timedelta
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def _create_tools():
    """创建 Agent 可调用的工具"""
    from langchain_core.tools import tool

    @tool
    def rag_search(query: str) -> str:
        """向量检索：从知识库搜索相关文档。用于查询具体事件、某时间/机构的事件列表等。"""
        return _rag_search(query)

    @tool
    def stats_analysis(question: str) -> str:
        """统计分析：对多维表格做聚合统计。用于机构排名、上报数量、按月份/分类统计等。"""
        return _stats_analysis(question)

    @tool
    def get_current_time(timezone: str = "Asia/Shanghai") -> str:
        """联网获取当前时间。用于：现在几点了、今天日期、当前时间等。timezone 如 Asia/Shanghai（中国）、America/New_York 等。"""
        return _get_current_time(timezone)

    return [rag_search, stats_analysis, get_current_time]


def _get_llm():
    """DeepSeek（OpenAI 兼容）"""
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=LLM_MODEL,
        api_key=LLM_API_KEY,
        base_url=LLM_API_BASE,
        temperature=0.2,
    )


def _build_agent():
    """构建 Agentic RAG 图（缓存复用）"""
    global _agent_graph
    if _agent_graph is not None:
        return _agent_graph
    from langgraph.graph import StateGraph, MessagesState, START, END
    from langgraph.prebuilt import ToolNode
    from langchain_core.messages import SystemMessage

    tools = _create_tools()
    llm = _get_llm().bind_tools(tools)
    tool_node = ToolNode(tools)

    SYSTEM = """你是企业知识库助手。根据用户问题选择工具：
- 现在几点/今天日期/当前时间 → 用 get_current_time
- 统计/机构排名/上报数量/哪个最积极/按月份 → 用 stats_analysis
- 电梯/漏水/渗漏/特种设备/困人/故障/管道/消防/人身安全 等具体事件 → 用 stats_analysis
- 其他查询 → 用 rag_search

回答要求：当 stats_analysis 返回事件详情时，请先做简要分析（机构分布、事件类型、时间趋势等），再列出关键记录，不要只罗列数据。"""

    def agent(state):
        msgs = [SystemMessage(content=SYSTEM)] + list(state["messages"])
        response = llm.invoke(msgs)
        return {"messages": [response]}

    def should_continue(state) -> Literal["tools", "__end__"]:
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return "__end__"

    workflow = StateGraph(MessagesState)
    workflow.add_node("agent", agent)
    workflow.add_node("tools", tool_node)

    workflow.add_edge(START, "agent")
    workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", "__end__": END})
    workflow.add_edge("tools", "agent")

    _agent_graph = workflow.compile()
    return _agent_graph


def _fast_path(question: str):
    """
    快捷路径：问题明确匹配某工具时直接调用，省 1 次 LLM 往返（约 1–3 秒）。
    """
    q = question.strip()
    # 时间类：直接返回，无需 LLM
    if any(kw in q for kw in ("现在几点", "今天日期", "当前时间", "今天几号", "现在什么时候")):
        return _get_current_time()
    # 仅纯统计类走快捷路径（直接返回数字）；具体事件查询（电梯/漏水等）走 Agent，由 LLM 做分析总结
    if any(kw in q for kw in ("统计", "哪个机构", "上报最积极", "机构排名", "上报数量", "按月份")):
        return _stats_analysis(question)
    return None


def query(question: str, use_fast_path: bool = True) -> str:
    """
    Agentic RAG 入口：优先快捷路径，否则 Agent 选择工具并生成回答。
    use_fast_path=False 时跳过快捷路径，始终走 LLM 分析。
    """
    if not LLM_API_KEY:
        return "LLM 未配置，无法使用 Agentic RAG。"

    # 快捷路径：省 1 次 LLM 调用（可关闭以获取 LLM 分析）
    if use_fast_path:
        fast = _fast_path(question)
        if fast is not None:
            return fast

    from langchain_core.messages import HumanMessage

    graph = _build_agent()
    result = graph.invoke({"messages": [HumanMessage(content=question)]})

    last_msg = result["messages"][-1]
    if hasattr(last_msg, "content") and last_msg.content:
        return (last_msg.content or "").strip()
    return "未能生成回答，请重试。"


if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else "哪个机构上报最积极？"
    print("问题:", q)
    print("回答:", query(q))
