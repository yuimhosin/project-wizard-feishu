# -*- coding: utf-8 -*-
"""
养老社区改良改造进度管理 - Streamlit 交互看板
审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部运行保障协调招采/施工 → 总部督促验收
飞书工作台免登：从工作台打开可免登，直接打开则自动跳转飞书授权。
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import os
import sys
import sqlite3

# 项目根目录（app203.py 在根目录时 parent 即为根目录）
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_loader import load_single_csv, load_from_directory, load_uploaded, get_稳定需求_mask, TIMELINE_COLS
from location_config import 园区_TO_城市, 城市_COORDS

try:
    from openai import OpenAI
    DEEPSEEK_CLIENT_AVAILABLE = True
except ImportError:
    DEEPSEEK_CLIENT_AVAILABLE = False

# 飞书 OAuth 登录（可选：配置 FEISHU_APP_ID、FEISHU_APP_SECRET、FEISHU_REDIRECT_URI 后启用）
FEISHU_LOGIN_ENABLED = bool(os.getenv("FEISHU_APP_ID") and os.getenv("FEISHU_APP_SECRET"))

# 图表配色：饼图用 20+ 种不重复颜色，避免多分类时颜色重复
CHART_COLORS_PIE = [
    "#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de",
    "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc", "#5ad8a6",
    "#6dc8ec", "#945fb9", "#ff9845", "#1e9bb5", "#ffbf00",
    "#c23531", "#2f4554", "#61a0a8", "#d48265", "#749f83",
    "#ca8622", "#bda29a", "#6e7074", "#546570", "#c4ccd3",
]

st.set_page_config(
    page_title="养老社区改良改造进度管理",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _require_feishu_login() -> bool:
    """飞书工作台免登：从工作台打开时自动带 code，无需点击。未登录时自动跳转飞书授权。"""
    if not FEISHU_LOGIN_ENABLED:
        return True
    if "feishu_user" not in st.session_state:
        st.session_state.feishu_user = None
    # 处理 OAuth 回调 / 工作台免登：URL 中带 code
    try:
        q = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
    except Exception:
        q = {}
    code = q.get("code", [None])[0] if isinstance(q.get("code"), list) else q.get("code")
    if code and not st.session_state.feishu_user:
        try:
            from feishu_oauth import exchange_code_for_user
            user = exchange_code_for_user(code)
            if user:
                st.session_state.feishu_user = user
                try:
                    if hasattr(st, "query_params") and "code" in st.query_params:
                        del st.query_params["code"]
                except Exception:
                    pass
                st.rerun()
        except Exception:
            pass
    if st.session_state.feishu_user:
        return True
    # 未登录：工作台免登 - 自动跳转飞书授权（从工作台打开会带 code，直接打开则跳转）
    from feishu_oauth import build_authorize_url
    redirect_uri = os.getenv("FEISHU_REDIRECT_URI", "")
    if not redirect_uri:
        redirect_uri = os.getenv("STREAMLIT_SERVER_BASE_URL", "http://localhost:8501/")
        if redirect_uri and not redirect_uri.endswith("/"):
            redirect_uri += "/"
    auth_url = build_authorize_url(redirect_uri)
    st.title("养老社区改良改造进度管理")
    st.markdown("---")
    if auth_url:
        st.info("正在跳转到飞书登录… 请从飞书工作台打开本应用可免登。")
        # 工作台免登：自动跳转，无需点击
        st.markdown(
            f'<meta http-equiv="refresh" content="1;url={auth_url}">'
            f'<p>若未自动跳转，请 <a href="{auth_url}" target="_self">点击此处</a> 使用飞书登录。</p>',
            unsafe_allow_html=True,
        )
        st.link_button("🔐 飞书登录", auth_url, type="primary")
        st.caption("工作台免登：飞书开放平台 → 应用 → 网页应用 → 桌面端主页 填授权链接；安全设置 → 重定向URL 填本应用地址。")
    else:
        st.warning("飞书登录未配置。请设置 FEISHU_APP_ID、FEISHU_APP_SECRET、FEISHU_REDIRECT_URI。")
    st.stop()


# 默认数据目录与单文件（Streamlit Cloud 部署用项目根目录）
DEFAULT_DATA_DIR = str(ROOT_DIR)
DEFAULT_SINGLE_FILE = str(ROOT_DIR / "改良改造报表-V4.csv")

# 内嵌默认数据（加密存储，随 git 提交，数据库为空时自动加载）
DEFAULT_BUNDLED_CSV = ROOT_DIR / "改良改造报表-V4-sample.csv.enc"

# 专业 9 大类（与 CSV 中「专业」列对应，用于分类统计）
专业大类 = [
    "土建设施", "供配电系统", "暖通/供冷系统", "弱电系统", "供排水系统",
    "电梯系统", "其它系统", "消防系统", "安防系统"
]


# ---------- 团队共享数据：SQLite 存储 ----------
DB_PATH = os.getenv("APP203_DB_PATH", "app203_projects.db")


def _get_db_connection():
    return sqlite3.connect(DB_PATH)


def load_from_db() -> pd.DataFrame:
    """从 SQLite 加载团队共享数据表 projects。若不存在则返回空表。"""
    if not Path(DB_PATH).exists():
        return pd.DataFrame()
    try:
        with _get_db_connection() as conn:
            return pd.read_sql("SELECT * FROM projects", conn)
    except Exception:
        return pd.DataFrame()


def save_to_db(df: pd.DataFrame):
    """将当前 DataFrame 全量写入 SQLite（覆盖 projects 表）。"""
    if df is None or df.empty:
        return
    with _get_db_connection() as conn:
        df.to_sql("projects", conn, if_exists="replace", index=False)


def _ensure_default_data_seeded():
    """应用启动时：若数据库为空且存在内嵌 .enc 文件，则自动加载并写入数据库。"""
    if not load_from_db().empty:
        return
    bundled = DEFAULT_BUNDLED_CSV if DEFAULT_BUNDLED_CSV.exists() else ROOT_DIR / "改良改造报表-V4-sample.csv"
    if not bundled.exists():
        return
    try:
        df = load_single_csv(str(bundled))
        if not df.empty:
            save_to_db(df)
    except Exception:
        pass


def _ensure_project_columns(df: pd.DataFrame) -> pd.DataFrame:
    """保证关键列存在，便于新增/修改向导统一写入。"""
    needed = [
        "序号", "园区", "所属区域", "城市", "所属业态",
        "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
        "专业", "专业分包", "项目名称", "备注说明", "拟定金额", "上传凭证",
    ]
    out = df.copy()
    for col in needed:
        if col not in out.columns:
            out[col] = "" if col not in ["序号", "拟定金额"] else 0
    return out


def _strip_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """去掉列名为空字符串的列，避免 data_editor 因重复空列名报错。"""
    keep_cols = [c for c in df.columns if str(c).strip() != ""]
    return df[keep_cols].copy()


def _canonicalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    加载后统一规范化：只保留分析所需列、合并城市列、固定列顺序，避免多列/错位导致后面列显示为空。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # 去掉空列名
    out = _strip_empty_columns(out)
    # 社区 -> 园区
    if "社区" in out.columns and "园区" not in out.columns:
        out = out.rename(columns={"社区": "园区"})
    elif "社区" in out.columns and "园区" in out.columns:
        out["园区"] = out["园区"].fillna(out["社区"])
        out = out.drop(columns=["社区"], errors="ignore")
    # 所在城市 -> 合并到 城市，只保留一列
    if "所在城市" in out.columns:
        if "城市" not in out.columns:
            out["城市"] = out["所在城市"]
        else:
            out["城市"] = out["城市"].fillna(out["所在城市"])
        out = out.drop(columns=["所在城市"], errors="ignore")
    # 专业细分 -> 专业分包（若缺）
    if "专业细分" in out.columns and "专业分包" not in out.columns:
        out["专业分包"] = out["专业细分"]
    if "专业细分" in out.columns and "专业分包" in out.columns:
        out["专业分包"] = out["专业分包"].fillna(out["专业细分"])
    if "专业细分" in out.columns:
        out = out.drop(columns=["专业细分"], errors="ignore")
    # 拟定金额 转数值
    if "拟定金额" in out.columns:
        out["拟定金额"] = pd.to_numeric(out["拟定金额"], errors="coerce").fillna(0)
    # 序号 转数值（保持整数显示）
    if "序号" in out.columns:
        out["序号"] = pd.to_numeric(out["序号"], errors="coerce")
    # 只保留已知列并固定顺序，避免多余列导致错位
    base_order = [
        "序号", "园区", "所属区域", "城市", "所属业态",
        "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
        "专业", "专业分包", "项目名称", "备注说明", "拟定金额",
    ]
    timeline_cols = [c for c in TIMELINE_COLS if c in out.columns]
    extra = ["上传凭证"] if "上传凭证" in out.columns else []
    want = base_order + timeline_cols + extra
    # 保留所有在 want 里的列；不在 want 里的其他列也保留在末尾，避免丢数据
    existing = list(out.columns)
    ordered = [c for c in want if c in existing]
    rest = [c for c in existing if c not in ordered]
    out = out[ordered + rest].copy()
    return out


def _get_next_序号(df: pd.DataFrame) -> int:
    """根据现有数据自动生成下一个序号。"""
    if "序号" not in df.columns or df.empty:
        return 1
    try:
        nums = pd.to_numeric(df["序号"], errors="coerce")
        m = nums.max()
        return int(m) + 1 if pd.notna(m) else 1
    except Exception:
        return 1


@st.cache_data(ttl=300)
def load_data(source_type: str, path_or_dir: str, pattern: str = "*.csv") -> pd.DataFrame:
    """根据数据源类型加载数据。"""
    if source_type == "单文件":
        return load_single_csv(path_or_dir)
    return load_from_directory(path_or_dir, pattern)


def render_审核流程说明():
    """审核流程说明区块。"""
    st.markdown("### 📋 需求审核与实施流程说明")
    steps = [
        ("1. 社区提出", "一线园区提出改造需求。"),
        ("2. 紧急程度分级", "按一级（最高级）、二级、三级划分。"),
        ("3. 专业分类", "按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。"),
        ("4. 财务预算拆分", "按预算系统进行金额拆分与汇总。"),
        ("5. 一线立项时间", "一线填写需求并提出立项时间。"),
        ("6. 项目部施工", "项目部根据已确定的需求立项组织施工。"),
        ("7. 总部运行保障部", "督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。"),
        ("8. 施工验收", "总部运行保障部督促一线园区进行最终施工验收。"),
    ]
    for title, desc in steps:
        st.markdown(f"- **{title}**：{desc}")
    st.divider()


def render_园区分级分类(df: pd.DataFrame, 园区选择: list):
    """各园区分级、专业分类统计与明细。"""
    st.subheader("各园区分级分类统计")
    sub = df[df["园区"].isin(园区选择)] if 园区选择 else df

    c1, c2, c3 = st.columns(3)
    with c1:
        by_level = sub.groupby("项目分级", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**按紧急程度（分级）**")
        st.dataframe(by_level, use_container_width=True, hide_index=True)
    with c2:
        by_prof = sub.groupby("专业", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**按专业分类**")
        st.dataframe(by_prof, use_container_width=True, hide_index=True)
    with c3:
        by_park = sub.groupby("园区", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**按园区**")
        st.dataframe(by_park, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("**全部项目清单（可筛选）**")
    level_filter = st.multiselect("按分级筛选", options=sub["项目分级"].dropna().unique().tolist(), default=None)
    prof_filter = st.multiselect("按专业筛选", options=sub["专业"].dropna().unique().tolist(), default=None)
    detail = sub.copy()
    if level_filter:
        detail = detail[detail["项目分级"].isin(level_filter)]
    if prof_filter:
        detail = detail[detail["专业"].isin(prof_filter)]
    st.caption(f"共 {len(detail)} 条项目")
    base_cols = ["园区", "序号", "项目分级", "项目分类", "专业", "项目名称", "拟定金额", "拟定承建组织", "需求立项", "验收"]
    exist_cols = [c for c in base_cols if c in detail.columns]
    df_detail = detail[exist_cols].copy()
    if "验收" in df_detail.columns:
        df_detail = df_detail.rename(columns={"验收": "验收(社区需求完成交付)"})
    st.dataframe(df_detail, use_container_width=True, hide_index=True)


def render_总部视图(df: pd.DataFrame, 园区选择: list):
    """总部视图：各园区稳定需求数量与金额、施工进展、验收时间预告。"""
    st.subheader("总部视图：稳定需求与施工验收")
    sub = df[df["园区"].isin(园区选择)] if 园区选择 else df

    stable_mask = get_稳定需求_mask(sub)
    stable = sub[stable_mask]

    st.markdown("#### 各园区已确定稳定需求数量与金额")
    summary = stable.groupby("园区", dropna=False).agg(
        稳定需求数量=("序号", "count"),
        稳定需求金额=("拟定金额", "sum"),
    ).reset_index()
    col1, col2 = st.columns(2)
    with col1:
        st.metric("稳定需求项目数", int(stable["序号"].count()))
    with col2:
        st.metric("稳定需求金额合计（万元）", f"{stable['拟定金额'].sum():.0f}")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 施工进展与验收时间预告")
    # 验收列
    accept_col = "验收"
    if accept_col not in sub.columns:
        for c in sub.columns:
            if "验收" in str(c):
                accept_col = c
                break
    impl_col = "实施"
    if impl_col not in sub.columns:
        impl_col = [c for c in sub.columns if "实施" in str(c)]
        impl_col = impl_col[0] if impl_col else None

    preview = sub[["园区", "序号", "项目名称", "拟定金额", "拟定承建组织"]].copy()
    preview["实施时间"] = sub[impl_col] if impl_col and impl_col in sub.columns else ""
    preview["验收时间"] = sub[accept_col] if accept_col in sub.columns else ""
    # 过滤无效日期
    def valid_date(s):
        if pd.isna(s): return False
        t = str(s).strip()
        if not t or t.startswith("-") or "1900" in t: return False
        return True
    preview["验收有效"] = preview["验收时间"].map(valid_date)
    st.dataframe(preview, use_container_width=True, hide_index=True)

    st.markdown("**验收时间预告（仅含有效日期）**")
    accept_preview = preview[preview["验收有效"]].copy()
    if accept_preview.empty:
        st.info("暂无有效验收日期，请在一线填报「验收(社区需求完成交付)」节点。")
    else:
        accept_preview = accept_preview.sort_values("验收时间").drop(columns=["验收有效"])
        st.dataframe(accept_preview, use_container_width=True, hide_index=True)


def _add_城市列(df: pd.DataFrame) -> pd.DataFrame:
    """为 df 增加「城市」列（根据园区映射），不修改原表。

    若原始数据中已包含「城市」列，则优先保留原值，仅对为空/缺失的行按园区映射补全；
    若不包含，则完全根据园区映射生成城市，无法映射时标记为「其他」。
    """
    out = df.copy()
    if "城市" in out.columns:
        mapped = out["园区"].map(园区_TO_城市)
        col = out["城市"].astype(str)
        mask_empty = col.isna() | (col.str.strip() == "")
        out.loc[mask_empty, "城市"] = mapped[mask_empty]
        out["城市"] = out["城市"].fillna("其他")
    else:
        out["城市"] = out["园区"].map(园区_TO_城市).fillna("其他")
    return out


def _build_城市_园区明细(df: pd.DataFrame) -> dict:
    """按城市汇总，每个城市下为各园区的：园区名称、项目总数、总预算。供地图 tooltip 使用。"""
    sub = df[df["城市"].notna() & (df["城市"] != "其他")]
    if sub.empty:
        return {}
    by_city_park = sub.groupby(["城市", "园区"], dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    out = {}
    for city in by_city_park["城市"].unique():
        rows = by_city_park[by_city_park["城市"] == city]
        parks = []
        total_n = 0
        total_a = 0
        for _, r in rows.iterrows():
            n = int(r["项目数"])
            a = int(r["金额合计"])
            parks.append({"园区名称": str(r["园区"]), "项目数": n, "预算万元": int(round(a))})
            total_n += n
            total_a += a
        out[str(city)] = {"项目总数": total_n, "总预算万元": int(round(total_a)), "园区列表": parks}
    return out


def _render_中国地图(df: pd.DataFrame, city_tooltip_data: dict):
    """中国地图：悬浮显示城市下各园区详情；点击城市后通过 URL 参数筛选并跳转下方详情。"""
    try:
        from pyecharts.charts import Geo
        from pyecharts import options as opts
        from pyecharts.commons.utils import JsCode
    except ImportError:
        st.warning("请安装 pyecharts：pip install pyecharts")
        return
    by_city = df.groupby("城市", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    data = []
    for _, row in by_city.iterrows():
        city = row["城市"]
        if city in 城市_COORDS and city != "其他":
            data.append((city, int(row["项目数"])))
    if not data:
        st.info("当前数据中暂无已配置区位的城市，或请先在侧边栏选择园区。")
        return
    geo = Geo(init_opts=opts.InitOpts(width="100%", height="400px", theme="light"))
    geo.add_schema(maptype="china", is_roam=True)
    for city, (lon, lat) in 城市_COORDS.items():
        geo.add_coordinate(city, lon, lat)
    geo.add(
        "项目数",
        data,
        type_="effectScatter",
        symbol_size=14,
        effect_opts=opts.EffectOpts(scale=4, brush_type="stroke"),
        label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=11),
    )
    # 悬浮详情：园区名称、园区上报项目总数、园区总预算；城市级汇总（JS 中用 [] 访问中文键）
    tooltip_js = JsCode(
        """
        function(params) {
            var name = params.name;
            var value = params.value;
            var n = (value && value[2]) != null ? value[2] : (value || 0);
            var info = typeof window.MAP_TOOLTIP_DATA !== 'undefined' && window.MAP_TOOLTIP_DATA[name];
            if (info) {
                var s = '<div style="text-align:left; min-width:200px;">';
                s += '<b>' + name + '</b><br/>';
                s += '项目总数：' + (info['项目总数'] || n) + ' 项<br/>';
                s += '总预算：' + (info['总预算万元'] || 0) + ' 万元<br/>';
                s += '<hr style="margin:6px 0;"/>';
                s += '各园区：<br/>';
                var list = info['园区列表'] || [];
                for (var i = 0; i < list.length; i++) {
                    var p = list[i];
                    s += '· ' + (p['园区名称'] || '') + '｜' + (p['项目数'] || 0) + ' 项｜' + (p['预算万元'] || 0) + ' 万<br/>';
                }
                s += '</div>';
                return s;
            }
            return name + '<br/>项目数：' + n + ' 项';
        }
        """
    )
    geo.set_global_opts(
        title_opts=opts.TitleOpts(title="各地市项目分布（点击城市可筛选下方详情）", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="item", formatter=tooltip_js),
        visualmap_opts=opts.VisualMapOpts(
            min_=min(d[1] for d in data),
            max_=max(d[1] for d in data),
            is_piecewise=False,
            pos_left="left",
            range_color=["#e0f3f8", "#0868ac"],
        ),
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as f:
        geo.render(f.name)
        html_path = f.name
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        # 注入 tooltip 数据与点击跳转：悬浮用 MAP_TOOLTIP_DATA，点击后带 ?selected_city= 刷新并定位下方
        import json
        tooltip_json = json.dumps(city_tooltip_data, ensure_ascii=False)
        inject = (
            "<script>\n"
            "window.MAP_TOOLTIP_DATA = " + tooltip_json + ";\n"
            "function attachMapClick() {\n"
            "  var dom = document.querySelector('[id^=\"_\"]');\n"
            "  if (dom && window.echarts) {\n"
            "    var inst = window.echarts.getInstanceByDom(dom);\n"
            "    if (inst && !inst._mapClickAttached) {\n"
            "      inst._mapClickAttached = true;\n"
            "      inst.on('click', function(params) {\n"
            "        if (params && params.name) {\n"
            "          var u = window.top.location.pathname || '/';\n"
            "          var q = 'selected_city=' + encodeURIComponent(params.name);\n"
            "          window.top.location.href = u + (u.indexOf('?')>=0 ? '&' : '?') + q;\n"
            "        }\n"
            "      });\n"
            "    }\n"
            "  }\n"
            "}\n"
            "if (document.readyState === 'complete') { setTimeout(attachMapClick, 300); }\n"
            "else { document.addEventListener('DOMContentLoaded', function() { setTimeout(attachMapClick, 300); }); }\n"
            "</script>\n"
        )
        # pyecharts 渲染的图表在 div 内，在 body 末尾插入 script
        if "</body>" in html:
            html = html.replace("</body>", inject + "</body>")
        else:
            html = html + inject
        st.components.v1.html(html, height=450, scrolling=False)
    finally:
        try:
            Path(html_path).unlink(missing_ok=True)
        except Exception:
            pass


def _render_图表_简易(sub: pd.DataFrame):
    """无 plotly 时的简易柱状图回退。"""
    c1, c2 = st.columns(2)
    with c1:
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_prof.empty:
            st.bar_chart(by_prof.set_index("专业")["项目数"])
    with c2:
        by_level = sub.groupby("项目分级", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_level.empty:
            st.bar_chart(by_level.set_index("项目分级")["项目数"])
    by_park = sub.groupby("园区", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False).head(20)
    if not by_park.empty:
        st.bar_chart(by_park.set_index("园区")["项目数"])
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    if not by_prof_m.empty:
        st.bar_chart(by_prof_m.set_index("专业")["金额"])


def render_地图与统计(df: pd.DataFrame, 园区选择: list):
    """地图与统计 Tab：中国地图、选择城市查看详情、按专业/分级/园区图表。"""
    df_with_city = _add_城市列(df)
    sub = df_with_city[df_with_city["园区"].isin(园区选择)] if 园区选择 else df_with_city

    st.subheader("中国地图 · 各地市项目分布")
    city_tooltip_data = _build_城市_园区明细(sub)
    _render_中国地图(sub, city_tooltip_data)

    st.markdown("---")
    st.markdown("### 选择城市查看该地区详情")
    cities = sub["城市"].dropna().unique().tolist()
    cities = [c for c in cities if c != "其他"]
    if not cities:
        cities = ["（暂无已配置区位的城市）"]
    # 点击地图城市后会带 ?selected_city=xxx 刷新页面，此处默认选中该城市
    default_from_url = st.query_params.get("selected_city")
    if default_from_url and default_from_url in cities:
        default_index = cities.index(default_from_url)
    else:
        default_index = 0
    选中城市 = st.selectbox("选择城市", options=cities, index=default_index, key="map_city_select")
    if 选中城市 and 选中城市 != "（暂无已配置区位的城市）":
        city_df = sub[sub["城市"] == 选中城市]
        parks_in_city = city_df["园区"].dropna().unique().tolist()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("该城市项目数", int(city_df["序号"].count()))
        with c2:
            st.metric("该城市金额合计（万元）", f"{city_df['拟定金额'].sum():.0f}")
        with c3:
            st.metric("涉及园区数", len(parks_in_city))
        by_park = city_df.groupby("园区", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        st.markdown("**该城市下各园区统计**")
        st.dataframe(by_park, use_container_width=True, hide_index=True)
        st.markdown("**该城市项目明细**")
        cols_show = [c for c in ["园区", "序号", "项目分级", "专业", "项目名称", "拟定金额", "需求立项", "验收"] if c in city_df.columns]
        st.dataframe(city_df[cols_show], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 图表统计")

    try:
        import plotly.express as px
    except ImportError:
        st.warning("请安装 plotly 以使用美化图表：pip install plotly")
        _render_图表_简易(sub)
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**按专业 · 项目数**")
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_prof.empty:
            fig = px.bar(
                by_prof, x="专业", y="项目数", color="项目数",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="项目数")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        st.markdown("**按项目分级 · 项目数**")
        by_level = sub.groupby("项目分级", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_level.empty:
            colors = (CHART_COLORS_PIE * (1 + len(by_level) // len(CHART_COLORS_PIE)))[: len(by_level)]
            fig = px.pie(
                by_level, values="项目数", names="项目分级", title="",
                color_discrete_sequence=colors, hole=0.35,
            )
            fig.update_traces(
                textposition="outside",
                textinfo="label+percent",
                textfont_size=12,
                pull=[0.02] * len(by_level),
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                margin=dict(t=20, b=60, l=20, r=20),
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**按园区 · 项目数**")
        by_park = sub.groupby("园区", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_park.empty:
            fig = px.bar(
                by_park, x="园区", y="项目数", color="项目数",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="项目数")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c4:
        st.markdown("**按城市 · 项目数**")
        by_city = sub.groupby("城市", dropna=False).agg(项目数=("序号", "count")).reset_index()
        by_city = by_city[by_city["城市"] != "其他"].sort_values("项目数", ascending=False)
        if not by_city.empty:
            fig = px.bar(
                by_city, x="城市", y="项目数", color="项目数",
                color_continuous_scale="Teal", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="项目数")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("**按专业 · 金额合计（万元）**")
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    if not by_prof_m.empty:
        fig = px.bar(
            by_prof_m, x="专业", y="金额", color="金额",
            color_continuous_scale="Viridis", text_auto=".0f",
        )
        fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=360, xaxis_title="", yaxis_title="金额（万元）")
        fig.update_traces(textfont_size=10)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _get_deepseek_client(api_key: str = None):
    """构造 DeepSeek 客户端，优先使用环境变量 DEEPSEEK_API_KEY。"""
    env_key = os.getenv("DEEPSEEK_API_KEY")
    final_key = api_key or env_key
    if not (DEEPSEEK_CLIENT_AVAILABLE and final_key):
        return None
    try:
        return OpenAI(api_key=final_key, base_url="https://api.deepseek.com")
    except Exception:
        return None


def _answer_with_deepseek(api_key: str, question: str, df: pd.DataFrame) -> str:
    """调用 DeepSeek 接口回答使用说明或分析问题。"""
    client = _get_deepseek_client(api_key)
    if client is None:
        return "未检测到可用的 DeepSeek 客户端。请设置环境变量 DEEPSEEK_API_KEY 后重试。"
    cols = list(df.columns)[:30]
    system_prompt = (
        "你是一个面向业务同事的中文 AI 助手，负责解答关于「养老社区改良改造进度管理看板」的使用问题，"
        "并根据已经加载到应用中的 DataFrame 数据给出简单的数据查询建议。用简体中文回答；"
        "若是查找/统计类问题，请说明筛选逻辑并指引用户在哪个 Tab、用哪些筛选器操作。"
        "不要编造不存在的字段名，字段仅限于用户提供的列名。"
    )
    user_prompt = (
        f"用户问题：{question}\n\n当前数据列名：{', '.join(cols)}\n\n"
        "你无法直接访问完整数据，仅能基于列名和业务含义给出操作建议。"
    )
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        return f"调用 DeepSeek 接口失败：{e}"


def _render_ai_assistant_panel(df: pd.DataFrame):
    """在右侧栏中渲染 AI 助手：对话历史 + 输入框。"""
    st.markdown("#### 💬 AI 助手")
    st.caption("解答使用说明、数据查询建议。需设置 DEEPSEEK_API_KEY。")
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if "ai_messages" not in st.session_state:
        st.session_state["ai_messages"] = [
            {"role": "assistant", "content": "你好，我是本看板的 AI 助手，可回答使用问题或帮你构思筛选与查询方式。"}
        ]
    for msg in st.session_state["ai_messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    question = st.chat_input("输入问题…", key="ai_chat_input_sidebar")
    if question:
        st.session_state["ai_messages"].append({"role": "user", "content": question})
        found_df = None
        if "三月" in question and ("立项" in question or "需求立项" in question) and "需求立项" in df.columns:
            dt = pd.to_datetime(df["需求立项"], errors="coerce", format="mixed")
            mask = dt.notna() & (dt.dt.month == 3)
            found_df = df.loc[mask].copy()
        with st.chat_message("assistant"):
            answer = _answer_with_deepseek(api_key, question, df)
            st.markdown(answer)
            if found_df is not None:
                if found_df.empty:
                    st.info("当前数据中未找到 3 月份立项的项目。")
                else:
                    st.markdown(f"**共 {len(found_df)} 条 3 月立项（前 50 条）：**")
                    show_cols = [c for c in ["园区", "项目名称", "拟定金额", "需求立项"] if c in found_df.columns]
                    st.dataframe(found_df[show_cols].head(50), use_container_width=True, hide_index=True)
        st.session_state["ai_messages"].append({"role": "assistant", "content": answer})


def _render_project_wizard(df: pd.DataFrame):
    """项目新增 / 修改：平铺表单。新增有必填校验，修改全部选填，只改想改的字段。"""
    from location_config import 园区_TO_区域
    import uuid

    # 原始数据用于展示，补齐列后的数据用于写回数据库
    df_raw = df.copy()
    df_all = _ensure_project_columns(df)

    mode = st.radio("操作类型", ["新增项目", "修改已有项目"], horizontal=True)

    # ---------- 修改已有项目 ----------
    if mode == "修改已有项目":
        st.markdown("### 步骤 1：查找要修改的项目")
        col1, col2 = st.columns(2)
        with col1:
            seq_input = st.text_input("按序号查找（可选）", value="", placeholder="例如：12")
        with col2:
            name_kw = st.text_input("按项目名称关键词查找（可选）", value="", placeholder="例如：配电、外立面等")

        target_row = None
        if not seq_input.strip() and not name_kw.strip():
            st.info("请先输入序号或项目名称关键词，然后回车进行查找。")
            return

        candidates = df_raw
        if seq_input.strip():
            # 按整数序号查找，支持输入 1 而不是 1.0
            try:
                seq_val = int(float(seq_input.strip()))
                candidates = candidates[pd.to_numeric(candidates["序号"], errors="coerce") == seq_val]
            except ValueError:
                candidates = candidates.iloc[0:0]  # 无效输入直接返回空结果
        if name_kw.strip():
            candidates = candidates[candidates["项目名称"].astype(str).str.contains(name_kw.strip(), na=False)]

        if candidates.empty:
            st.info('未找到匹配项目，可切换到「新增项目」，或调整查找条件。')
            return

        st.caption(f"找到 {len(candidates)} 条记录，请选择一条进行修改：")
        display_cols = ["序号", "园区", "项目名称", "项目分级", "拟定金额"]
        display_cols = [c for c in display_cols if c in candidates.columns]
        st.dataframe(candidates[display_cols].head(50), use_container_width=True, hide_index=True)

        seq_choices = sorted(candidates["序号"].dropna().astype(int).unique().tolist())
        chosen_seq = st.selectbox("选择要修改的项目序号", options=seq_choices)
        target_row = df_all[df_all["序号"].astype(int) == int(chosen_seq)].iloc[0]

        st.markdown("---")
        st.markdown(f"### 步骤 2：编辑项目（序号 {int(target_row['序号'])}）")

        with st.form("edit_project_form"):
            # 基本信息
            st.markdown("**基础信息**")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.text_input("序号（自动）", value=str(int(target_row["序号"])), disabled=True)
                园区_options = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
                园区默认 = str(target_row.get("园区", ""))
                园区 = st.selectbox(
                    "园区（选填）",
                    options=[""] + 园区_options,
                    index=(园区_options.index(园区默认) + 1) if 园区默认 in 园区_options else 0,
                )
            with c2:
                所属区域 = st.text_input("所属区域（选填）", value=str(target_row.get("所属区域", "")))
                城市 = st.text_input("所在城市（选填）", value=str(target_row.get("城市", "")))
            with c3:
                所属业态 = st.text_input("所属业态（选填）", value=str(target_row.get("所属业态", "")))

            # 项目属性
            st.markdown("**项目属性**")
            c4, c5, c6 = st.columns(3)
            with c4:
                项目分级 = st.text_input("项目分级（选填）", value=str(target_row.get("项目分级", "")))
            with c5:
                项目分类 = st.text_input("项目分类（选填）", value=str(target_row.get("项目分类", "")))
            with c6:
                拟定承建组织 = st.text_input("拟定承建组织（选填）", value=str(target_row.get("拟定承建组织", "")))

            c7, c8 = st.columns(2)
            with c7:
                总部重点关注项目 = st.text_input("总部重点关注项目（选填）", value=str(target_row.get("总部重点关注项目", "")))
            with c8:
                拟定金额 = st.number_input("拟定金额（万元，选填）", min_value=0.0, value=float(target_row.get("拟定金额") or 0.0), step=1.0)

            # 专业与名称
            st.markdown("**专业与名称**")
            c9, c10 = st.columns(2)
            with c9:
                专业 = st.text_input("专业（选填）", value=str(target_row.get("专业", "")))
            with c10:
                专业分包 = st.text_input("专业分包（选填）", value=str(target_row.get("专业分包", "")))
            项目名称 = st.text_input("项目名称（选填）", value=str(target_row.get("项目名称", "")))
            备注说明 = st.text_area("备注说明（选填）", value=str(target_row.get("备注说明", "")))

            # 各节点日期
            st.markdown("**项目节点日期（全部选填）**")
            date_values = {}
            for col in TIMELINE_COLS:
                if col not in df_all.columns:
                    continue
                raw_val = target_row.get(col, "")
                date_str = "" if pd.isna(raw_val) else str(raw_val)
                date_values[col] = st.text_input(f"{col}", value=date_str)

            col_save, col_del = st.columns(2)
            with col_save:
                submitted = st.form_submit_button("💾 保存修改")
            with col_del:
                delete_clicked = st.form_submit_button("🗑 删除该项目")

        seq_val = int(target_row["序号"])
        if delete_clicked:
            df_new = df_all[df_all["序号"].astype(int) != seq_val].copy()
            save_to_db(df_new)
            st.success(f"已删除序号为 {seq_val} 的项目。")
            st.rerun()

        if submitted:
            df_new = df_all.copy()
            mask = df_new["序号"].astype(int) == seq_val
            # 逐字段覆盖（全部字段都视为可选）
            update_dict = {
                "园区":园区,
                "所属区域": 所属区域,
                "城市": 城市,
                "所属业态": 所属业态,
                "项目分级": 项目分级,
                "项目分类": 项目分类,
                "拟定承建组织": 拟定承建组织,
                "总部重点关注项目": 总部重点关注项目,
                "专业": 专业,
                "专业分包": 专业分包,
                "项目名称": 项目名称,
                "备注说明": 备注说明,
                "拟定金额": 拟定金额,
            }
            for col, val in update_dict.items():
                if col in df_new.columns:
                    df_new.loc[mask, col] = val
            for col, val in date_values.items():
                if col in df_new.columns:
                    df_new.loc[mask, col] = val
            save_to_db(df_new)
            st.success("已保存修改。")
            st.rerun()
        return

    # ---------- 新增项目 ----------
    st.markdown("### 新增项目")
    df_all = _ensure_project_columns(df_all)
    next_seq = _get_next_序号(df_all)
    required_fields = ["园区", "所属业态", "项目分级", "项目分类", "拟定承建组织", "专业", "项目名称"]

    with st.form("add_project_form"):
        st.caption(f"新项目序号将自动设置为：{next_seq}")

        c1, c2, c3 = st.columns(3)
        with c1:
            parks = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
            园区 = st.selectbox("园区（必填）", options=[""] + parks)
        with c2:
            所属区域 = st.text_input("所属区域（选填）")
        with c3:
            城市 = st.text_input("所在城市（选填）")

        c4, c5, c6 = st.columns(3)
        with c4:
            所属业态 = st.text_input("所属业态（必填，例如：独立 / 护理等）")
        with c5:
            项目分级 = st.text_input("项目分级（必填，例如：一级/二级/三级）")
        with c6:
            项目分类 = st.text_input("项目分类（必填，例如：品质提升 / 大修等）")

        c7, c8 = st.columns(2)
        with c7:
            拟定承建组织 = st.text_input("拟定承建组织（必填，例如：项目部 / 社区分包）")
        with c8:
            总部重点关注项目 = st.text_input("总部重点关注项目（选填）")

        c9, c10 = st.columns(2)
        with c9:
            专业 = st.text_input("专业（必填，例如：土建设施 / 供配电等）")
        with c10:
            专业分包 = st.text_input("专业分包（选填，例如：土建-结构）")

        项目名称 = st.text_input("项目名称（必填）")
        备注说明 = st.text_area("备注说明（选填）")
        拟定金额 = st.number_input("拟定金额（万元，选填）", min_value=0.0, value=0.0, step=1.0)

        st.markdown("**项目节点日期（全部选填）**")
        date_values = {}
        for col in TIMELINE_COLS:
            date_values[col] = st.text_input(f"{col}", value="")

        submitted = st.form_submit_button("✅ 完成并写入数据库")

    if submitted:
        form_dict = {
            "序号": next_seq,
            "园区":园区,
            "所属区域": 所属区域,
            "城市": 城市,
            "所属业态": 所属业态,
            "项目分级": 项目分级,
            "项目分类": 项目分类,
            "拟定承建组织": 拟定承建组织,
            "总部重点关注项目": 总部重点关注项目,
            "专业": 专业,
            "专业分包": 专业分包,
            "项目名称": 项目名称,
            "备注说明": 备注说明,
            "拟定金额": 拟定金额,
        }
        # 校验必填字段
        missing = [k for k in required_fields if not str(form_dict.get(k, "")).strip()]
        if missing:
            st.error(f"以下字段为必填：{', '.join(missing)}")
            return

        # 自动根据园区填充区域和城市（若为空）
        if not form_dict["所属区域"] and 园区 in 园区_TO_区域:
            form_dict["所属区域"] = 园区_TO_区域[园区]
        if not form_dict["城市"] and 园区 in 园区_TO_城市:
            form_dict["城市"] = 园区_TO_城市[园区]

        token = str(uuid.uuid4())
        form_dict["上传凭证"] = token
        for col, val in date_values.items():
            form_dict[col] = val

        df_new_row = pd.DataFrame([form_dict])
        df_all2 = pd.concat([df_all, df_new_row], ignore_index=True)
        save_to_db(df_all2)
        st.success(f"已写入数据库。上传凭证：{token}")
        st.info("请截图或记录该凭证号，后续如需确认或审计可用于检索。")
        st.rerun()


def main():
    # 启动时自动用内嵌 .enc 初始化数据库（若为空）
    _ensure_default_data_seeded()

    _require_feishu_login()

    st.title("养老社区改良改造进度管理看板")
    st.caption("需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收")

    # 侧边栏：登录用户 + 数据源
    with st.sidebar:
        if FEISHU_LOGIN_ENABLED and st.session_state.get("feishu_user"):
            u = st.session_state.feishu_user
            st.caption(f"👤 {u.get('name') or u.get('open_id', '')}")
            if st.button("退出登录", key="feishu_logout"):
                st.session_state.feishu_user = None
                st.rerun()
        st.header("数据源")
        df_db = load_from_db()
        bundled = DEFAULT_BUNDLED_CSV
        if not bundled.exists():
            bundled = ROOT_DIR / "改良改造报表-V4-sample.csv"  # 兼容未加密的明文
        if df_db.empty and bundled.exists():
            try:
                df = load_single_csv(str(bundled))
                if not df.empty:
                    st.success(f"已加载内嵌默认数据，共 {len(df)} 条。点击保存可写入数据库。")
            except Exception as e:
                st.warning(f"默认数据加载失败：{e}")
                df = pd.DataFrame()
        elif not df_db.empty:
            df = df_db
            st.success(f"已从数据库加载，共 {len(df)} 条记录。")
        else:
            df = pd.DataFrame()
            st.info("暂无数据，请将 改良改造报表-V4-sample.csv.enc 放在项目根目录。")

        if not df.empty:
            parks = df["园区"].dropna().unique().tolist()
            园区选择 = st.multiselect("筛选园区", options=parks, default=parks)
        else:
            园区选择 = []

    if df.empty:
        st.warning("暂无数据。请将 改良改造报表-V4-sample.csv.enc 放在项目根目录。")
        render_审核流程说明()
        return

    # 先做列名/列顺序规范化：合并 所在城市→城市、去重、固定顺序，避免后面列显示错位或为空
    df = _canonicalize_df(df)
    # 再补齐可能缺失的关键列
    df = _ensure_project_columns(df)

    # 若专业、项目名称等关键列几乎全空，提示重新导入 CSV 修复列对齐
    if not df.empty and len(df) > 10:
        has_prof = "专业" in df.columns and df["专业"].astype(str).str.strip().str.len().gt(0).sum() > len(df) // 2
        has_name = "项目名称" in df.columns and df["项目名称"].astype(str).str.strip().str.len().gt(0).sum() > len(df) // 2
        if not has_prof or not has_name:
            st.warning("当前数据中「专业」「项目名称」等列多为空，可能是旧库列对齐问题。请用侧边栏「上传文件（覆盖数据库）」重新上传 **改良改造报表-V4.csv** 并保存，即可修复显示。")

    # 顶部标题 + AI 助手悬浮按钮
    header_col, ai_col = st.columns([6, 1])
    with header_col:
        st.title("养老社区改良改造进度管理看板")
        st.caption("需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收")
    with ai_col:
        pop = st.popover("💬 AI 助手", help="点此打开悬浮对话窗，随时询问看板使用与数据分析建议。")
        with pop:
            _render_ai_assistant_panel(df)

    render_审核流程说明()
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["地图与统计", "各园区分级分类", "总部视图", "全部项目", "新增 / 修改项目"])
    with tab1:
        render_地图与统计(df, 园区选择)
    with tab2:
        render_园区分级分类(df, 园区选择)
    with tab3:
        render_总部视图(df, 园区选择)
    with tab4:
        st.subheader("全部项目清单（可在线编辑）")
        st.caption(f"共 {len(df)} 条项目。可在下表中直接增删改，点击下方按钮保存到数据库。")
        # 按统一顺序只展示核心业务字段，避免列顺序错乱
        base_order = [
            "序号", "园区", "所属区域", "城市", "所属业态",
            "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
            "专业", "专业分包", "项目名称", "备注说明", "拟定金额",
        ]
        timeline_cols = [c for c in TIMELINE_COLS if c in df.columns]
        extra_cols = ["上传凭证"] if "上传凭证" in df.columns else []
        ordered_cols = [c for c in base_order + timeline_cols + extra_cols if c in df.columns]
        df_edit = df[ordered_cols].copy()
        edited_df = st.data_editor(
            df_edit,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            key="projects_editor",
        )
        if st.button("💾 保存所有更改到数据库（团队共享）", type="primary", key="save_editor"):
            save_to_db(edited_df)
            st.success("已保存到 SQLite 数据库。其他用户刷新页面后将看到最新数据。")
    with tab5:
        st.subheader("项目录入 / 修改向导")
        st.caption("按步骤逐条填写项目数据，自动生成所属区域、城市与上传凭证。")
        _render_project_wizard(df)


if __name__ == "__main__":
    main()
