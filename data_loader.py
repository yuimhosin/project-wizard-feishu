# -*- coding: utf-8 -*-
"""养老社区改良改造进度表 CSV/XLSX 解析与多园区数据加载。"""
from pathlib import Path
import pandas as pd

# 表头第二行（时间节点列名）
TIMELINE_COLS = [
    "需求立项", "需求审核", "规划设计方案", "成本核算", "项目决策",
    "招采", "实施", "验收", "结算"
]
# 验收列多种写法统一为「验收」
TIMELINE_COL_MAP = {"验收(社区需求完成交付)": "验收", "验收(社区结算)": "验收"}
# 进度表关键列，用于自动识别是否为有效分表
KEY_COLS = ("序号", "项目分级", "专业", "拟定金额", "项目名称")
PARK_TOKENS = ["燕园", "蜀园", "吴园", "粤园", "申园", "楚园", "鹭园", "大清谷", "湘园", "沈园", "桂园", "琴园",
               "赣园", "苏园", "甬园", "豫园", "渝园", "徽园", "鹏园", "瓯园", "福园", "儒园", "津园", "滇园"]


def _read_first_two_lines(path: str):
    """读取前两行，优先 utf-8-sig，失败则尝试 gbk。"""
    for enc in ("utf-8-sig", "utf-8", "gbk", "gb2312"):
        try:
            with open(path, "r", encoding=enc) as f:
                line0 = f.readline()
                line1 = f.readline()
            return line0.strip().split(","), line1.strip().split(","), enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError("无法识别文件编码，请另存为 UTF-8 或 GBK 的 CSV")

def _parse_header(path: str):
    """读取前两行，合并为列名。返回 (names, encoding)。支持第一行 9 或 10 列、第二行时间节点从第 8 或第 9 列开始。"""
    line0, line1, enc = _read_first_two_lines(path)
    line0 = [str(x).strip().strip("\ufeff") for x in line0]
    line1 = [str(x).strip() for x in line1]
    # 第一行：取前 9 列（序号～拟定承建组），若只有 8 列则全取
    n_first = min(9, len(line0)) if len(line0) >= 8 else len(line0)
    part1 = line0[:n_first] if n_first else line0
    # 第二行：时间节点可能在 index 8 或 9 开始，取 9 个
    n_time = len(TIMELINE_COLS)
    start = 8 if len(line1) >= 8 + n_time else (9 if len(line1) >= 9 + n_time else max(0, len(line1) - n_time))
    part2 = line1[start : start + n_time]
    while len(part2) < n_time:
        part2.append("")
    names = part1 + part2[:n_time]
    return names, enc


def _normalize_timeline_col(name: str) -> str:
    n = (name or "").strip()
    return TIMELINE_COL_MAP.get(n, n) if n else ""


def _parse_header_from_rows(row0, row1, n_time: int = 9):
    """从两行（列表或 Series）解析列名，与 CSV 表头逻辑一致。"""
    line0 = [str(x).strip().strip("\ufeff") for x in row0]
    line1 = [str(x).strip() for x in row1]
    n_first = min(9, len(line0)) if len(line0) >= 8 else len(line0)
    part1 = line0[:n_first] if n_first else line0
    start = 8 if len(line1) >= 8 + n_time else (9 if len(line1) >= 9 + n_time else max(0, len(line1) - n_time))
    part2 = line1[start : start + n_time]
    while len(part2) < n_time:
        part2.append("")
    return part1 + part2[:n_time]


def _is_progress_sheet(names: list) -> bool:
    """根据列名判断是否为进度表分表（含序号、项目分级/专业/拟定金额等）。"""
    names_set = {str(x).strip() for x in names if x}
    has_序号 = "序号" in names_set or "编号" in names_set
    has_key = any(k in names_set for k in ["项目分级", "专业", "拟定金额", "项目名称"])
    return has_序号 and has_key


def _normalize_loaded_df(df: pd.DataFrame, 园区名: str = None, default_园区_from: str = "") -> pd.DataFrame:
    """
    对已设好列名的 DataFrame 做统一规范化：验收列简称、拟定承建组织、序号过滤、合计行、拟定金额、园区。
    default_园区_from 用于从文件名或 sheet 名解析园区（如含「燕园」）。
    """
    if df.empty:
        return df
    names = list(df.columns)
    for i, n in enumerate(names):
        if n and "验收" in str(n) and "社区" in str(n):
            names[i] = "验收"
    names = [str(x).strip().strip("\ufeff") for x in names]
    df.columns = names
    if "拟定承建组" in df.columns and "拟定承建组织" not in df.columns:
        df = df.rename(columns={"拟定承建组": "拟定承建组织"})
    # 序号列
    序号列 = None
    for c in ["序号", "编号"]:
        if c in df.columns:
            序号列 = df[c]
            break
    if 序号列 is None and len(df.columns) > 0:
        序号列 = df.iloc[:, 0]
    if 序号列 is not None:
        s = 序号列.astype(str).str.strip()
        digit_ok = s.str.match(r"^\d+$", na=False)
        numeric_ok = pd.to_numeric(序号列, errors="coerce").notna()
        valid = digit_ok | numeric_ok
        df = df.loc[valid].copy()
    序号列名 = 序号列.name if 序号列 is not None else None
    if 序号列名 and 序号列名 in df.columns:
        df = df.loc[~df[序号列名].astype(str).str.strip().str.match(r"^(合计|差额|小计|合计行)", na=False)].copy()
    if "拟定金额" in df.columns:
        df["拟定金额"] = pd.to_numeric(df["拟定金额"], errors="coerce").fillna(0).astype(int)
    # 检查是否有"社区"列，如果有则重命名为"园区"
    if "社区" in df.columns and "园区" not in df.columns:
        df = df.rename(columns={"社区": "园区"})
    
    # 添加"专业分包"作为"专业细分"的别名（如果存在专业细分列）
    if "专业细分" in df.columns and "专业分包" not in df.columns:
        df["专业分包"] = df["专业细分"]
    elif "专业分包" in df.columns and "专业细分" not in df.columns:
        df["专业细分"] = df["专业分包"]
    
    # 若源数据中已经包含「园区」列，则优先保留，不再用文件名/表名覆盖
    if "园区" not in df.columns:
        if 园区名:
            df["园区"] = 园区名
        else:
            # 尝试从文件名/表名中识别园区token
            found_park = None
            for token in PARK_TOKENS:
                if token in default_园区_from:
                    found_park = token
                    break
            if found_park:
                df["园区"] = found_park
            else:
                # 如果无法识别，尝试从数据中提取园区信息
                # 检查是否有其他列可能包含园区信息（如第一列、项目名称等）
                park_from_data = None
                # 尝试从项目名称中提取园区名
                if "项目名称" in df.columns:
                    for _, row in df.head(10).iterrows():
                        proj_name = str(row.get("项目名称", ""))
                        for token in PARK_TOKENS:
                            if token in proj_name:
                                park_from_data = token
                                break
                        if park_from_data:
                            break
                
                if park_from_data:
                    df["园区"] = park_from_data
                else:
                    # 如果仍然无法识别，使用"未知园区"而不是None，避免后续筛选问题
                    df["园区"] = "未知园区"
    return df


def _load_flat_progress_csv(path: Path) -> pd.DataFrame:
    """
    加载已预处理好的长表 CSV（如工作簿2.csv、改良改造报表-V4.csv）。
    支持两种情况：
    1. 第一行为列名（包含"序号"、"园区"、"社区"等关键词）
    2. 第一行就是数据（没有列名，需要按位置识别）
    3. 特殊处理：改良改造报表-V4.csv 格式（列名可能乱码，需要按位置映射）
    """
    # 复用编码探测逻辑
    line0, line1, encoding = _read_first_two_lines(str(path))
    line0 = [str(x).strip().strip("\ufeff") for x in line0]
    line1 = [str(x).strip() for x in line1] if line1 else []
    
    # 特殊处理：改良改造报表-V4.csv 格式
    is_new_format = path.name == "改良改造报表-V4.csv"
    
    if is_new_format:
        # 改良改造报表-V4.csv：表头两行后为数据，严格按列位置读取前 14 列，避免多列/错位导致后面列读不出
        # 源表顺序：序号,社区,所属区域,所在城市,所属业态,项目分级,项目分类,拟定承建组织,总部重点关注项目,专业,专业分包,项目名称,备注说明,拟定金额
        CANONICAL_14 = [
            "序号", "社区", "所属区域", "城市", "所属业态",
            "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
            "专业", "专业分包", "项目名称", "备注说明", "拟定金额",
        ]
        df = pd.read_csv(
            path, header=None, skiprows=2, encoding=encoding,
            low_memory=False, dtype=str, keep_default_na=False,
        )
        # 只保留前 14 列，按位置赋列名，不依赖 CSV 列数
        n = min(14, df.shape[1])
        df = df.iloc[:, :n].copy()
        df.columns = CANONICAL_14[:n]
        if n < 14:
            for c in CANONICAL_14[n:]:
                df[c] = ""
        # 拟定金额、序号转数值；社区→园区等
        df = _normalize_loaded_df(df, 园区名=None, default_园区_from=path.stem)
    else:
        # 原有格式处理
        # 检查第一行是否是列名：如果包含"序号"、"园区"、"社区"等关键词，则认为是列名
        has_header = False
        header_keywords = ["序号", "园区", "社区", "项目名称", "项目分级", "专业", "拟定金额"]
        for cell in line0:
            if any(keyword in str(cell) for keyword in header_keywords):
                has_header = True
                break
        
        if has_header:
            # 第一行是列名
            header = line0
            df = pd.read_csv(path, header=None, skiprows=1, encoding=encoding)
        else:
            # 第一行是数据，没有列名，需要根据位置推断
            # 尝试读取前几行来判断列数
            df = pd.read_csv(path, header=None, encoding=encoding)
            # 根据常见格式推断列名：通常第1列是园区/社区，第2列是序号等
            num_cols = df.shape[1]
            header = []
            for i in range(num_cols):
                # 检查第一行第i列的值，尝试推断列名
                first_val = str(df.iloc[0, i]).strip() if len(df) > 0 else ""
                # 如果第一列的值看起来像园区名（包含园区token），则认为是园区列
                if i == 0 and any(token in first_val for token in PARK_TOKENS):
                    header.append("园区")
                elif i == 0 and first_val and not first_val.isdigit():
                    # 第一列可能是园区/社区
                    header.append("园区")
                elif i == 1 and (first_val.isdigit() or first_val == ""):
                    # 第二列可能是序号
                    header.append("序号")
                else:
                    # 其他列使用通用名称
                    header.append(f"列{i+1}")
        
        # 对齐列数
        if df.shape[1] > len(header):
            df = df.iloc[:, : len(header)]
        elif df.shape[1] < len(header):
            for j in range(df.shape[1], len(header)):
                df[j] = ""
        
        df.columns = header[:df.shape[1]]
    
    # 检查是否有"社区"列，如果有则重命名为"园区"
    if "社区" in df.columns and "园区" not in df.columns:
        df = df.rename(columns={"社区": "园区"})
    
    # 如果第一列没有列名但包含园区信息，尝试识别
    if "园区" not in df.columns and df.shape[1] > 0:
        first_col = df.iloc[:, 0]
        # 检查第一列是否包含园区名
        sample_values = first_col.head(10).astype(str).tolist()
        has_park_name = any(any(token in val for token in PARK_TOKENS) for val in sample_values)
        if has_park_name:
            # 重命名第一列为园区
            df = df.rename(columns={df.columns[0]: "园区"})
    
    # 正常规范化（此时通常已经有「园区」列，不会被覆盖）
    return _normalize_loaded_df(df, 园区名=None, default_园区_from=path.stem)


def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """确保列名唯一，避免 pd.concat 时报 InvalidIndexError。重复列名依次加后缀 _2, _3..."""
    if df.columns.is_unique:
        return df
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for i, c in enumerate(cols):
        name = (c or "").strip()
        key = name if name else f"__empty_{i}"
        if key not in seen:
            seen[key] = 0
            new_cols.append(c if (c and str(c).strip()) else f"Unnamed_{i}")
        else:
            seen[key] += 1
            suffix = seen[key] + 1
            new_cols.append(f"{name}_{suffix}" if name else f"Unnamed_{i}_{suffix}")
    out = df.copy()
    out.columns = new_cols
    return out


def load_single_csv(path: str, 园区名: str = None) -> pd.DataFrame:
    """
    加载单张进度表 CSV。
    表头为两行：第一行为基础信息，第二行为时间节点（需求立项、需求审核等）。
    若未传入 园区名，则从文件名中尝试解析（如含「燕园」则取燕园）。
    """
    path = Path(path)
    if not path.suffix.lower() == ".csv":
        raise ValueError("仅支持 .csv 文件")
    # 特殊处理：工作簿2.csv、改良改造报表-V4.csv 等已预处理好的长表 CSV，第一行即为完整表头
    if path.name == "工作簿2.csv" or path.name == "改良改造报表-V4.csv":
        return _load_flat_progress_csv(path)
    names, encoding = _parse_header(str(path))
    # 时间节点列统一为简称（含「验收(社区结算)」「验收(社区需求完成交付)」等）
    for i, n in enumerate(names):
        if n and "验收" in n and "社区" in n:
            names[i] = "验收"
    # 列名去 BOM、首尾空格，便于匹配「序号」
    names = [str(x).strip().strip("\ufeff") for x in names]
    # 列数对齐：CSV 可能有多余逗号
    df = pd.read_csv(path, header=None, skiprows=2, encoding=encoding)
    if df.shape[1] > len(names):
        df = df.iloc[:, : len(names)]
    elif df.shape[1] < len(names):
        for j in range(df.shape[1], len(names)):
            df[j] = ""
    df.columns = names
    return _normalize_loaded_df(df, 园区名=园区名, default_园区_from=path.stem)


def load_single_xlsx(path: str, 园区名: str = None) -> pd.DataFrame:
    """
    加载 XLSX：读取所有工作表，自动识别进度表分表（含序号、项目分级/专业/拟定金额等），
    支持两行表头，按 sheet 名或文件名解析园区并合并为一张表。
    """
    path = Path(path)
    if path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError("仅支持 .xlsx / .xls 文件")
    engine = "openpyxl" if path.suffix.lower() == ".xlsx" else None
    if engine == "openpyxl":
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            raise ImportError("请先安装 openpyxl：pip install openpyxl")
    # 读取所有 sheet（header=None 便于自己解析两行表头）
    xl = pd.ExcelFile(path, engine=engine)
    frames = []
    for sheet_name in xl.sheet_names:
        try:
            raw = pd.read_excel(path, sheet_name=sheet_name, header=None, engine=engine)
        except Exception:
            continue
        if raw.empty or raw.shape[0] < 3:
            continue
        row0 = raw.iloc[0].tolist()
        row1 = raw.iloc[1].tolist()
        names = _parse_header_from_rows(row0, row1)
        if not _is_progress_sheet(names):
            continue
        data = raw.iloc[2:].copy()
        if data.shape[1] > len(names):
            data = data.iloc[:, : len(names)]
        elif data.shape[1] < len(names):
            for j in range(data.shape[1], len(names)):
                data[j] = ""
        data.columns = names
        df_sheet = _normalize_loaded_df(data.copy(), 园区名=园区名, default_园区_from=sheet_name)
        if not df_sheet.empty:
            frames.append(df_sheet)
    if not frames:
        # 若所有 sheet 都未识别为进度表，尝试把第一个 sheet 当单表（两行表头）
        try:
            raw = pd.read_excel(path, sheet_name=0, header=None, engine=engine)
            if raw.shape[0] >= 3:
                row0 = raw.iloc[0].tolist()
                row1 = raw.iloc[1].tolist()
                names = _parse_header_from_rows(row0, row1)
                data = raw.iloc[2:].copy()
                data.columns = names
                if data.shape[1] > len(names):
                    data = data.iloc[:, : len(names)]
                df_one = _normalize_loaded_df(data.copy(), 园区名=园区名, default_园区_from=path.stem)
                if not df_one.empty:
                    return df_one
        except Exception:
            pass
        return pd.DataFrame()
    # 合并前确保每个表列名唯一，否则 pd.concat 会报 InvalidIndexError
    frames = [_ensure_unique_columns(f) for f in frames]
    return pd.concat(frames, ignore_index=True)


def load_uploaded(path: str, filename: str = "", 园区名: str = None) -> pd.DataFrame:
    """根据扩展名自动选择 CSV 或 XLSX 加载。filename 用于解析园区（如含燕园/蜀园）。"""
    path = Path(path)
    suffix = path.suffix.lower()
    name_for_park = filename or path.stem
    if suffix == ".csv":
        return load_single_csv(str(path), 园区名=园区名)
    if suffix in (".xlsx", ".xls"):
        return load_single_xlsx(str(path), 园区名=园区名)
    raise ValueError(f"不支持的文件格式：{suffix}，请上传 .csv 或 .xlsx")


def load_from_directory(dir_path: str, pattern: str = "*.csv") -> pd.DataFrame:
    """从目录加载所有匹配的 CSV，合并为多园区一张表。"""
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        return pd.DataFrame()
    frames = []
    for f in dir_path.glob(pattern):
        try:
            df = load_single_csv(str(f))
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def get_稳定需求_mask(df: pd.DataFrame) -> pd.Series:
    """
    稳定需求：需求已立项（需求立项日期有效）且非无效日期。
    无效日期示例：1900-01-06、空、-14 等。
    """
    # 兼容列名中包含“需求立项”的各种写法（如“需求立项日期”）
    col = None
    for c in df.columns:
        if "需求立项" in str(c):
            col = c
            break
    if col is None:
        # 若表中无相关列，则无法区分是否已立项，这里统一视为“未立项”（False）；
        # 上层逻辑可自行决定是否当作稳定需求。
        return pd.Series(False, index=df.index)

    s = df[col]
    # 兼容多种填报格式：日期类型、字符串（含时间）、斜杠/短横线等
    dt = pd.to_datetime(s, errors="coerce", format="mixed")
    # 1900 年等 Excel 默认日期视为无效，2000 年之后视为真实立项
    valid = dt.notna() & (dt.dt.year >= 2000)
    return valid
