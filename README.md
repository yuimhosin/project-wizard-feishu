# 养老社区项目向导 + 飞书推送

从「养老社区改良改造进度管理看板」中提取的独立模块，提供：

- **项目新增**：按步骤录入新项目，自动生成所属区域、城市与上传凭证
- **项目修改**：按序号或名称查找项目，编辑并保存
- **项目删除**：删除指定项目
- **飞书推送**：可选在保存时推送变更通知到飞书群（自定义机器人 Webhook）

## 快速开始

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 配置

### 飞书 Webhook

1. 在飞书群中添加「自定义机器人」，复制 Webhook URL
2. 在应用侧边栏「飞书推送」区域粘贴 URL
3. 勾选「保存到数据库时同时推送到飞书」

也可通过环境变量或 Streamlit Secrets 配置：

- `FEISHU_WEBHOOK_URL`
- `.streamlit/secrets.toml` 中的 `FEISHU_WEBHOOK_URL` 或 `feishu_webhook_url`

### 数据存储

默认使用 SQLite 数据库 `app203_projects.db`，可通过环境变量 `APP203_DB_PATH` 修改路径。

## 数据源

- **数据库（团队共享）**：使用 SQLite 中的 projects 表
- **上传文件**：上传 CSV 或 Excel 覆盖数据库
- **目录下全部 CSV**：从指定目录加载并覆盖数据库

首次启动若数据库为空且存在 `改良改造报表-V4.csv`，将自动用其初始化。

## 项目结构

```
.
├── app.py           # 主应用
├── feishu_client.py # 飞书 Webhook 推送
├── data_loader.py   # CSV/XLSX 数据加载
├── location_config.py # 园区-城市/区域映射
├── requirements.txt
└── README.md
```
