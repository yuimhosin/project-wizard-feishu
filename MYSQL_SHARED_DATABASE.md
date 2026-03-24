# 与 elderly-dashboard 共用 MySQL 数据库

[elderly-dashboard](https://github.com/yuimhosin/elderly-dashboard) 与 [project-wizard-feishu](https://github.com/yuimhosin/project-wizard-feishu) 使用同一份 `app203.py` 数据层逻辑：表名固定为 **`projects`**，通过环境变量指向**同一 MySQL 库**即可实现双应用读写同一套团队数据。

## 1. MySQL 侧准备

**推荐：直接执行仓库根目录下的 `mysql_setup_shared_projects.sql`**（用 `root` 或有 `CREATE USER` 权限的账号登录后 `source` 或重定向执行）。脚本会：

1. 创建数据库（示例名 `elderly_projects`），字符集 **`utf8mb4`**。
2. 创建专用账号（默认 `app203_shared`），并授予对该库的 **`ALL PRIVILEGES`**（含建表/删表/整表覆盖写，满足 `to_sql(..., if_exists="replace")`）。

执行前请编辑脚本里的 **密码**（以及按需把 `'%'` 改成 `'localhost'` 等主机限制）。

手工操作时等价于：

1. 创建数据库（示例名 `elderly_projects`），字符集 **`utf8mb4`**。
2. 授予应用账号对该库的 `CREATE`、`SELECT`、`INSERT`、`UPDATE`、`DELETE`、`DROP`（首次建表/覆盖写入需要）等权限。  
   - 首次保存时由 pandas `to_sql(..., if_exists="replace")` 自动建表；若你希望手工建表，需与 CSV 列一致（含中文列名）。

### 「两个项目的人都能改」指什么？

- **数据库层**：两个 Streamlit 应用使用**同一套** `APP203_DATABASE_URL`（或同一套 `MYSQL_*`），即共用 **同一 MySQL 用户 + 同一库**，任一应用里点保存，写的都是同一张 `projects` 表。
- **谁能打开页面、谁能编辑**：仍由你配置的 **飞书登录**（`FEISHU_LOGIN_REQUIRED`）和飞书应用成员决定；需要「谁都能改」时，在飞书里给对应人员开通应用可见/可登录，或（仅内网演示）关闭登录门禁（不推荐对外网）。

## 2. 应用配置（两仓库填相同值）

任选其一：

### 方式 A：一条连接串

在 **Streamlit Cloud** → App → Settings → Secrets，或本机 `.env` / `.streamlit/secrets.toml`：

```toml
APP203_DATABASE_URL = "mysql+pymysql://用户:密码@主机:3306/数据库名?charset=utf8mb4"
```

密码中含特殊字符时，请对用户名/密码做 URL 编码，或改用方式 B。

### 方式 B：分项变量（推荐）

```toml
MYSQL_HOST = "你的主机"
MYSQL_PORT = "3306"
MYSQL_USER = "你的用户"
MYSQL_PASSWORD = "你的密码"
MYSQL_DATABASE = "elderly_projects"
```

未设置 `MYSQL_HOST` 且未设置 `APP203_DATABASE_URL` 时，会回退到本地 **`APP203_DB_PATH`**（默认 `app203_projects.db`）的 SQLite。

## 3. 依赖

确保 `requirements203.txt`（或主 `requirements.txt`）包含：

- `SQLAlchemy>=2.0.0`
- `pymysql>=1.1.0`

## 4. 并发说明

当前实现为「全表读入 → 修改 → **整表覆盖写回**」。多用户同时编辑时，后保存的一方会覆盖先保存的变更。若需高并发行级更新，需要改为按主键 `UPDATE` 或引入乐观锁（后续可迭代）。

## 5. 同步到 GitHub

将上述 `app203.py` 中数据库相关代码与 `env_example_app203.txt` 的 MySQL 段落**同步到两个仓库**后，分别在 Streamlit Cloud 里配置**相同** Secrets，即可共用数据库。
