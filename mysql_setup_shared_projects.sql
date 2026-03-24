-- =============================================================================
-- 共用库初始化：project-wizard-feishu-github 与 elderly-dashboard-main 的 app203
-- 表名：projects（首次在应用内保存时由程序自动创建；本脚本只负责「库 + 账号 + 权限」）
--
-- 使用方式（在能管理 MySQL 的账号下执行，例如 root）：
--   mysql -h 主机 -P 3306 -u root -p < mysql_setup_shared_projects.sql
-- 或在 mysql 客户端里 source 本文件。
--
-- 执行前请修改下面两处：
--   1) 数据库名 ELDERLY_PROJECTS_DB（若改名，两项目的 MYSQL_DATABASE / URL 里库名要一致）
--   2) 账号 APP203_USER 与密码 APP203_PASSWORD（两项目填同一套）
-- =============================================================================

-- 1) 数据库（utf8mb4 与连接串 ?charset=utf8mb4 一致）
CREATE DATABASE IF NOT EXISTS elderly_projects
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

-- 2) 应用专用账号：可从任意主机连接（适合本机、局域网、云主机）
--    若只允许本机，把 '%' 改为 'localhost'。
--    MySQL 8.0.11+ 支持 IF NOT EXISTS；老版本请改为普通 CREATE USER（见文件末尾注释）

CREATE USER IF NOT EXISTS 'app203_shared'@'%' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';

-- 3) 权限：该库下全部对象，满足 pandas to_sql(..., if_exists="replace") 建表/删表/整表写入
GRANT ALL PRIVILEGES ON elderly_projects.* TO 'app203_shared'@'%';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
-- 应用侧（两个项目填完全相同）示例：
--
-- APP203_DATABASE_URL=mysql+pymysql://app203_shared:CHANGE_ME_STRONG_PASSWORD@你的主机:3306/elderly_projects?charset=utf8mb4
--
-- 或分项：
-- MYSQL_HOST=你的主机
-- MYSQL_PORT=3306
-- MYSQL_USER=app203_shared
-- MYSQL_PASSWORD=CHANGE_ME_STRONG_PASSWORD
-- MYSQL_DATABASE=elderly_projects
-- -----------------------------------------------------------------------------
--
-- 兼容 MySQL 5.7（无 IF NOT EXISTS 时）可手工执行：
--   DROP USER IF EXISTS 'app203_shared'@'%';
--   CREATE USER 'app203_shared'@'%' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';
--   GRANT ALL PRIVILEGES ON elderly_projects.* TO 'app203_shared'@'%';
--   FLUSH PRIVILEGES;
