# 推送到 GitHub

已完成本地 Git 初始化与首次提交。远程已配置为 `https://github.com/yuimhosin/project-wizard-feishu.git`。

## 1. 在 GitHub 创建新仓库

1. 登录 [GitHub](https://github.com)
2. 点击右上角 **+** → **New repository**
3. 仓库名称填写：**project-wizard-feishu**
4. 选择 **Public**，**不要**勾选 "Add a README"（本地已有）
5. 点击 **Create repository**

## 2. 推送

创建好仓库后，在项目目录执行：

```bash
cd f:\text2sql\project-wizard-feishu
git push -u origin master
```

## 3. 使用 SSH（如已配置）

```bash
git remote add origin git@github.com:YOUR_USERNAME/REPO_NAME.git
git push -u origin main
```
