# Streamlit Cloud 部署说明

## 运行养老社区改良改造看板

1. 在 [Streamlit Cloud](https://share.streamlit.io/) 创建应用，选择 `project-wizard-feishu` 仓库
2. **Main file path** 填写：`streamlit_app.py`
3. 或 **Advanced settings** → **Run command** 填写：`streamlit run streamlit_app.py --server.port 8501`

## 入口文件

- `app.py`：部署入口，重定向到 app203（养老社区看板）
- `streamlit_app.py`：同上，任选其一即可

## 飞书工作台免登

在 Streamlit Cloud Secrets 中配置：
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_REDIRECT_URI`：填 Streamlit Cloud 应用地址，如 `https://xxx.streamlit.app/`
