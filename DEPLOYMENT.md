# SMS Monitoring Bot - Deployment Guide

## 1. GitHub Repository Setup
1. Create a **Private** repository on GitHub.
2. Do **not** upload `config.json` or `.env` files.
3. Push the code using the Git commands provided in the setup report.

## 2. Render Deployment (Background Worker)
1. Log in to [Render.com](https://render.com).
2. Click **New +** -> **Blueprint**.
3. Connect your GitHub repository.
4. Render will automatically detect `render.yaml` and configure the service as a **Background Worker**.

## 3. Secret Files Configuration (CRITICAL)
Since `config.json` is excluded from Git for security, you must add it manually to Render:
1. In the Render Dashboard, navigate to your **Service**.
2. Click **Environment**.
3. Scroll down to **Secret Files**.
4. Click **Add Secret File**.
5. **Filename:** `config.json`
6. **Contents:** Paste your local `config.json` content here.
7. Click **Save Changes**.

## 4. Operational Monitoring
- **Logs:** View the "Startup Summary" in the Render Logs tab to verify all device IDs and sources are loaded.
- **Dead Entities:** The bot will automatically create `dead_devices.txt` and `dead_sources.txt` in the container's ephemeral storage if targets fail.
- **Auto-Reload:** Any changes pushed to the `main` branch will trigger an automatic redeploy.

## 5. Environment Variables
- `PYTHON_VERSION`: Set to `3.13.1` (automatically handled by `render.yaml`).
