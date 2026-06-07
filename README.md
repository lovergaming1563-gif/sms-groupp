# SMS-GROUP

Project for scanning Firebase sources and forwarding SMS to Telegram.

## Setup
1. Configure `config.json` (see `DEPLOYMENT.md`).
2. Add Firebase sources to `firebase_sources.json`.
3. Add device IDs to source-specific device files (e.g., `firebase1_devices.txt`).
4. Install requirements: `pip install -r requirements.txt`.
5. Run the bot: `python bot.py`.
