#!/usr/bin/env bash
set -euo pipefail

# clone repository if needed
REPO_URL=${REPO_URL:-"https://github.com/yourusername/booking-semeru.git"}
REPO_DIR=$(basename "$REPO_URL" .git)

if [ ! -d .git ]; then
    git clone "$REPO_URL"
    cd "$REPO_DIR"
fi

# install required packages
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git

# set up virtual environment
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# create sample .env if missing
if [ ! -f .env ]; then
    cat <<'EOF' > .env
TELEGRAM_BOT_TOKEN=your_token_here
EOF
    echo "Sample .env created. Update it with your actual credentials before starting the service."
fi

# set up systemd service
SERVICE_FILE=/etc/systemd/system/semeru-bot.service
sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Semeru Booking Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
EnvironmentFile=$(pwd)/.env
ExecStart=$(pwd)/venv/bin/python bot-semeru.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable semeru-bot
sudo systemctl restart semeru-bot

echo "Installation complete. The semeru-bot service has been started."
echo "Use 'sudo systemctl restart semeru-bot' to restart the bot." \
     "View logs with 'sudo journalctl -u semeru-bot -f'."
