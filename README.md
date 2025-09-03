# Booking Semeru Bot

Skrip ini men-deploy bot `bot-semeru.py` sebagai service di VPS Ubuntu menggunakan `systemd`.

## Langkah instalasi

1. **Masuk ke VPS dan pasang git**
   ```bash
   sudo apt update && sudo apt install git -y
   ```
2. **Kloning repositori dan masuk ke foldernya**
   ```bash
   git clone https://github.com/yourusername/booking-semeru.git
   cd booking-semeru
   ```
3. **Jalankan installer**
   ```bash
   bash install.sh
   ```
   Skrip akan membuat virtual environment, memasang dependensi dari `requirements.txt`, membuat berkas `.env` contoh, serta menyiapkan service `semeru-bot`.
4. **Isi kredensial bot**
   ```bash
   nano .env
   ```
   Perbarui nilai `TELEGRAM_BOT_TOKEN` atau variabel lain sesuai kebutuhan.
5. **Kelola service**
   - Mulai ulang bot:
     ```bash
     sudo systemctl restart semeru-bot
     ```
   - Lihat log secara real-time:
     ```bash
     sudo journalctl -u semeru-bot -f
     ```

Setelah langkah-langkah di atas, bot berjalan otomatis setiap kali VPS direstart.
