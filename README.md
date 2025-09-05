# Booking Semeru Bot

Panduan setup dan deploy bot Telegram untuk "war tiket" menggunakan VPS Ubuntu.

## Persiapan VPS

1. **Perbarui paket dan pasang utilitas dasar**
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install -y git python3-venv python3-pip ufw
   ```
2. **Set zona waktu ke Asia/Jakarta**
   ```bash
   sudo timedatectl set-timezone Asia/Jakarta
   ```
3. **Amankan SSH dan firewall (opsional tetapi disarankan)**
   - Ganti port SSH, nonaktifkan login root/password.
   - Aktifkan firewall minimal:
     ```bash
     sudo ufw allow OpenSSH
     sudo ufw enable
     ```
4. **Optimasi jaringan untuk latensi rendah**
   ```bash
   echo 'net.core.default_qdisc=fq' | sudo tee -a /etc/sysctl.conf
   echo 'net.ipv4.tcp_congestion_control=bbr' | sudo tee -a /etc/sysctl.conf
   sudo sysctl -p
   ```
5. **Tingkatkan batas file descriptor**
   ```bash
   echo '* soft nofile 1048576' | sudo tee -a /etc/security/limits.conf
   echo '* hard nofile 1048576' | sudo tee -a /etc/security/limits.conf
   ```

## Deploy Bot

1. **Kloning repositori dan masuk ke foldernya**
   ```bash
   git clone https://github.com/yourusername/booking-semeru.git
   cd booking-semeru
   ```
2. **Jalankan installer**
   ```bash
   bash install.sh
   ```
   Skrip membuat virtual environment, memasang dependensi `requirements.txt`, membuat contoh `.env`, dan menyiapkan service `semeru-bot`.
3. **Isi kredensial bot**
   ```bash
   nano .env
   ```
   Perbarui `TELEGRAM_BOT_TOKEN` atau variabel lain sesuai kebutuhan.
4. **Kelola service bot**
   - Mulai ulang bot:
     ```bash
     sudo systemctl restart semeru-bot
     ```
   - Lihat log real-time:
     ```bash
     sudo journalctl -u semeru-bot -f
     ```
   Service akan otomatis aktif kembali setelah VPS direboot.

## Monitoring Latensi

Latensi ke `bromotenggersemeru.id` penting selama jendela 15:55–16:15 WIB.

### Menggunakan skrip mandiri
```bash
python monitor_latency.py &
```
Skrip mencatat `latency.log` setiap 5 detik hanya pada jam rawan.

### Menggunakan perintah Telegram
- Jalankan perintah `/monitor_latency` dari grup Telegram tempat bot berada.
- Bot hanya menerima perintah ini di grup dan akan mengirim hasilnya ke grup yang sama.

## Tips Tambahan
- Login ke akun pemesanan lebih awal untuk "pemanasan" koneksi.
- Gunakan `logrotate` atau mekanisme serupa untuk mengelola ukuran log.
- Pastikan sumber daya VPS (CPU/RAM) cukup dan lokasi server dekat dengan server tujuan.
