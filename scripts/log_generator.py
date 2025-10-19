import json
import random
import os
from datetime import datetime, timedelta
from multiprocessing import Pool, Value, Lock
from tqdm import tqdm
import sys

# -----------------------------
# Yapılandırmalar
# -----------------------------
NUM_FILES = 10  # Oluşturulacak dosya sayısı (Değiştirilebilir)
LOGS_PER_FILE = 500_000  # Her dosyada üretilecek log satırı sayısı (Değiştirilebilir)

OUTPUT_DIR = "logs"  # Çıktı dizini
BATCH_SIZE = 10_000  # Daha iyi I/O için toplu yazma boyutu

# Endpoint'ler ve trafik olasılıkları (çeşitli trafik)
ENDPOINTS = ["/login", "/checkout", "/pay", "/cart", "/inventory"]
ENDPOINT_PROBS = [0.45, 0.25, 0.12, 0.15, 0.03]  # Login baskın, inventory nadir

SERVICES = ["auth-service", "order-service", "payment-service", "inventory-service"]
REGIONS = ["eu-central", "us-east", "ap-south"]
REGION_PROBS = [0.6, 0.25, 0.15]  # Avrupa daha baskın

# Servis bazlı hata oranları (bazı servisler daha sorunlu)
SERVICE_ERROR_RATES = {
    "auth-service": 0.05,  # %5 hata oranı - stabil
    "order-service": 0.08,  # %8 hata oranı - orta düzeyde sorun
    "payment-service": 0.15,  # %15 hata oranı - problemli
    "inventory-service": 0.03  # %3 hata oranı - çok stabil
}

SERVICE_WARN_RATES = {
    "auth-service": 0.15,  # %15 uyarı
    "order-service": 0.20,  # %20 uyarı
    "payment-service": 0.25,  # %25 uyarı
    "inventory-service": 0.10  # %10 uyarı
}

# Endpoint bazlı yanıt süresi aralıkları (ms)
ENDPOINT_RESPONSE_TIMES = {
    "/login": (50, 300),  # Hızlı kimlik doğrulama
    "/checkout": (200, 1500),  # Orta - iş mantığı
    "/pay": (300, 3000),  # Yavaş - dış ödeme servisi
    "/cart": (80, 400),  # Hızlı - cache ağırlıklı
    "/inventory": (100, 2500)  # Değişken - veritabanı sorguları
}

# Güç yasası dağılımlı IP havuzu (az sayıda yoğun, çok sayıda hafif kullanıcı)
IP_POOL = [f"192.168.1.{i}" for i in range(1, 201)]  # 200 IP'ye genişletildi
# İlk 10 kullanıcı = %40, sonraki 40 = %35, kalan 150 = %25
IP_PROBS = (
        [0.04] * 10 +  # Yoğun kullanıcılar: her biri %4
        [0.00875] * 40 +  # Orta kullanıcılar: her biri %0.875
        [0.00167] * 150  # Hafif kullanıcılar: her biri %0.167
)

ERROR_CODES = ["TOKEN_EXPIRED", "PAYMENT_FAILED", "OUT_OF_STOCK", "DB_TIMEOUT", "NETWORK_ERROR", "RATE_LIMIT"]

# Uyarı detayları ve olasılıkları
WARN_DETAILS = ["İstek tekrar deneniyor", "Yavaş yanıt algılandı", "Cache miss", "Eski API kullanımı"]
WARN_PROBS = [0.5, 0.3, 0.15, 0.05]

STATUS_CODES = [200, 400, 401, 404, 500, 504]
STATUS_PROBS = [0.70, 0.08, 0.05, 0.07, 0.08, 0.02]  # %70 başarı, çeşitli hatalar

# Global sayaç ve kilit
counter = None
counter_lock = None


# -----------------------------
# Helper Functions
# -----------------------------
def init_worker(shared_counter, shared_lock):
    """Her işçi süreci için paylaşılan sayaç ve kilidi başlat"""
    global counter, counter_lock
    counter = shared_counter
    counter_lock = shared_lock


def generate_log(base_timestamp):
    """Gerçekçi zaman damgası varyasyonu ile tek bir log kaydı üret"""
    # Add random microseconds for timestamp variation
    timestamp_offset = timedelta(microseconds=random.randint(0, 999999))
    timestamp = (base_timestamp + timestamp_offset).isoformat() + "Z"

    endpoint = random.choices(ENDPOINTS, ENDPOINT_PROBS)[0]
    service = random.choice(SERVICES)
    region = random.choices(REGIONS, REGION_PROBS)[0]

    # User ID with power-law distribution (some users more active)
    # Top 50 users = 50% of traffic, next 200 = 30%, rest 750 = 20%
    user_dist = random.random()
    if user_dist < 0.50:
        user_id = random.randint(1, 50)  # Heavy users
    elif user_dist < 0.80:
        user_id = random.randint(51, 250)  # Medium users
    else:
        user_id = random.randint(251, 1000)  # Light users

    ip = random.choices(IP_POOL, IP_PROBS)[0]

    # Response time based on endpoint (realistic variance)
    min_time, max_time = ENDPOINT_RESPONSE_TIMES[endpoint]
    # Use log-normal distribution for realistic response times (most fast, some slow)
    mean_time = (min_time + max_time) / 2
    sigma = 0.5  # Standard deviation for log-normal
    response_time = int(random.lognormvariate(
        random.uniform(4, 6),  # Mean in log space
        sigma
    ) % (max_time - min_time) + min_time)

    # Determine level based on service-specific error/warn rates
    error_rate = SERVICE_ERROR_RATES[service]
    warn_rate = SERVICE_WARN_RATES[service]

    rand_val = random.random()
    if rand_val < error_rate:
        level = "ERROR"
        status_code = random.choices([400, 401, 404, 500, 504], [0.15, 0.10, 0.15, 0.50, 0.10])[0]
    elif rand_val < error_rate + warn_rate:
        level = "WARN"
        status_code = random.choices([200, 400, 504], [0.70, 0.20, 0.10])[0]
    else:
        level = "INFO"
        status_code = random.choices([200, 201, 204], [0.90, 0.07, 0.03])[0]

    log = {
        "timestamp": timestamp,
        "service": service,
        "endpoint": endpoint,
        "level": level,
        "user_id": user_id,
        "ip": ip,
        "region": region,
        "response_time": response_time,
        "status_code": status_code
    }

    # Add level-specific fields
    if level == "ERROR":
        # Different errors for different services
        if service == "payment-service":
            error_code = random.choices(
                ["PAYMENT_FAILED", "NETWORK_ERROR", "DB_TIMEOUT"],
                [0.60, 0.25, 0.15]
            )[0]
        elif service == "auth-service":
            error_code = random.choices(
                ["TOKEN_EXPIRED", "RATE_LIMIT", "DB_TIMEOUT"],
                [0.70, 0.20, 0.10]
            )[0]
        else:
            error_code = random.choice(ERROR_CODES)

        log["error_code"] = error_code
        log["message"] = f"Error occurred: {error_code}"
    elif level == "WARN":
        num_details = random.randint(1, 2)
        log["warn_detail"] = random.choices(WARN_DETAILS, weights=WARN_PROBS, k=num_details)
        log["message"] = ", ".join(log["warn_detail"])
    else:  # INFO
        log["message"] = "Request processed successfully"

    return log


def generate_batch(batch_size, base_timestamp):
    """Bir log kümesi (batch) üret"""
    return [generate_log(base_timestamp) for _ in range(batch_size)]


def generate_file(file_index):
    """Toplu yazma ile tek bir log dosyası üret"""
    global counter, counter_lock

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = os.path.join(OUTPUT_DIR, f"logs_{file_index:03d}.json")
        base_timestamp = datetime.utcnow()

        with open(filename, "w", buffering=8192 * 16) as f:  # 128KB buffer
            remaining = LOGS_PER_FILE

            while remaining > 0:
                batch_size = min(BATCH_SIZE, remaining)
                batch = generate_batch(batch_size, base_timestamp)

                # Write batch to file
                for log in batch:
                    f.write(json.dumps(log, separators=(',', ':')) + "\n")

                # Update counter
                with counter_lock:
                    counter.value += batch_size

                remaining -= batch_size

        return filename
    except Exception as e:
        print(f"\nError generating file {file_index}: {e}", file=sys.stderr)
        raise


# -----------------------------
# Ana Çalıştırma
# -----------------------------
def main():
    global counter, counter_lock

    print("=" * 60)
    print("📊 LOG OLUŞTURUCU - Büyük Veri Ödevi")
    print("=" * 60)
    print(f"Dosya sayısı: {NUM_FILES}")
    print(f"Her dosyada log: {LOGS_PER_FILE:,}")
    print(f"Toplam log: {NUM_FILES * LOGS_PER_FILE:,}")
    print(f"Çıktı dizini: {OUTPUT_DIR}")
    print("=" * 60 + "\n")

    # Paylaşılan sayaç ve kilidi başlat
    counter = Value('i', 0)
    counter_lock = Lock()

    total_logs = NUM_FILES * LOGS_PER_FILE
    num_processes = min(NUM_FILES, os.cpu_count() or 1)

    # Çıktı dizinini oluştur
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Havuz ile ilerleme çubuğu kullan
    with Pool(processes=num_processes, initializer=init_worker, initargs=(counter, counter_lock)) as pool:
        with tqdm(total=total_logs, desc="Loglar üretiliyor", unit="log", unit_scale=True) as pbar:
            # Dosya üretimini başlat
            result = pool.map_async(generate_file, range(NUM_FILES))

            # İlerleme çubuğunu güncelle
            last_count = 0
            while not result.ready():
                current = counter.value
                pbar.update(current - last_count)
                last_count = current
                result.wait(0.1)

            # Son güncelleme
            pbar.update(total_logs - last_count)

            # Sonuçları al (hata varsa burada fırlatılır)
            filenames = result.get()

    # Toplam boyutu hesapla
    total_size = sum(
        os.path.getsize(f) for f in [os.path.join(OUTPUT_DIR, f"logs_{i:03d}.json") for i in range(NUM_FILES)])

    print("\n" + "=" * 60)
    print("✅ OLUŞTURMA TAMAMLANDI!")
    print("=" * 60)
    print(f"📁 Oluşturulan dosya: {NUM_FILES}")
    print(f"📊 Toplam log: {total_logs:,}")
    print(f"💾 Toplam boyut: {total_size / (1024 ** 2):.2f} MB")
    print(f"📂 Konum: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 60)
    print("\n🎯 Sonraki Adımlar:")
    print("1. Oluşan logları 'logs/' klasöründe inceleyin")
    print("2. Depolama altyapınızı seçin (HDFS veya MinIO)")
    print("3. Yükleme betiği ile logları yükleyin")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Oluşturma kullanıcı tarafından durduruldu")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Hata: {e}", file=sys.stderr)
        sys.exit(1)