"""
MinIO'ya Log Yükleme Betiği - ÖDEV
==================================

Bu betik, oluşturulan log dosyalarını MinIO'ya yüklemek için kullanılacaktır.

GÖREV:
------
Aşağıdaki işlemleri gerçekleştiren kodu yazmalısınız:

1. MinIO'ya Bağlantı Kurma:
   - Minio client kullanarak MinIO'ya bağlanın
   - Bağlantı ayarları aşağıda verilmiştir

2. Bucket Oluşturma:
   - MinIO üzerinde bucket oluşturun
   - Bucket zaten varsa hata vermesin

3. Log Dosyalarını Bulma:
   - logs/ klasöründeki tüm .json dosyalarını bulun
   - Path ve glob kullanabilirsiniz

4. Dosyaları Yükleme:
   - Her log dosyasını MinIO'ya yükleyin
   - İlerleme çubuğu gösterin (tqdm kullanarak)
   - Hata durumunda devam edin, diğer dosyaları yüklemeye devam edin

5. Doğrulama:
   - Yüklenen dosyaları listeleyin
   - Özet bilgi verin (kaç dosya yüklendi, nerede)

İPUCU:
------
- MinIO client: from minio import Minio
- Hata yönetimi: from minio.error import S3Error
- Dosya bulma: from pathlib import Path
- İlerleme: from tqdm import tqdm
- MinIO metodları: client.bucket_exists(), client.make_bucket(), client.fput_object()
"""

import os
from minio import Minio
from minio.error import S3Error
from pathlib import Path
from tqdm import tqdm

# ============================================
# MinIO Yapılandırması (HAZIR - DEĞİŞTİRME)
# ============================================
# Docker ağı içindeyken endpoint olarak 'minio:9000', hosttan çalışırken ortam değişkeni kullanılır
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "logs"
MINIO_SECURE = False

# Log dosyalarının bulunduğu dizin
LOGS_DIR = "logs"

# ============================================
# BURADAN İTİBAREN SİZİN GÖREVİNİZ
# ============================================

def upload_to_minio():
    """
    Log dosyalarını MinIO'ya yükle
    
    TODO: Bu fonksiyonu tamamlayın
    """
    # 1. MinIO istemcisini oluşturun
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    print(f"✅ MinIO'ya bağlantı kuruldu: {MINIO_ENDPOINT}")
    
    # Bağlantıyı test et
    try:
        client.list_buckets()
        print(f"✅ MinIO bağlantısı başarıyla doğrulandı")
    except Exception as e:
        print(f"❌ MinIO bağlantı hatası: {e}")
        return
    
    # 2. Bucket var mı kontrol et, yoksa oluştur

    # 3. Yüklenecek log dosyalarını bulun
    
    # 4. Her dosyayı MinIO'ya yükleyin
    
    # 5. Yükleme sonucunu doğrulayın ve rapor edin

    pass


if __name__ == "__main__":
    try:
        upload_to_minio()
    except Exception as e:
        print(f"❌ Hata oluştu: {e}")