"""
HDFS'ye Log Yükleme Betiği - ÖDEV
==================================

Bu betik, oluşturulan log dosyalarını HDFS'ye yüklemek için kullanılacaktır.

GÖREV:
------
Aşağıdaki işlemleri gerçekleştiren kodu yazmalısınız:

1. HDFS'ye Bağlantı Kurma:
   - InsecureClient kullanarak HDFS'ye bağlanın
   - Bağlantı ayarları aşağıda verilmiştir

2. Dizin Oluşturma:
   - HDFS üzerinde hedef dizini oluşturun
   - Dizin zaten varsa hata vermesin

3. Log Dosyalarını Bulma:
   - logs/ klasöründeki tüm .json dosyalarını bulun
   - Path ve glob kullanabilirsiniz

4. Dosyaları Yükleme:
   - Her log dosyasını HDFS'ye yükleyin
   - İlerleme çubuğu gösterin (tqdm kullanarak)
   - Hata durumunda devam edin, diğer dosyaları yüklemeye devam edin

5. Doğrulama:
   - Yüklenen dosyaları listeleyin
   - Özet bilgi verin (kaç dosya yüklendi, nerede)

İPUCU:
------
- HDFS client: from hdfs import InsecureClient
- Dosya bulma: from pathlib import Path
- İlerleme: from tqdm import tqdm
- HDFS metodları: client.makedirs(), client.write(), client.list()
"""

import os
from hdfs import InsecureClient
from pathlib import Path
from tqdm import tqdm

# ============================================
# HDFS Yapılandırması (HAZIR - DEĞİŞTİRME)
# ============================================
# Docker ağı içindeyken 'namenode', hosttan çalışırken 'localhost' kullanılır
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = "root"
HDFS_PATH = "/logs/raw"

# Log dosyalarının bulunduğu dizin
LOGS_DIR = "logs"

# ============================================
# BURADAN İTİBAREN SİZİN GÖREVİNİZ
# ============================================

def upload_to_hdfs():
    """
    Log dosyalarını HDFS'ye yükle
    
    TODO: Bu fonksiyonu tamamlayın
    """
    # 1. HDFS istemcisini oluşturun
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    print(f"✅ HDFS'ye bağlantı kuruldu: {HDFS_URL}")
    
    # Bağlantıyı test et
    try:
        client.status('/')
        print(f"✅ HDFS bağlantısı başarıyla doğrulandı")
    except Exception as e:
        print(f"❌ HDFS bağlantı hatası: {e}")
        return
    
    # 2. HDFS'de hedef dizini oluşturun

    # 3. Yüklenecek log dosyalarını bulun
    
    # 4. Her dosyayı HDFS'ye yükleyin
    
    # 5. Yükleme sonucunu doğrulayın ve rapor edin

    pass


if __name__ == "__main__":
    try:
        upload_to_hdfs()
    except Exception as e:
        print(f"❌ Hata oluştu: {e}")