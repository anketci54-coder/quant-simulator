# Quant Simulator

Rust ile yazılmış Binance Futures piyasa verisi kullanan bir işlem simülatörüdür. Proje gerçek emir göndermez; panelde gösterilen `PendingOpen -> Open -> Closed` akışı simülasyondur.

## Güvenlik sınırı

Bu sürüm canlı işlem için hazır değildir. HMAC bağımlılıkları bulunsa da API anahtarı okunmaz, imzalı `/fapi/v1/order` çağrısı yapılmaz ve exchange pozisyonlarıyla reconciliation uygulanmaz. API anahtarlarını repoya eklemeyin.

## Çalıştırma

1. `.env.example` dosyasını `.env` olarak kopyalayın.
2. Kontrolleri çalıştırın:

```bash
cargo fmt --all --check
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

3. Uygulamayı başlatın:

```bash
cargo run
```

Panel varsayılan olarak yalnızca `127.0.0.1:8080` üzerinde dinler.

## Risk modeli

- Varsayılan olarak yeni girişler kapalıdır (`ENTRY_ENABLED=false`)
- En fazla 10 eşzamanlı pozisyon
- Aynı yönde en fazla 3 pozisyon (`MAX_SAME_SIDE_POSITIONS`)
- İşlem başına en fazla %0,5 bakiye riski
- Tek işlemde bakiyenin en fazla %10'u marjin olarak ayrılır; gerçek miktar ATR stop mesafesine göre daha düşük kalabilir
- Toplam açık portföy riski en fazla %2
- Pozisyon marjı en fazla 100 USDT
- Üç ardışık kayıp veya %2 seans kaybında giriş kilidi
- Sembol başına bir saat cooldown
- 3x kaldıraç
- En az 20M USDT hacim, en fazla %0,10 spread ve üç ardışık OBI teyidi
- Yalnızca standart ASCII `*USDT` sembolleri; tarama başına en likit 12 aday
- Futures `exchangeInfo` üzerinden sembol bazlı `LOT_SIZE`
- Başlangıçta bakiye `closed_trades` ledger toplamından otomatik uzlaştırılır
- Üretim Futures public market verisi (`fapi.binance.com`); API anahtarı kullanılmaz
- `MTF_V3`: BTC 1 saatlik piyasa rejimi ile yön filtresi
- 15 dakikalık EMA/ADX ana trendi, 5 dakikalık teyit ve 1 dakikalık giriş zamanlaması
- OBI yalnızca kısa vadeli giriş zamanlaması için kullanılır
- Peak PnL %0,80'de ücret üstü break-even, %1,50'de en az %0,40 kâr kilidi
- TP1'de %30, TP2'de %30 kısmi realizasyon; kalan %40 ATR tabanlı trend stopuyla yönetilir
- Kapanış aşaması (`SL/BE/LOCK/TP1/TP3`), strateji sürümü, rejim ve giriş sinyal görüntüsü saklanır
- Cooldown yeniden başlatmalar arasında SQLite üzerinde korunur
- Panelde `MTF_V3` örnek sayısı ve strateji PnL'ı eski işlemlerden ayrı gösterilir
- SQLite WAL ve atomik batch snapshot
- Varsayılan başlangıç bakiyesi: 10.000 USDT (`INITIAL_BALANCE_USDT`)
- Panel fiyatları büyüklüğe göre 2/4/6/8 ondalık, PnL ve bakiye 2 ondalık gösterilir

## Bilinen canlı-geçiş eksikleri

- İmzalı emir katmanı ve `reduceOnly`
- Mark-price ve user-data websocket
- Partial fill/reject/idempotency yönetimi
- Tick-size, min-notional ve liquidation kontrolleri
- Funding, gerçek komisyon ve slipaj modeli
- Binance ile yeniden başlatma reconciliation
- Kalıcı günlük zarar baz çizgisi ve operatör acil-durdurma anahtarı
