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
- En fazla 5 eşzamanlı pozisyon
- İşlem başına en fazla %0,5 bakiye riski
- Toplam açık portföy riski en fazla %2
- Pozisyon marjı en fazla 100 USDT
- Üç ardışık kayıp veya %2 seans kaybında giriş kilidi
- Sembol başına bir saat cooldown
- 3x kaldıraç
- En az 20M USDT hacim, en fazla %0,10 spread ve üç ardışık OBI teyidi
- Futures `exchangeInfo` üzerinden sembol bazlı `LOT_SIZE`
- Başlangıçta bakiye `closed_trades` ledger toplamından otomatik uzlaştırılır
- Peak PnL tabanlı break-even ve trailing stop
- SQLite WAL ve atomik batch snapshot
- Varsayılan başlangıç bakiyesi: 1000 USDT

## Bilinen canlı-geçiş eksikleri

- İmzalı emir katmanı ve `reduceOnly`
- Mark-price ve user-data websocket
- Partial fill/reject/idempotency yönetimi
- Tick-size, min-notional ve liquidation kontrolleri
- Funding, gerçek komisyon ve slipaj modeli
- Binance ile yeniden başlatma reconciliation
- Günlük zarar limiti ve acil durdurma
