# ZooKeeper (Go)

**English:** [README.md](README.md) · **Mimari:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

*ZooKeeper: Wait-free coordination for Internet-scale systems* (Hunt, Konar, Junqueira, Reed — USENIX ATC 2010) makalesinin Go implementasyonu. Proje, bu kütüphaneyi gerçek senaryolarda kullanan bir uygulamayı (`zkapp`) ve etkileşimli bir web laboratuvarını da içerir. Dış bağımlılık yoktur, yalnızca standart kütüphane kullanılır.

## Web laboratuvarı (önerilen başlangıç)

```sh
docker compose up -d --build    # ya da: go run ./cmd/zkapp web
# tarayıcıda http://localhost:8080
```

1. **Node'ları sen başlatırsın:** "1 node" ya da "3 node'luk cluster" ile başla, "+ Node ekle" ile büyüt. Her node kartında rol (lider/takipçi), epoch ve zxid görünür; Durdur, Başlat ve Kaldır butonları vardır. Üyelik statik olduğu için node eklemek ya da kaldırmak diğer node'ları yeniden başlatır; veriler korunur.
2. **"Ne yapmak istersin?" listesinden bir işlem seç:**
   - **Veri:** znode oluştur, oku, güncelle (sürümlü), sil, çocukları listele, watch kur.
   - **Uygulamalar:** servis, gateway, scheduler, config servisi, worker ve bilet ajanı başlat; config yaz, kuyruğa iş ekle.
   - **Arıza deneyleri:** lideri çökert, bir süreci çökert ya da durdur.
   - **Hazır senaryolar:** 6 senaryo.

   Her işlemin altında "ne olur?" açıklaması vardır.
3. **İzle:**
   - **Loglar** sekmesinde her node, kendi kopyasına uyguladığı her değişikliği yazar (`✎ znode created in local replica … zxid=2:5`). Ayarlar menüsünden node loglarını *debug* yapınca PROPOSAL → ACK → COMMIT adımları da görünür.
   - **Znode ağacı** sekmesinde her node'un kendi kopyası görünür.

Seçenekler: `-n 3` (açılışta 3 node), `-addr 0.0.0.0:8080`, `-echo` (logları terminale de bas), `-server-log debug`.

## WAL ve kurtarma

Her işlem, follower ACK göndermeden önce (lider de kendi oyunu saymadan önce) CRC'li bir write-ahead log'a yazılıp diske zorlanır (`fsync`). Her 10 000 işlemde bir, yazmaları durdurmadan fuzzy snapshot alınır. Yeniden başlayan bir sunucu önce snapshot'ı yükler, sonra log'u okur. Commit edildiği henüz bilinmeyen işlemleri, yeni lider commit noktasını bildirene kadar uygulamaz (DIFF ya da SNAP ile). İşlemler idempotent olduğu için fuzzy snapshot'ın üzerine log'u tekrar oynatmak doğru sonucu verir. Docker cluster'ı `FileStorage`'ı volume'larda kullanır. Web laboratuvarı ise bellek içi depolama kullanır: node çöküp yeniden başlayınca veriler kalır, ama laboratuvar süreci kapanınca kaybolur. Ayrıntılar için [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Docker

```sh
docker compose up -d --build          # web lab → http://localhost:8080
```

Gerçek dağıtık kurulum: her ZooKeeper sunucusu ve her uygulama ayrı bir container'da çalışır. Sunucu verileri volume'larda kalıcıdır.

```sh
docker compose --profile cluster up -d --build
docker compose --profile cluster logs -f zk1 zk2 zk3      # sunucu logları
docker compose --profile cluster logs -f gateway scheduler-1 scheduler-2 api-1 shop-1 worker-1

docker compose --profile cluster stop zk3                 # bir sunucuyu (ör. lideri) öldür
docker compose --profile cluster start zk3                # geri getir: diskten kurtarır, DIFF ile yakalar
docker compose --profile cluster stop api-1               # servis kaydı, oturum süresi dolunca kaybolur

docker compose run --rm cli zkapp config-set rateLimit=75 # tek seferlik komutlar
docker compose run --rm cli zkapp queue-produce -count 5
docker compose run --rm cli zkapp tree /

docker compose --profile cluster down -v                  # her şeyi kapat ve verileri sil
```

| Container | İçerik |
|---|---|
| `lab` | Web laboratuvarı (kendi içinde 3 sunucu), port 8080 |
| `zk1` `zk2` `zk3` | `zkserver` ensemble'ı, host portları 2181–2183 |
| `api-1` `api-2` | HTTP servisleri, `/services/api` altında kayıtlı |
| `gateway` | Servisleri keşfedip istekleri dağıtır |
| `scheduler-1` `scheduler-2` | Lider seçimi, yalnızca lider iş çalıştırır |
| `shop-1` | `/apps/shop/config` ayarını canlı uygular |
| `worker-1` | `/apps/queue` kuyruğundan iş alır |

## Terminalde demo

```sh
go run ./cmd/zkapp demo
```

Bu komut aynı süreç içinde 3 sunuculu bir ensemble başlatır ve 6 senaryoyu sırayla çalıştırır. Senaryoların içinde hata enjeksiyonu (çöken uygulama süreçleri ve çöken ZooKeeper lideri) vardır. Her adımın logları görünür; sonda tüm doğrulamaların sonucu yazdırılır (`✔` / `✘`).

```sh
go run ./cmd/zkapp demo discovery lock        # yalnızca bazı senaryolar
go run ./cmd/zkapp demo -server-log debug      # sunucu tarafında her PROPOSAL / ACK / COMMIT
go run ./cmd/zkapp demo -log trace             # heartbeat'ler dahil her şey
go run ./cmd/zkapp demo -pause 3s              # adımlar arasında daha uzun bekleme
```

## Senaryolar (`app/`)

| # | Senaryo | Gerçek dünyadaki problem | ZooKeeper çözümü | Demoda gösterilen hata |
|---|---|---|---|---|
| 1 | **Dinamik konfigürasyon** ([app/config.go](app/config.go)) | Rate limit, feature flag ve log seviyesini yeniden başlatmadan tüm servislerde değiştirmek | Tek znode + watch. Operatörler sürüme koşullu `setData` yapar. | İki operatör aynı anda yazar: `BadVersion` alınır, yeniden denenir, kayıp güncelleme olmaz |
| 2 | **Servis keşfi + yük dengeleme** ([app/discovery.go](app/discovery.go)) | Instance'lar gelir gider, adresleri önceden bilinmez | Her instance `EPHEMERAL\|SEQUENTIAL` znode ile kaydolur. Gateway çocukları watch'lar. | Bir instance çöker (session timeout sonrası silinir), biri düzgün kapanır (anında silinir) |
| 3 | **Lider seçimi** ([app/scheduler.go](app/scheduler.go)) | Periyodik işler tam olarak bir kez çalışmalı, ama zamanlayıcı da yüksek erişilebilir olmalı | Election tarifi: en düşük sıra numarası lider olur, herkes yalnızca bir öncekini izler | Lider çöker, sonra yenisi kapatılır. Aynı anda en fazla 1 lider doğrulanır. |
| 4 | **Dağıtık kilit** ([app/tickets.go](app/tickets.go)) | İşlem (transaction) desteği olmayan bir kaynakta (JSON dosyası) bilet satışında fazla satış | Herd etkisi olmayan kilit | Önce kilitsiz (fazla satış görülür), sonra kilitli (tam 12 satış) |
| 5 | **İş kuyruğu** ([app/queue.go](app/queue.go)) | İşler tam olarak bir worker'a gitmeli ve çöken worker'ın işi kaybolmamalı | İşler `SEQUENTIAL`, sahiplik (claim) `EPHEMERAL` znode | Bir worker iş ortasında çöker; claim'i sona erer, iş başka worker'a geçer |
| 6 | **ZooKeeper sunucu arızası** | Koordinasyon servisinin kendisi çöktüğünde ne olur? | Zab lider seçimi, oturumun başka sunucuya taşınması, watch'ların yeniden kaydı | Lider sunucu öldürülür; uygulamalar çalışmaya devam eder, sunucu geri gelip DIFF ile yakalar |

## Çok terminalli mod (gerçek TCP, her bileşen ayrı süreç)

```sh
go build -o zkapp ./cmd/zkapp          # Windows'ta: go build -o zkapp.exe ./cmd/zkapp

# Terminal 1: 3 sunuculu ensemble (127.0.0.1:2181-2183)
zkapp cluster                            # -log debug ile her proposal/commit görünür, -dir data ile kalıcı olur

# Terminal 2, 3: HTTP servisleri
zkapp service -name api-1
zkapp service -name api-2

# Terminal 4: yük dengeleyici
zkapp gateway

# Terminal 5, 6: lider seçimi
zkapp scheduler -name scheduler-1
zkapp scheduler -name scheduler-2

# Terminal 7: canlı konfigürasyon
zkapp config-service -name shop-1
zkapp config-set rateLimit=50 feature.newCheckout=on      # başka bir terminalden

# Kilit: kilitsiz ve kilitli satışı karşılaştırmak için
zkapp tickets-init -count 20
zkapp tickets-agent -name agent-1 -nolock   &   zkapp tickets-agent -name agent-2 -nolock
zkapp tickets-agent -name agent-1           &   zkapp tickets-agent -name agent-2

# Kuyruk
zkapp queue-worker -name worker-1
zkapp queue-produce -count 10

# Durumu incelemek
zkapp tree /apps
zkapp tree /services
```

Bir süreci **Ctrl+C** ile durdurursanız oturumu kapanır ve ephemeral znode'ları anında silinir. **Terminal penceresini kapatırsanız** ya da süreci öldürürseniz (çökme), silinme session timeout'tan sonra olur. Bu farkı gateway ve scheduler loglarında izleyebilirsiniz.

## Loglar

Her satır: `zaman · seviye · bileşen │ mesaj anahtar=değer`.

```
00:12:01.110 INFO  server-3          │ ★ phase 3 (broadcast): ensemble established, serving clients epoch=1 followers="[2 1]"
00:12:03.415 DEBUG gateway           │ → request op=getChildren path=/services/api xid=7 watch=true
00:12:03.416 INFO  scheduler-2/election │ waiting: watching only the znode before ours (no herd effect) watching=n-0000000000
00:12:05.873 WARN  worker-2          │ 💥 process CRASHED (no goodbye to ZooKeeper: its session will expire after the timeout)
00:12:07.880 INFO  server-1          │ ★ session expired: no server heard from it within its timeout session=1f3a… timeout=2s
```

- **Bileşenler:** `server-N` ZooKeeper sunucularıdır. Diğer isimler uygulama süreçleridir. `isim/lock`, `isim/election`, `isim/config`, `isim/group`, `isim/barrier` ise o süreç içinde çalışan tariflerdir.
- **Seviyeler:**
  - `INFO`: yaşam döngüsü olayları (seçim, Zab fazları, oturumlar, snapshot'lar, uygulama adımları). `★` ile başlayan satırlar büyük geçişleri işaretler.
  - `DEBUG`: her istek/yanıt, proposal, ack, commit, uygulanan işlem ve watch.
  - `TRACE`: heartbeat'ler ve seçim mesajları.
- `NO_COLOR=1` renkleri kapatır. Testlerde `ZK_LOG=debug go test ./tests/ -run TestLeaderFailover -v` ile loglar açılır.
- Kütüphane tarafında logger opsiyoneldir: `server.Config.Logger` ve `client.Config.Logger` alanlarına bir `*slog.Logger` verilir, `nil` ise log basılmaz. Terminal çıktısı için [zlog/](zlog/) paketi kullanılabilir.

## Paketler

| Paket | İçerik | Makale |
|---|---|---|
| `zk` | Ortak tipler: `Stat`, create bayrakları, olaylar, hata kodları, istemci–sunucu protokolü | §2.1, §2.2 |
| `server` | Replike sunucu: veri ağacı, request processor, Zab tarzı atomik yayın, lider seçimi, oturumlar, watch'lar, WAL ve fuzzy snapshot | §4 |
| `client` | İstemci kütüphanesi: sync ve async API, FIFO callback'ler, watch'lar, heartbeat, sunucu değiştirme | §2.2, §4.4 |
| `recipes` | Config, rendezvous, grup üyeliği, basit kilit, herd etkisiz kilit, R/W kilit, çift bariyer, lider seçimi | §2.4 |
| `app` | Yukarıdaki gerçek dünya senaryoları ve demo orkestratörü | §3 |
| `zlog` | Renkli, seviyeli, bileşen etiketli terminal logger (`log/slog` handler'ı) | |
| `web` | Web laboratuvarı: HTTP API, SSE log akışı, gömülü tek sayfalık arayüz | |
| `transport` | Mesaj tabanlı FIFO bağlantılar: TCP (gob) ve bellek içi ağ | §4.2 |
| `cmd/zkapp` | Demo ve çok terminalli uygulama | |
| `cmd/zkserver`, `cmd/zkcli` | Tek sunucu süreci ve düşük seviyeli CLI | |

## Makaledeki kavramların koddaki karşılıkları

- **Znode'lar** (regular, ephemeral, sequential), sürümler ve koşullu güncellemeler: [server/datatree.go](server/datatree.go), [server/prep.go](server/prep.go)
- **Watch'lar** tek seferliktir. Bildirim, yeni durum okunabilir hale gelmeden önce istemciye ulaşır: [server/watches.go](server/watches.go)
- **Linearizable yazmalar**: lider istekleri idempotent işlemlere çevirir. "Gelecek durum" henüz uygulanmamış işlemler de hesaba katılarak bulunur: [server/prep.go](server/prep.go)
- **Zab**: discovery → senkronizasyon (DIFF/SNAP, NEWLEADER) → broadcast (PROPOSAL/ACK/COMMIT): [server/leader.go](server/leader.go), [server/follower.go](server/follower.go)
- **Lider seçimi** `(epoch, zxid, id)` sırasına göre yapılır: [server/election.go](server/election.go)
- **Okumalar yereldir**. `sync` liderin kuyruğunun sonuna girer ve hiçbir şey yayınlamaz. Başka bir istemcinin yazmasını kesin görmek için okumadan önce `Sync` çağrılmalıdır (§2.3).
- **Oturumlar**: istemci s/3 boşta kalınca heartbeat gönderir, 2s/3 boyunca ses alamazsa sunucu değiştirir. Yeni sunucu, istemcinin gördüğü son zxid'den gerideyse oturumu kabul etmez.
- **Fuzzy snapshot** + idempotent tekrar oynatma (makaledeki `/foo`–`/goo` örneği test edilir): [server/datatree_test.go](server/datatree_test.go)
- **WAL** kayıtları uzunluk önekli ve CRC'lidir. Yazma sırasında kesilmiş bir kuyruk kurtarmada atılır: [server/storage.go](server/storage.go)

## Kütüphane kullanımı

```go
c, err := client.Connect(client.Config{
    Servers:        []string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"},
    SessionTimeout: 10 * time.Second,
    Name:           "my-service",
    Logger:         zlog.NewTerminal(slog.LevelDebug), // opsiyonel
})
defer c.Close()

c.Create("/app1", []byte("cfg"), zk.FlagPersistent)
data, stat, ch, _ := c.GetDataW("/app1")   // tek seferlik watch
c.SetData("/app1", []byte("v2"), stat.Version)
<-ch                                       // NodeDataChanged

l := recipes.NewLock(c, "/locks/db")
l.Lock(ctx); defer l.Unlock()
```

## Testler

```sh
go test ./...              # kütüphane, ensemble testleri ve tüm demo senaryoları
go test -race ./...
```

## Sınırlamalar

- ACL'ler, kotalar ve dinamik üyelik (reconfig) yoktur.
- Lider seçimi basitleştirilmiş, tur tabanlı bir oylamadır. Zab'ın formal bir implementasyonu değildir.
- Bağlantı kesildiğinde yanıtı gelmemiş istekler `ErrConnectionLoss` ile döner; işlemin uygulanıp uygulanmadığı bilinemez (gerçek ZooKeeper'daki gibi).
