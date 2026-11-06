# ZooKeeper (Go)

*ZooKeeper: Wait-free coordination for Internet-scale systems* (Hunt, Konar, Junqueira, Reed — USENIX ATC 2010) makalesinin Go ile yazılmış bir implementasyonu. Dış bağımlılık yoktur, yalnızca standart kütüphane kullanılır.

## Paketler

| Paket | İçerik | Makale |
|---|---|---|
| `zk` | Ortak tipler: `Stat`, create bayrakları, olaylar, hata kodları, istemci–sunucu protokolü | §2.1, §2.2 |
| `server` | Replike sunucu: veri ağacı, request processor, Zab tarzı atomik yayın, lider seçimi, oturumlar, watch'lar, WAL ve fuzzy snapshot | §4 |
| `client` | İstemci kütüphanesi: sync ve async API, FIFO callback'ler, watch'lar, heartbeat, sunucu değiştirme | §2.2, §4.4 |
| `recipes` | İstemci tarafı primitifler: config, rendezvous, grup üyeliği, basit kilit, herd etkisiz kilit, R/W kilit, çift bariyer, lider seçimi | §2.4 |
| `transport` | Mesaj tabanlı FIFO bağlantılar: TCP (gob) ve testler için bellek içi ağ | §4.2 |
| `cmd/zkserver`, `cmd/zkcli` | Sunucu ve komut satırı istemcisi | |
| `examples/readyconfig` | §2.3'teki "ready znode" senaryosu | §2.3 |

## Makaledeki kavramların koddaki karşılıkları

- **Znode'lar** (regular, ephemeral, sequential), sürümler ve koşullu güncellemeler (`-1` = sürüm kontrolü yok): [server/datatree.go](server/datatree.go), [server/prep.go](server/prep.go)
- **Watch'lar** tek seferliktir ve oturuma bağlıdır. Bildirim, yeni durum okunabilir hale gelmeden önce istemci bağlantısının kuyruğuna girer: [server/watches.go](server/watches.go)
- **Linearizable yazmalar**: yazmalar lidere iletilir, lider bunları *idempotent* işlemlere dönüştürür (setDataTXN, errorTXN). "Gelecek durum" henüz uygulanmamış işlemler de hesaba katılarak bulunur: [server/prep.go](server/prep.go)
- **Atomik yayın (Zab)**: discovery (epoch), senkronizasyon (DIFF/SNAP, NEWLEADER), broadcast (PROPOSAL/ACK/COMMIT). İşlemler ACK'lenmeden önce diske yazılır: [server/leader.go](server/leader.go), [server/follower.go](server/follower.go)
- **Lider seçimi**: en güncel geçmişe sahip sunucu `(epoch, zxid, id)` sırasıyla seçilir: [server/election.go](server/election.go)
- **Okumalar yereldir**: her okuma, sunucunun son zxid'i ile etiketlenir. `sync`, liderin kuyruğunun sonuna yerleşir ve hiçbir şey yayınlamaz: [server/session.go](server/session.go), `leaderSync`
- **FIFO istemci sırası**: bir oturumun okumaları, kendinden önceki yazmalar tamamlanana kadar bekler. İstemci callback'leri tek bir goroutine'de sırayla çalışır.
- **Oturumlar**: zaman aşımına uğrayan oturumu lider sonlandırır (CloseSession işlemi ephemeral'ları siler). İstemci s/3 boşta kalınca heartbeat gönderir, 2s/3 boyunca ses alamazsa sunucu değiştirir. Yeni sunucu, istemcinin gördüğü son zxid'den gerideyse oturumu kabul etmez.
- **Fuzzy snapshot**: ağaç kilitlenmeden DFS ile gezilir. İşlemler idempotent olduğu için snapshot sonrası log tekrar oynatılınca doğru duruma ulaşılır. Makaledeki `/foo`–`/goo` örneği birebir test edilir: [server/datatree_test.go](server/datatree_test.go)
- **WAL**: kayıtlar uzunluk önekli ve CRC'lidir. Yazma sırasında kesilmiş bir kuyruk kurtarmada atılır: [server/storage.go](server/storage.go)

## Kullanım

```go
c, err := client.Connect(client.Config{
    Servers:        []string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"},
    SessionTimeout: 10 * time.Second,
})
defer c.Close()

c.Create("/app1", []byte("cfg"), zk.FlagPersistent)
data, stat, ch, _ := c.GetDataW("/app1")   // tek seferlik watch
c.SetData("/app1", []byte("v2"), stat.Version)
<-ch                                       // NodeDataChanged

// Asenkron: callback'ler istek sırasıyla çağrılır
c.CreateAsync("/app1/p-", nil, zk.FlagEphemeral|zk.FlagSequential, func(p string, err error) {})

// Başka bir istemcinin yazmasını kesin görmek için önce sync ("slow read")
c.Sync("/app1")

// Tarifler
l := recipes.NewLock(c, "/locks/db")
l.Lock(ctx); defer l.Unlock()
```

Okumalar bağlı olunan sunucunun yerel replikasından yapılır ve bayat olabilir. Başka bir istemcinin yazmasını görmek gerekiyorsa okumadan önce `Sync` çağrılmalıdır (§2.3).

### Ensemble'ı çalıştırmak

```sh
go build ./cmd/...
zkserver -id 1 -servers 1=127.0.0.1:2181,2=127.0.0.1:2182,3=127.0.0.1:2183 -dir data1
zkserver -id 2 -servers 1=127.0.0.1:2181,2=127.0.0.1:2182,3=127.0.0.1:2183 -dir data2
zkserver -id 3 -servers 1=127.0.0.1:2181,2=127.0.0.1:2182,3=127.0.0.1:2183 -dir data3

zkcli -servers 127.0.0.1:2181,127.0.0.1:2182 create /demo hello
zkcli -servers 127.0.0.1:2183 create -s /demo/item- a
zkcli -servers 127.0.0.1:2182 ls /demo
```

## Testler

```sh
go test ./...            # bellek içi ağda 3/5 sunuculu ensemble'lar + TCP testi
go test -race ./...
go run ./examples/readyconfig
```

Test kapsamı: temel işlemler, koşullu güncellemeler, replikasyon ve replikaların yakınsaması, sequential/ephemeral znode'lar, watch'lar (bildirimin yeni durumdan önce geldiği dahil), oturum zaman aşımı, async pipeline'da FIFO sırası, sync ile slow read, lider çökmesi, takipçinin DIFF ve SNAP ile yakalaması, tüm ensemble'ın diskten yeniden başlaması, azınlıkta kalan sunucunun hizmeti durdurması, watch'larla sunucu değiştirme, TCP ve tüm tarifler.

## Sınırlamalar

- ACL'ler, kotalar ve dinamik üyelik (reconfig) yoktur. Makale bunları kapsamıyor.
- Lider seçimi, makalenin kapsamı dışında bıraktığı ayrıntılar için basitleştirilmiş, tur tabanlı bir oylamadır. Zab'ın tam bir formal implementasyonu değildir.
- Bağlantı kesildiğinde yanıtı gelmemiş istekler `ErrConnectionLoss` ile döner; işlemin uygulanıp uygulanmadığı bilinemez (gerçek ZooKeeper'daki gibi).
