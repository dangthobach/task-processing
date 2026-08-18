# Skill: stream-batch-processing

> Xử lý dữ liệu lớn (triệu-chục triệu record) theo streaming hoặc batch mà không tràn bộ nhớ — SAX streaming Excel/XML, cursor-based/keyset pagination, chunk-oriented batch insert, backpressure, checkpoint/resume, idempotency, ETL pipeline design. LUÔN dùng skill này khi user nhắc tới "xử lý file Excel/CSV lớn", "import/export hàng triệu record", "OutOfMemory", "batch processing", "ETL", "migration data", "streaming", "cursor pagination", hoặc khi thấy code đang load toàn bộ file/query result vào List/Slice/Vec trong memory. Áp dụng cho Java/Spring Boot, Go và Rust.

# Stream/Batch Processing cho Dữ liệu Lớn

## Nguyên tắc cốt lõi: Bounded Memory

Câu hỏi bắt buộc trước khi viết bất kỳ đoạn code xử lý dữ liệu lớn nào: **"Nếu input tăng gấp 100 lần, code này còn chạy được không?"** Nếu câu trả lời phụ thuộc vào việc tăng RAM máy chủ, đó là dấu hiệu đang buffer toàn bộ thay vì stream.

```
❌ Load-all:   [Đọc toàn bộ file] → [Xử lý] → [Ghi toàn bộ]     (memory = O(n))
✅ Streaming:  [Đọc 1 record] → [Xử lý] → [Ghi] → lặp lại       (memory = O(1) hoặc O(chunk_size))
✅ Batch:      [Đọc N record] → [Xử lý] → [Ghi N] → lặp lại     (memory = O(chunk_size), throughput cao hơn streaming từng record)
```

**Streaming từng record**: memory thấp nhất, độ trễ thấp, nhưng overhead per-record (network round-trip, transaction) cao nếu ghi từng dòng.
**Batch/chunk-oriented**: gom N record (thường 500-5000 tuỳ use case) rồi xử lý/ghi 1 lần — cân bằng giữa memory và throughput. Đây là lựa chọn mặc định cho ETL/migration.

## Quy trình thiết kế pipeline xử lý dữ liệu lớn

1. **Xác định nguồn có hỗ trợ đọc tuần tự không** (file: luôn có; DB: cần cursor/keyset, KHÔNG dùng `OFFSET` lớn; message queue: luôn có).
2. **Chọn chunk size** dựa trên: kích thước trung bình 1 record × chunk size phải nhỏ hơn nhiều so với heap/memory limit của process; đo thực tế thay vì đoán, bắt đầu 500-1000 rồi tune theo throughput đo được.
3. **Thiết kế idempotency**: pipeline PHẢI chạy lại được từ giữa chừng mà không tạo duplicate — dùng unique constraint ở đích, hoặc upsert, hoặc checkpoint table lưu record/offset cuối đã xử lý thành công.
4. **Thiết kế checkpoint/resume**: với job chạy hàng giờ trên hàng triệu record, không được để 1 lỗi ở record thứ 9,999,999 buộc chạy lại từ đầu — lưu tiến độ định kỳ (mỗi N chunk) vào bảng/state riêng.
5. **Validate và báo lỗi theo dòng, không fail-fast toàn bộ job** (trừ khi lỗi hệ thống): thu thập lỗi từng record vào file/log riêng, tiếp tục xử lý record khác — đúng mô hình PDMS đã làm với Excel validation error reporting.
6. **Kiểm soát song song có giới hạn** (bounded parallelism/backpressure): xử lý song song nhiều chunk tăng throughput nhưng phải giới hạn (worker pool, semaphore) để không làm sập DB/downstream bằng lượng connection/request vượt khả năng.

## Backpressure — nguyên tắc chung

Khi tốc độ sản xuất dữ liệu (producer) nhanh hơn tốc độ tiêu thụ (consumer), phải có cơ chế làm chậm producer lại, KHÔNG để hàng đợi phình vô hạn trong memory:
- Bounded channel/queue (kích thước giới hạn, producer block khi đầy) — cách đơn giản và hiệu quả nhất
- Semaphore giới hạn số tác vụ đồng thời khi gọi downstream (DB, external API)
- Kafka consumer: giới hạn `max.poll.records` + xử lý xong mới commit offset, không prefetch quá nhiều

## Cursor-based / Keyset Pagination — luôn ưu tiên hơn `OFFSET`

`OFFSET n LIMIT m` chậm dần khi `n` lớn (DB phải quét qua `n` row rồi bỏ) và không an toàn khi có ghi/xoá xen kẽ (duplicate/miss record). Dùng keyset pagination: `WHERE id > :last_seen_id ORDER BY id LIMIT :chunk_size` — luôn O(log n) với index, ổn định khi data thay đổi giữa chừng.

## Checklist review code xử lý dữ liệu lớn

- [ ] Không có `readAllBytes()`/`ioutil.ReadAll()`/`fs::read_to_string()` trên file có thể lớn không kiểm soát kích thước
- [ ] Không có query DB không giới hạn (`findAll()`, `SELECT *` không LIMIT) trên bảng có thể triệu record
- [ ] Có chunk size rõ ràng, có thể cấu hình (không hardcode) để tune theo môi trường
- [ ] Có idempotency key hoặc upsert logic — chạy lại pipeline 2 lần không tạo duplicate
- [ ] Lỗi từng record được thu thập riêng, không làm chết cả job (trừ lỗi hạ tầng)
- [ ] Song song hoá (nếu có) bị giới hạn bởi worker pool/semaphore, không spawn không giới hạn

## Tài liệu tham khảo theo ngôn ngữ

- `references/java-spring.md` — Apache POI SAX streaming, JPA batch insert (`hibernate.jdbc.batch_size`), Spring Batch chunk-oriented, keyset pagination với Spring Data, Kafka consumer batching (dựa trên kinh nghiệm thực tế PDMS: 10M+ record credit migration, 200K+ record/sheet Excel ETL)
- `references/go.md` — `bufio.Scanner`/`io.Reader` streaming, pipeline pattern qua channel, worker pool có giới hạn, `database/sql` cursor qua `rows.Next()`
- `references/rust.md` — `Iterator` lazy evaluation, `async Stream` trait, `tokio::sync::Semaphore` cho bounded concurrency, zero-copy parsing


---


# 📄 stream-batch-processing/references/go.md

# Stream/Batch Processing với Go

## `io.Reader`/`bufio.Scanner` — streaming file mặc định của Go, không cần thư viện ngoài

```go
func ProcessLargeFile(path string) error {
    f, err := os.Open(path)
    if err != nil { return err }
    defer f.Close()

    scanner := bufio.NewScanner(f)
    scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // tăng buffer nếu dòng dài, nhưng vẫn bounded
    for scanner.Scan() {
        line := scanner.Text() // 1 dòng tại 1 thời điểm, memory O(1) theo số dòng
        if err := processLine(line); err != nil {
            recordError(line, err) // ghi lỗi riêng, KHÔNG return err làm dừng cả file
            continue
        }
    }
    return scanner.Err()
}
```
Với CSV lớn dùng `encoding/csv` + `Reader.Read()` theo dòng (không dùng `ReadAll()`). Với Excel, dùng thư viện hỗ trợ streaming (VD: `qax-os/excelize` có `Rows()` iterator streaming) thay vì load toàn sheet.

## Pipeline pattern qua channel — Go idiomatic cho ETL

```go
func RunPipeline(ctx context.Context, source <-chan RawRecord) <-chan Result {
    validated := make(chan ValidRecord, 100)  // bounded channel — chính là backpressure tự nhiên của Go
    results := make(chan Result, 100)

    // Stage 1: validate — N worker song song, giới hạn rõ ràng
    var wg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for rec := range source {
                if v, err := validate(rec); err == nil {
                    validated <- v
                } else {
                    recordError(rec, err)
                }
            }
        }()
    }
    go func() { wg.Wait(); close(validated) }()

    // Stage 2: batch insert theo chunk
    go func() {
        defer close(results)
        batch := make([]ValidRecord, 0, chunkSize)
        for v := range validated {
            batch = append(batch, v)
            if len(batch) >= chunkSize {
                results <- insertBatch(ctx, batch)
                batch = batch[:0] // reset slice, giữ lại capacity đã cấp phát — tránh alloc lại mỗi batch
            }
        }
        if len(batch) > 0 {
            results <- insertBatch(ctx, batch)
        }
    }()

    return results
}
```
Channel có buffer giới hạn (`make(chan T, 100)`) tự động tạo backpressure: khi stage sau chậm hơn stage trước, channel đầy → `send` block → stage trước tự chậm lại, không cần cơ chế riêng.

## Worker pool giới hạn song song (bounded concurrency)

```go
func ProcessConcurrently(ctx context.Context, chunks []Chunk, maxWorkers int) error {
    sem := make(chan struct{}, maxWorkers) // semaphore qua buffered channel
    var wg sync.WaitGroup
    errCh := make(chan error, len(chunks))

    for _, chunk := range chunks {
        wg.Add(1)
        sem <- struct{}{} // acquire — block nếu đã đủ maxWorkers đang chạy
        go func(c Chunk) {
            defer wg.Done()
            defer func() { <-sem }() // release
            if err := processChunk(ctx, c); err != nil {
                errCh <- err
            }
        }(chunk)
    }
    wg.Wait()
    close(errCh)
    return firstError(errCh)
}
```
Hoặc dùng `golang.org/x/sync/errgroup` với `SetLimit(maxWorkers)` (Go 1.20+) — API gọn hơn tự viết semaphore thủ công.

## Cursor-based pagination với `database/sql`

```go
// KHÔNG dùng OFFSET lớn — dùng keyset
func FetchNextChunk(ctx context.Context, db *sql.DB, lastID int64, size int) ([]Record, error) {
    rows, err := db.QueryContext(ctx,
        `SELECT id, payload FROM raw_records WHERE id > $1 ORDER BY id LIMIT $2`, lastID, size)
    if err != nil { return nil, err }
    defer rows.Close()

    records := make([]Record, 0, size) // pre-allocate capacity đã biết trước — tránh nhiều lần grow slice
    for rows.Next() {
        var r Record
        if err := rows.Scan(&r.ID, &r.Payload); err != nil { return nil, err }
        records = append(records, r)
    }
    return records, rows.Err()
}
```

## Checkpoint/resume

```go
type Checkpoint struct {
    JobID       string
    LastID      int64
    ProcessedAt time.Time
}

func RunResumableJob(ctx context.Context, jobID string) error {
    cp, err := loadCheckpoint(ctx, jobID) // nil nếu job chưa từng chạy
    lastID := int64(0)
    if cp != nil { lastID = cp.LastID }

    for {
        chunk, err := FetchNextChunk(ctx, db, lastID, chunkSize)
        if err != nil { return err }
        if len(chunk) == 0 { break }

        if err := processChunk(ctx, chunk); err != nil {
            return err // dừng job, checkpoint đã lưu ở lần thành công gần nhất — chạy lại resume từ đó
        }
        lastID = chunk[len(chunk)-1].ID
        if err := saveCheckpoint(ctx, jobID, lastID); err != nil { return err } // lưu định kỳ mỗi chunk
    }
    return nil
}
```

## Cảnh báo Go-specific

- `ioutil.ReadAll`/`os.ReadFile` tiện nhưng load hết vào `[]byte` — chỉ dùng khi biết chắc file nhỏ (config, template), không dùng cho file dữ liệu người dùng upload.
- Slice trong Go tự động grow (thường gấp đôi) khi `append` vượt capacity — nếu biết trước kích thước, luôn `make([]T, 0, knownSize)` để tránh nhiều lần cấp phát lại + copy ngầm (xem thêm skill `memory-optimization`).


---


# 📄 stream-batch-processing/references/java-spring.md

# Stream/Batch Processing với Java 21 / Spring Boot

> Dựa trên kinh nghiệm thực tế PDMS: Apache POI SAX streaming validation cho Excel, ETL 10M+ record credit migration data (3 loại sheet), 200K+ record/sheet với staging table + PostgreSQL validation stored procedure.

## Apache POI SAX streaming — đọc Excel lớn không load hết vào memory

`XSSFWorkbook` thông thường load toàn bộ file vào memory (DOM-based) — với file vài trăm nghìn dòng sẽ OOM. Dùng SAX-based streaming API:

```java
try (OPCPackage pkg = OPCPackage.open(filePath, PackageAccess.READ)) {
    XSSFReader reader = new XSSFReader(pkg);
    SharedStrings sst = reader.getSharedStringsTable(); // cần cho cell dạng shared string
    StylesTable styles = reader.getStylesTable();

    XMLReader xmlReader = XMLReaderFactory.createXMLReader();
    xmlReader.setContentHandler(new XSSFSheetXMLHandler(
        styles, sst,
        new SheetContentsHandler() {
            @Override
            public void cell(String cellRef, String formattedValue, XSSFComment comment) {
                // xử lý TỪNG cell ngay khi parser đọc tới — không giữ lại trong memory
            }
            @Override
            public void endRow(int rowNum) {
                // 1 dòng đã đọc xong → validate, gom vào buffer nhỏ để batch insert (xem dưới)
            }
        }, false));

    try (InputStream sheet = reader.getSheetsData().next()) {
        xmlReader.parse(new InputSource(sheet)); // parse tuần tự, memory O(1) theo kích thước file
    }
}
```
Xử lý lỗi Date column trong streaming mode (đã gặp ở PDMS): SAX handler không tự parse kiểu Date như POI DOM API — phải tự detect cell style number format (`styles.getStyleAt(styleIdx)`) để biết cell có phải date không, rồi convert `formattedValue` thủ công. Ghi lỗi ra file riêng (row number + cell ref + lý do) thay vì throw exception làm dừng cả file.

## JPA Batch Insert — bật đúng config, tránh bẫy phổ biến

```properties
# application.yml
spring.jpa.properties.hibernate.jdbc.batch_size=500
spring.jpa.properties.hibernate.order_inserts=true
spring.jpa.properties.hibernate.order_updates=true
spring.jpa.properties.hibernate.jdbc.batch_versioned_data=true
```
**Bẫy phổ biến khiến batch insert KHÔNG hoạt động dù đã set `batch_size`:**
1. Entity dùng `GenerationType.IDENTITY` — Hibernate BẮT BUỘC insert từng dòng để lấy ID sinh ra ngay (không batch được). Dùng `SEQUENCE` hoặc `TABLE` strategy nếu cần batch insert thật sự.
2. Không gọi `flush()` + `clear()` định kỳ trong vòng lặp — Persistence Context giữ hết entity trong memory (giống load-all), phải flush theo chunk:
```java
@Transactional
public void bulkInsert(Stream<RawRecord> records) {
    AtomicInteger counter = new AtomicInteger();
    records.forEach(record -> {
        entityManager.persist(mapToEntity(record));
        if (counter.incrementAndGet() % BATCH_SIZE == 0) {
            entityManager.flush();
            entityManager.clear(); // giải phóng Persistence Context — bước hay bị quên nhất
        }
    });
}
```

## Cursor/Keyset pagination với Spring Data

```java
// KHÔNG dùng Pageable với offset lớn trên bảng triệu record
// DÙNG keyset:
@Query("SELECT r FROM RawRecord r WHERE r.id > :lastId ORDER BY r.id ASC")
List<RawRecord> findNextChunk(@Param("lastId") Long lastId, Pageable limit);

Long lastId = 0L;
List<RawRecord> chunk;
do {
    chunk = repo.findNextChunk(lastId, PageRequest.of(0, CHUNK_SIZE));
    process(chunk);
    if (!chunk.isEmpty()) lastId = chunk.get(chunk.size() - 1).getId();
} while (!chunk.isEmpty());
```
Hoặc dùng `@QueryHints({@QueryHint(name = HINT_FETCH_SIZE, value = "500")})` + `Stream<T>` return type của Spring Data để DB driver stream kết quả thay vì buffer hết (cần `hibernate.jdbc.fetch_size` phù hợp và transaction mở suốt thời gian đọc).

## Staging table pattern (kinh nghiệm migration PDMS)

Với ETL 10M+ record, đọc trực tiếp từ file rồi ghi thẳng vào bảng nghiệp vụ chính rủi ro cao (lock contention, khó rollback 1 phần). Pattern đã dùng:
1. Đọc file theo chunk → insert vào **staging table** (không ràng buộc FK chặt, cho phép dữ liệu thô/lỗi)
2. Chạy **PostgreSQL stored procedure** validate + transform từ staging → bảng chính, theo batch, trong transaction riêng cho từng batch (không phải 1 transaction khổng lồ cho cả 10M record)
3. Ghi lại **checkpoint** (batch cuối đã xử lý thành công) vào bảng riêng — job restart được từ batch tiếp theo, không chạy lại từ đầu
4. **Snapshot table** để xử lý race condition khi có ETL job chạy song song với traffic ghi thật (đã áp dụng ở PDMS)

## Kafka consumer batching

```java
@KafkaListener(topics = "document-events", containerFactory = "batchFactory")
public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
    // xử lý theo batch, giảm round-trip DB
    processBatch(records);
    ack.acknowledge(); // commit offset SAU khi xử lý xong toàn batch — đảm bảo at-least-once, không mất dữ liệu khi crash giữa chừng
}
```
```properties
spring.kafka.consumer.max-poll-records=500
spring.kafka.listener.type=batch
```

## Bounded parallelism qua Virtual Threads (Java 21)

```java
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    Semaphore limiter = new Semaphore(20); // giới hạn 20 tác vụ đồng thời gọi DB/external service
    List<Future<?>> futures = chunks.stream().map(chunk -> executor.submit(() -> {
        limiter.acquireUninterruptibly();
        try { processChunk(chunk); } finally { limiter.release(); }
    })).toList();
    for (var f : futures) f.get();
}
```
Virtual thread rẻ (không tốn OS thread) nên tạo nhiều task nhỏ không sao, nhưng vẫn cần giới hạn số lượng request đồng thời tới DB/service downstream bằng Semaphore — bounded parallelism là giới hạn ở tài nguyên downstream, không phải ở chi phí thread.


---


# 📄 stream-batch-processing/references/rust.md

# Stream/Batch Processing với Rust

## `Iterator` — lazy evaluation là mặc định, tận dụng thay vì collect sớm

```rust
// ❌ Load-all: collect() ép toàn bộ vào Vec trước khi xử lý
let records: Vec<Record> = read_all_lines(path)?.into_iter().map(parse).collect();
for r in records { process(r); }

// ✅ Streaming: iterator chain lazy, mỗi record chỉ tồn tại tại 1 thời điểm
let file = File::open(path)?;
let reader = BufReader::new(file);
for line in reader.lines() {
    let line = line?;
    match parse_record(&line) {
        Ok(record) => process(record)?,
        Err(e) => record_error(&line, e), // ghi lỗi riêng, không dừng cả file
    }
}
```
Iterator adapter (`.map()`, `.filter()`, `.take()`) không thực thi gì cho tới khi có consumer cuối (`for`, `.collect()`, `.sum()`) — tự nhiên tạo pipeline streaming mà không cần thư viện riêng.

## `async Stream` — cho I/O bất đồng bộ (DB, network, Kafka)

```rust
use futures::stream::{self, StreamExt, TryStreamExt};

async fn process_large_query(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut stream = sqlx::query_as::<_, Record>("SELECT id, payload FROM raw_records ORDER BY id")
        .fetch(pool); // trả về Stream, KHÔNG buffer hết kết quả — driver stream từng row qua network

    let mut batch = Vec::with_capacity(CHUNK_SIZE);
    while let Some(record) = stream.try_next().await? {
        batch.push(record);
        if batch.len() >= CHUNK_SIZE {
            insert_batch(pool, &batch).await?;
            batch.clear(); // giữ capacity đã cấp phát, tránh re-allocate mỗi batch
        }
    }
    if !batch.is_empty() {
        insert_batch(pool, &batch).await?;
    }
    Ok(())
}
```
`sqlx::query_as(...).fetch(pool)` trả `Stream` thay vì `fetch_all()` — khác biệt quan trọng nhất khi xử lý bảng triệu record: `fetch_all()` buffer hết vào `Vec` trong memory trước khi trả về.

## Bounded concurrency qua `Semaphore` + `buffer_unordered`

```rust
use tokio::sync::Semaphore;
use std::sync::Arc;

async fn process_concurrently(chunks: Vec<Chunk>, max_concurrent: usize) -> Result<(), Error> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    stream::iter(chunks)
        .map(|chunk| {
            let sem = semaphore.clone();
            async move {
                let _permit = sem.acquire().await.unwrap(); // block tới khi có slot trống
                process_chunk(chunk).await
            }
        })
        .buffer_unordered(max_concurrent) // chạy tối đa max_concurrent future cùng lúc
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
```
`buffer_unordered(n)` là cách idiomatic nhất ở Rust để giới hạn song song trên 1 stream — không cần tự quản lý `JoinHandle` thủ công cho use case đơn giản.

## Checkpoint/resume

```rust
#[derive(sqlx::FromRow)]
struct Checkpoint { job_id: String, last_id: i64 }

async fn run_resumable_job(pool: &PgPool, job_id: &str) -> Result<(), Error> {
    let mut last_id = load_checkpoint(pool, job_id).await?.map(|c| c.last_id).unwrap_or(0);

    loop {
        let chunk = fetch_next_chunk(pool, last_id, CHUNK_SIZE).await?;
        if chunk.is_empty() { break; }

        process_chunk(&chunk).await?; // lỗi ở đây → job dừng, checkpoint vẫn giữ tiến độ lần trước
        last_id = chunk.last().unwrap().id;
        save_checkpoint(pool, job_id, last_id).await?; // lưu định kỳ mỗi chunk
    }
    Ok(())
}
```

## Zero-copy parsing khi có thể — tránh clone không cần thiết

```rust
// ❌ Mỗi field clone ra String riêng — cấp phát heap nhiều lần không cần thiết
fn parse_slow(line: &str) -> Record {
    let parts: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
    Record { id: parts[0].clone(), name: parts[1].clone() }
}

// ✅ Borrow trực tiếp từ input, chỉ clone khi thật sự cần giữ lâu dài (VD: đưa vào struct sống lâu hơn buffer)
fn parse_fast<'a>(line: &'a str) -> RecordRef<'a> {
    let mut parts = line.split(',');
    RecordRef { id: parts.next().unwrap(), name: parts.next().unwrap() }
}
```
Dùng `Cow<'a, str>` khi phần lớn trường hợp không cần sửa đổi dữ liệu (borrow được) nhưng thỉnh thoảng cần owned (VD: sau khi trim/normalize) — tránh phải luôn `to_string()`.

## Liên hệ BPMP Engine

Engine Rust của BPMP xử lý WIR (Workflow Intermediate Representation) và event stream qua Kafka/Redpanda — áp dụng đúng pattern `async Stream` + `buffer_unordered` ở tầng infra-kafka khi consume batch event, giữ domain/application crate không phụ thuộc trực tiếp vào chi tiết streaming (xem skill `clean-architecture-webapp`, phần Dependency Rule).

## Cảnh báo Rust-specific

- `.collect::<Vec<_>>()` giữa chừng pipeline là dấu hiệu hay gặp nhất của việc vô tình phá lazy evaluation — chỉ collect ở điểm THẬT SỰ cần toàn bộ dữ liệu cùng lúc (VD: cần sort trước khi ghi).
- `fetch_all()` của sqlx tiện cho query nhỏ (config, lookup table) — không dùng cho bảng lớn, luôn `fetch()` (stream) cho dữ liệu không giới hạn kích thước trước.
