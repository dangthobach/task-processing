# Skill: memory-optimization

> Tối ưu bộ nhớ cho backend/webapp — đo trước khi tối ưu (profiling), GC tuning (Java G1/ZGC), tránh allocation thừa (autoboxing, unnecessary clone/copy), object pooling, off-heap, escape analysis. LUÔN dùng skill này khi user nhắc tới "OutOfMemory", "OOM", "memory leak", "tối ưu bộ nhớ", "GC pause", "heap tăng dần", "process bị OOMKilled trên k8s", hoặc khi review code có pattern cấp phát nhiều object tạm thời trong vòng lặp lớn. Áp dụng cho Java/Spring Boot (JVM), Go và Rust — mỗi runtime có mô hình quản lý bộ nhớ khác hẳn nhau.

# Tối ưu Bộ nhớ cho Backend/Webapp

## Nguyên tắc số 1: Đo trước, tối ưu sau — không đoán

Tối ưu bộ nhớ dựa trên đoán (guess) gần như luôn sai chỗ. Quy trình bắt buộc:
1. **Xác định triệu chứng cụ thể**: OOMKilled trên K8s? Heap tăng dần không giảm (leak)? GC pause dài gây latency spike? RSS cao hơn heap dự kiến?
2. **Profile để tìm allocation hotspot thật sự** trước khi sửa code — công cụ theo ngôn ngữ ở phần dưới.
3. **Sửa đúng chỗ đo được**, đo lại để xác nhận cải thiện — không sửa "cho chắc" ở nơi chưa đo.

## 3 mô hình quản lý bộ nhớ — khác nhau căn bản, không áp kỹ thuật chéo

| | JVM (Java) | Go | Rust |
|---|---|---|---|
| Cơ chế | GC (tracing, generational) | GC (concurrent, low-latency hơn JVM cũ nhưng vẫn có STW ngắn) | Không GC — ownership/borrow checker tại compile time |
| Chi phí | GC pause, heap sizing, off-heap phức tạp | GC pause ngắn hơn nhưng vẫn tồn tại; escape analysis quyết định stack/heap | Không có runtime GC cost, nhưng lỗi ownership bắt ở compile time, phải thiết kế đúng từ đầu |
| Công cụ đo | JFR (Java Flight Recorder), async-profiler, `jcmd`, heap dump + Eclipse MAT | `pprof` (`go tool pprof`), `runtime/trace` | `valgrind --tool=massif`, `heaptrack`, `dhat`, hoặc đơn giản đo qua `jemalloc`/`tikv-jemallocator` stats |

Không mang tư duy "tune GC" từ Java sang Go/Rust một cách máy móc — Go GC tuning chỉ có vài biến (`GOGC`, `GOMEMLIMIT`), Rust hoàn toàn không có khái niệm này.

## Nguyên tắc chung across mọi ngôn ngữ

1. **Streaming thay vì buffer toàn bộ** — đây là kỹ thuật tối ưu bộ nhớ hiệu quả nhất, xem chi tiết ở skill `stream-batch-processing`; phần lớn vấn đề OOM trong webapp là do load-all thay vì stream, không phải do GC/allocator.
2. **Pre-allocate khi biết trước kích thước** (`ArrayList(capacity)`, `make([]T, 0, n)`, `Vec::with_capacity(n)`) — tránh nhiều lần cấp phát lại + copy khi cấu trúc tự grow.
3. **Tránh giữ reference sống lâu hơn cần thiết** — cache không có eviction policy, static field tích luỹ dần, listener không unregister đều là nguyên nhân leak phổ biến bất kể ngôn ngữ.
4. **Object pooling chỉ đáng làm khi đo thấy allocation/GC pressure thật sự là bottleneck** — pooling tăng độ phức tạp code (phải quản lý vòng đời thủ công), không nên áp dụng mặc định.
5. **Batch size và buffer size phải có giới hạn trên (cap)**, không phụ thuộc hoàn toàn vào input — 1 request với input bất thường lớn không được phép làm process OOM.

## Checklist khi nhận task "tối ưu bộ nhớ"

- [ ] Đã có số liệu cụ thể chưa (heap dump, profiler output, biểu đồ RSS theo thời gian)? Nếu chưa, làm bước này trước.
- [ ] Vấn đề là **leak** (tăng dần không giảm) hay **peak usage cao** (spike rồi giảm)? Hai loại cần hướng sửa khác nhau hoàn toàn.
- [ ] Đã loại trừ nguyên nhân phổ biến nhất (load-all thay vì stream) trước khi đi sâu vào GC tuning/allocator tuning chưa?
- [ ] Fix có đo lại để xác nhận cải thiện thật, không chỉ "có vẻ đỡ hơn"?

## Tài liệu tham khảo theo ngôn ngữ

- `references/java-spring.md` — JFR/async-profiler, G1GC vs ZGC, escape analysis, tránh autoboxing, JPA first/second-level cache pitfalls gây phình heap, off-heap options
- `references/go.md` — `pprof` heap profiling, `GOGC`/`GOMEMLIMIT`, escape analysis (`go build -gcflags="-m"`), `sync.Pool`, tránh interface boxing
- `references/rust.md` — không GC nhưng vẫn "leak" được (`Rc` cycle, unbounded `Vec`/cache), `Box`/`Rc`/`Arc` trade-off, arena allocator, đo bằng `heaptrack`/`dhat`


---


# 📄 memory-optimization/references/go.md

# Tối ưu Bộ nhớ với Go

## Đo trước: `pprof` — built-in, dùng ngay không cần cài thêm gì lớn

```go
import _ "net/http/pprof"

func main() {
    go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }() // expose pprof endpoint
    // ... phần còn lại của app
}
```
```bash
# Heap profile hiện tại — xem object nào đang chiếm memory
go tool pprof http://localhost:6060/debug/pprof/heap

# Trong pprof interactive: top, list <function>, web (cần graphviz) để xem chi tiết
```
Với batch job không có HTTP server, dùng trực tiếp trong code:
```go
f, _ := os.Create("heap.prof")
pprof.WriteHeapProfile(f)
f.Close()
```

## `GOGC` và `GOMEMLIMIT` — 2 biến quan trọng nhất để tune GC của Go

```bash
GOGC=100         # default — GC chạy khi heap tăng gấp đôi so với sau lần GC trước; giảm xuống (VD 50) = GC chạy thường xuyên hơn, tốn CPU nhưng giữ heap thấp hơn
GOMEMLIMIT=512MiB # (Go 1.19+) đặt trần cứng cho memory — Go runtime tự điều chỉnh GC để không vượt ngưỡng này, RẤT quan trọng khi chạy trong container K8s có memory limit (tránh OOMKilled)
```
Trên K8s, luôn set `GOMEMLIMIT` thấp hơn container memory limit một chút (VD limit 1Gi thì set `GOMEMLIMIT=900MiB`) — Go GC dựa vào con số này để chủ động GC sớm hơn trước khi bị kernel OOMKill, thay vì chỉ dựa vào `GOGC` (vốn không biết gì về container limit).

## Escape Analysis — kiểm tra được trực tiếp qua compiler flag

```bash
go build -gcflags="-m" ./... 2>&1 | grep "escapes to heap"
```
Cho biết chính xác biến nào bị đẩy lên heap thay vì ở stack (thường do: trả về pointer từ function, lưu vào interface, capture trong closure sống lâu hơn function). Không cần đoán — chạy lệnh này trên hot path để biết chỗ nào đáng tối ưu.

```go
// ❌ Trả về pointer buộc struct escape lên heap dù chỉ cần trong scope ngắn
func newRecord() *Record { r := Record{}; return &r }

// ✅ Trả về value khi không cần chia sẻ ownership — compiler được tự do giữ ở stack
func newRecord() Record { return Record{} }
```

## `sync.Pool` — object pooling, chỉ dùng khi đã đo thấy GC pressure thật

```go
var bufferPool = sync.Pool{
    New: func() interface{} { return make([]byte, 0, 4096) },
}

func processRequest() {
    buf := bufferPool.Get().([]byte)
    defer bufferPool.Put(buf[:0]) // reset length trước khi trả về pool, giữ capacity

    // dùng buf cho xử lý tạm thời, tránh cấp phát []byte mới mỗi request
}
```
`sync.Pool` phù hợp cho object tạo/huỷ liên tục với tần suất cao (buffer xử lý request, encoder/decoder tạm) trong service có traffic lớn. KHÔNG dùng cho object cần giữ state lâu dài hoặc số lượng object nhỏ — pool có overhead riêng (đồng bộ hoá), chỉ đáng khi đã profile thấy allocation ở đây là hotspot.

## Tránh interface boxing thừa

```go
// ❌ []interface{} với giá trị nguyên thuỷ (int, string) gây boxing — mỗi phần tử alloc riêng
var items []interface{}
for i := 0; i < 1_000_000; i++ { items = append(items, i) }

// ✅ Dùng generic (Go 1.18+) hoặc concrete type — không boxing
var items []int
for i := 0; i < 1_000_000; i++ { items = append(items, i) }
```

## Pre-allocate slice/map khi biết trước kích thước

```go
// ❌ append liên tục khiến Go phải grow + copy slice nhiều lần (thường gấp đôi mỗi lần vượt capacity)
var results []Record
for _, r := range rawData { results = append(results, transform(r)) }

// ✅ Biết trước kích thước → pre-allocate 1 lần
results := make([]Record, 0, len(rawData))
for _, r := range rawData { results = append(results, transform(r)) }

// Map cũng tương tự
cache := make(map[string]Record, expectedSize)
```

## Leak phổ biến ở Go dù có GC

- **Goroutine leak**: goroutine block mãi mãi trên channel không ai gửi/nhận (thường do thiếu `context` cancellation) — GC không thu hồi được stack của goroutine đang "sống" dù không làm gì. Luôn truyền `context.Context` và có đường thoát (`select { case <-ctx.Done(): return }`).
- **Slice giữ reference tới array lớn hơn nhiều so với phần thực dùng**: `sub := large[:10]` vẫn giữ toàn bộ backing array của `large` sống trong memory. Dùng `append([]T{}, large[:10]...)` để copy ra slice mới độc lập nếu cần giải phóng phần còn lại.
- **Map không xoá key cũ**: Go map không tự shrink khi xoá phần tử (memory được giữ lại cho tới khi map bị GC hoàn toàn) — với cache dài hạn cần map, cân nhắc cấu trúc có TTL/eviction (LRU cache library) thay vì map trần.


---


# 📄 memory-optimization/references/java-spring.md

# Tối ưu Bộ nhớ với Java 21 / JVM

## Đo trước: JFR (Java Flight Recorder) — miễn phí, built-in, overhead thấp

```bash
# Bật JFR khi start app (production-safe, overhead thường <1-2%)
java -XX:+FlightRecorder -XX:StartFlightRecording=duration=60s,filename=recording.jfr -jar app.jar

# Hoặc gắn vào process đang chạy
jcmd <pid> JFR.start duration=60s filename=recording.jfr
```
Mở file `.jfr` bằng JDK Mission Control (JMC) hoặc `async-profiler` để xem: allocation profile (object nào cấp phát nhiều nhất), GC pause timeline, thread state. Đây luôn là bước đầu tiên trước khi đoán nguyên nhân.

## Heap dump khi nghi ngờ leak

```bash
jcmd <pid> GC.heap_dump /tmp/heap.hprof
```
Mở bằng Eclipse MAT (Memory Analyzer Tool) — dùng "Leak Suspects Report" để tìm object retain nhiều nhất và GC root giữ nó sống. Trong context Spring: nguyên nhân leak phổ biến nhất là `@Cacheable` không có eviction policy, `ThreadLocal` không `remove()` sau khi dùng (đặc biệt nguy hiểm với thread pool tái sử dụng thread), hoặc listener/subscriber không unregister.

## G1GC vs ZGC — chọn theo mục tiêu latency

| | G1GC (default JDK 9+) | ZGC (production-ready từ JDK 15+, tốt hơn nhiều từ JDK 21) |
|---|---|---|
| Pause time | Vài chục-trăm ms, tăng theo heap size | Dưới 1-10ms gần như không đổi kể cả heap rất lớn |
| Throughput | Cao hơn ZGC ở workload thông thường | Đánh đổi throughput lấy pause time thấp |
| Khi nào chọn | Batch job, ETL (throughput quan trọng hơn latency) | Service phục vụ request real-time, SLA latency chặt (webapp/API tương tác người dùng) |

```bash
# ZGC cho service latency-sensitive
java -XX:+UseZGC -Xmx4g -jar app.jar

# G1GC cho batch/ETL job (thường không cần chỉnh, đã là default)
java -XX:+UseG1GC -Xmx8g -jar migration-job.jar
```

## Escape Analysis — vì sao không phải object nào cũng lên heap

JIT compiler có thể phát hiện object không "thoát" khỏi scope của method (không được return, không lưu vào field) và cấp phát trên stack thay vì heap (hoặc loại bỏ hoàn toàn qua scalar replacement) — không cần code làm gì đặc biệt, nhưng cần tránh:
- Truyền object qua interface method không inline được (escape analysis khó xuyên qua virtual call)
- Lưu object tạm vào collection dùng chung (buộc phải escape lên heap)

Không cần chủ động "viết code để escape analysis hoạt động" — chỉ cần biết đây là lý do vì sao 1 số benchmark vi mô cho kết quả khó đoán, và không nên tự thêm object pooling cho object nhỏ, ngắn hạn (escape analysis + G1GC thường đã xử lý tốt object trẻ).

## Tránh autoboxing thừa trong vòng lặp lớn

```java
// ❌ Mỗi phần tử tạo 1 Integer object (autoboxing), với 10 triệu record = 10 triệu object rác
List<Integer> ids = new ArrayList<>();
for (int i = 0; i < 10_000_000; i++) ids.add(i);

// ✅ Dùng primitive stream/array khi không thực sự cần Object semantics
int[] ids = IntStream.range(0, 10_000_000).toArray(); // không autoboxing
```
Trong ETL/migration xử lý hàng triệu record (bối cảnh PDMS), kiểm tra kỹ các cấu trúc dữ liệu trung gian (`Map<Long, ...>`, `List<Integer>`) — cân nhắc thư viện primitive collection (Eclipse Collections, fastutil) khi số lượng đủ lớn để chi phí boxing đáng kể.

## JPA/Hibernate — nguyên nhân phình heap đặc thù Spring Data

1. **First-level cache (Persistence Context) không clear** trong vòng lặp xử lý lớn — mọi entity `persist()`/`find()` đều bị giữ lại tới khi transaction kết thúc. Bắt buộc `flush()` + `clear()` định kỳ (xem chi tiết ở skill `stream-batch-processing`).
2. **Second-level cache không giới hạn kích thước** — nếu bật `@Cacheable` ở entity, phải cấu hình eviction policy (LRU + max size) qua provider (Ehcache/Caffeine), không để cache tự do phình theo số lượng entity distinct.
3. **N+1 query + eager fetch trên collection lớn** — không trực tiếp là "memory leak" nhưng gây spike memory tức thời khi load 1 entity kéo theo hàng nghìn related entity qua `FetchType.EAGER`. Luôn `FetchType.LAZY` mặc định, chỉ eager khi đã đo thấy cần.

## Off-heap — khi nào cân nhắc

Với cache lớn (hàng GB) hoặc dữ liệu cần giữ lâu nhưng không muốn tăng áp lực GC lên heap, cân nhắc off-heap storage (Chronicle Map, Ehcache off-heap tier). Chỉ đáng làm khi đã đo thấy GC pause do heap lớn là bottleneck thật — off-heap tăng độ phức tạp (serialization cost, không được JVM quản lý tự động).

## Virtual Thread (JDK 21) và bộ nhớ

Virtual thread có stack rất nhẹ (không cố định 1MB như platform thread) — cho phép tạo hàng trăm nghìn virtual thread mà không tốn nhiều memory như platform thread tương ứng. Tuy nhiên `ThreadLocal` dùng trong virtual thread vẫn tốn memory theo số lượng thread đang sống — cẩn thận nếu code cũ dùng `ThreadLocal` nhiều khi migrate sang virtual thread với concurrency cao (hàng trăm nghìn thread).


---


# 📄 memory-optimization/references/rust.md

# Tối ưu Bộ nhớ với Rust

## Không có GC không nghĩa là không "leak" được

Rust không có garbage collector, nhưng vẫn hoàn toàn có thể giữ memory không cần thiết:
- `Rc<RefCell<T>>` tạo chu trình tham chiếu (reference cycle) — 2 object trỏ vòng qua nhau qua `Rc` không bao giờ về 0 reference count, không bao giờ được giải phóng.
- `Vec`/`HashMap`/cache không có giới hạn kích thước hoặc eviction policy — về bản chất giống leak dù compiler không cảnh báo gì (đây là logic lỗi, không phải memory-safety lỗi mà Rust bắt được).
- Buffer cấp phát lớn nhưng chỉ dùng 1 phần rồi giữ mãi trong struct sống lâu.

## Đo trước: `heaptrack` / `dhat` / `valgrind massif`

```bash
# heaptrack — profiler heap phổ biến nhất cho Rust/C++, xem allocation theo call stack
heaptrack ./target/release/my-service
heaptrack_gui heaptrack.my-service.<pid>.gz

# dhat — tích hợp trực tiếp vào code Rust, chỉ bật khi cần (feature flag)
```
```rust
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    // ... chạy app, dhat sinh report allocation khi profiler drop
}
```
Với BPMP Engine (Rust, chạy dài hạn với OpenRaft/RocksDB), nên có build variant bật `dhat`/`heaptrack` riêng cho môi trường staging để định kỳ kiểm tra allocation pattern, không bật ở production (overhead đáng kể).

## `Box`, `Rc`, `Arc` — chọn đúng loại con trỏ, không mặc định dùng `Arc` "cho an toàn"

| Loại | Khi dùng | Chi phí |
|---|---|---|
| Giá trị trực tiếp (không con trỏ) | Mặc định, luôn ưu tiên trước | Không có overhead, nằm trên stack hoặc inline trong struct cha |
| `Box<T>` | Cần heap allocation (kích thước không biết compile-time, recursive type, trait object `Box<dyn Trait>`) nhưng chỉ 1 owner | 1 lần alloc, không có reference counting |
| `Rc<T>` | Nhiều owner trong CÙNG 1 thread, không cần thread-safe | Overhead reference counting (không atomic, rẻ) |
| `Arc<T>` | Nhiều owner CHIA SẺ GIỮA nhiều thread (như hầu hết service async/tokio) | Overhead reference counting atomic (đắt hơn `Rc`) — chỉ dùng khi thật sự cần chia sẻ giữa thread |

Lỗi thường gặp: dùng `Arc<Mutex<T>>` mặc định cho state chỉ cần đọc, không cần ghi đồng thời — cân nhắc `Arc<T>` trần (nếu bất biến) hoặc `ArcSwap`/`RwLock` (nếu đọc nhiều ghi ít) thay vì `Mutex` khoá toàn bộ mỗi lần đọc.

## Tránh `clone()` thừa — dấu hiệu hay gặp nhất khi mới quen borrow checker

```rust
// ❌ Clone để "cho dễ", đặc biệt phổ biến khi mới học Rust và bị borrow checker chặn
fn process(data: Vec<Record>) -> Vec<Record> {
    let mut result = data.clone(); // clone toàn bộ Vec không cần thiết
    result.iter_mut().for_each(|r| r.normalize());
    result
}

// ✅ Nhận ownership trực tiếp nếu caller không cần giữ lại data gốc — không cần clone
fn process(mut data: Vec<Record>) -> Vec<Record> {
    data.iter_mut().for_each(|r| r.normalize());
    data
}

// ✅ Hoặc borrow nếu chỉ cần đọc, không cần ownership
fn summarize(data: &[Record]) -> Summary { /* ... */ }
```
Quy tắc: mỗi lần viết `.clone()`, tự hỏi "caller có thực sự cần giữ bản gốc sau lời gọi này không?" — nếu không, chuyển sang nhận ownership (`T` thay vì `&T` rồi `.clone()`).

## `Cow<'a, T>` — borrow khi có thể, owned khi cần, không phải luôn 1 trong 2

```rust
use std::borrow::Cow;

fn normalize(input: &str) -> Cow<str> {
    if input.chars().all(|c| !c.is_uppercase()) {
        Cow::Borrowed(input) // trường hợp phổ biến (đã lowercase sẵn): không alloc gì cả
    } else {
        Cow::Owned(input.to_lowercase()) // chỉ alloc khi thực sự cần biến đổi
    }
}
```
Hữu ích trong pipeline xử lý dữ liệu lớn (ETL) khi phần lớn record không cần transform — tránh alloc `String` mới cho MỌI record khi chỉ 1 phần nhỏ thực sự cần.

## Arena allocator — khi cấp phát/giải phóng theo batch cùng lúc

Với workload cấp phát nhiều object nhỏ có cùng vòng đời (VD: parse 1 request, dùng xong giải phóng hết cùng lúc), arena allocator (`bumpalo` crate) nhanh hơn nhiều so với cấp phát từng object qua allocator mặc định — 1 lần cấp phát block lớn, "giải phóng" bằng cách reset con trỏ thay vì gọi `drop` từng object.
```rust
let arena = bumpalo::Bump::new();
let record = arena.alloc(Record::new()); // cấp phát trong arena, không gọi system allocator riêng
// arena.reset() hoặc drop(arena) giải phóng TOÀN BỘ 1 lần
```
Chỉ đáng dùng khi đã đo thấy allocator mặc định (`malloc`/`jemalloc`) là bottleneck thật với pattern "nhiều alloc nhỏ, giải phóng đồng loạt" — không phải lựa chọn mặc định cho code thông thường.

## `#[global_allocator]` — đổi allocator toàn cục khi cần

```rust
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```
`jemalloc` thường giảm fragmentation tốt hơn allocator mặc định của glibc cho service long-running với allocation pattern đa dạng (đúng đặc điểm 1 service webapp/Engine chạy dài hạn) — cân nhắc bật cho BPMP Engine nếu profiling cho thấy fragmentation là vấn đề, đo trước/sau bằng RSS theo thời gian để xác nhận.

## Cảnh báo Rust-specific

- Đừng dùng `Arc<Mutex<T>>` như phản xạ mặc định mỗi khi cần shared state — luôn tự hỏi có thật sự cần multi-thread ownership không, hay `&T`/ownership rõ ràng đã đủ.
- `Vec::with_capacity` quan trọng tương đương Java `ArrayList(capacity)`/Go `make([]T, 0, n)` — luôn dùng khi biết trước kích thước, tránh nhiều lần realloc + copy ngầm khi `Vec` tự grow.
