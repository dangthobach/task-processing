# Skill: design-patterns-apply

> Áp dụng đầy đủ 23 design pattern GoF (Creational, Structural, Behavioral) + enterprise pattern (Repository, Unit of Work, CQRS, Saga, Circuit Breaker) để tổ chức mã nguồn clean, khoa học, dễ mở rộng — Singleton, Factory Method, Abstract Factory, Builder, Prototype, Adapter, Bridge, Composite, Decorator, Facade, Flyweight, Proxy, Chain of Responsibility, Command, Interpreter, Iterator, Mediator, Memento, Observer, State, Strategy, Template Method, Visitor. LUÔN dùng skill này khi user hỏi "nên dùng pattern nào", muốn liệt kê/tra cứu pattern GoF, khi thấy code có nhiều if/else hoặc switch lặp lại theo loại, khi cần thêm biến thể xử lý mới mà không muốn sửa code cũ, khi thiết kế cách gọi external service có thể lỗi, hoặc khi nhắc tới "design pattern", "GoF", "refactor", "extensible", "pluggable", "tổ chức code khoa học". Áp dụng cho Java/Spring Boot, Go và Rust — mỗi ngôn ngữ có idiom khác nhau, KHÔNG áp GoF kiểu Java 1:1 sang Go/Rust.

# Design Pattern cho Webapp/Backend

## Nguyên tắc chọn pattern

1. **Pattern giải quyết một triệu chứng cụ thể, không phải trang trí.** Trước khi áp dụng, xác định triệu chứng đang gặp (bảng dưới), rồi mới chọn pattern tương ứng.
2. **Ngôn ngữ khác nhau → idiom khác nhau.** GoF viết cho ngôn ngữ OOP có inheritance (C++/Smalltalk). Go không có inheritance — nhiều pattern (Factory, Strategy) đơn giản chỉ là function/closure/interface nhỏ, không cần class hierarchy. Rust dùng trait + enum thay cho nhiều pattern hành vi. Đọc `references/<ngôn ngữ>.md` để lấy idiom đúng thay vì dịch nguyên bản Java.
3. **Over-engineering là rủi ro thật, không phải lý thuyết suông.** Nếu chỉ có 1 biến thể và không có kế hoạch thêm biến thể thứ 2 trong tương lai gần, đừng dựng abstraction (interface + factory + registry) cho nó — dùng `if` đơn giản. Thêm pattern khi biến thể thứ 2 xuất hiện thật (Rule of Three).

## Danh mục đầy đủ 23 pattern GoF

Tra cứu nhanh cả 23 pattern kinh điển, phân theo 3 nhóm chuẩn của Gang of Four. Cột "Mức độ liên quan" đánh giá theo bối cảnh backend webapp/banking (khác với GUI/game nơi GoF được viết ra ban đầu) — dùng để tránh áp dụng pattern chỉ vì "có trong sách" mà không thực sự cần. Code mẫu chi tiết từng pattern nằm ở `references/<ngôn ngữ>.md`, tổ chức theo đúng 3 nhóm này.

### Creational (khởi tạo object)

| Pattern | Ý định | Liên quan trong backend | Ghi chú |
|---|---|---|---|
| **Singleton** | Đảm bảo 1 instance duy nhất, truy cập toàn cục | Thấp — cẩn trọng | Spring bean mặc định đã là singleton scope; tự viết Singleton thủ công (static instance) thường là anti-pattern che giấu global state, khó test. Ở Go/Rust càng hiếm cần vì DI thường thực hiện qua wiring tường minh ở composition root |
| **Factory Method** | 1 method tạo object, quyết định class cụ thể tại runtime | Cao | Chọn implementation theo input (loại document, loại message) — nền tảng của Strategy dispatch |
| **Abstract Factory** | Họ (family) nhiều factory tạo nhóm object liên quan tới nhau | Trung bình | Khi cần chọn cả 1 bộ dependency liên quan theo môi trường (VD: bộ driver DB + cache + queue khác nhau giữa dev/staging/prod) |
| **Builder** | Khởi tạo object phức tạp, nhiều field optional, từng bước | Cao | Object có nhiều field optional hoặc cần validate thứ tự khởi tạo |
| **Prototype** | Tạo object mới bằng clone object có sẵn thay vì `new` từ đầu | Thấp trong webapp thường | Phổ biến hơn ở đồ hoạ/game; trong backend có thể relevant khi cần clone Aggregate cho snapshot/versioning — liên hệ Memento bên dưới |

### Structural (tổ chức quan hệ giữa object)

| Pattern | Ý định | Liên quan trong backend | Ghi chú |
|---|---|---|---|
| **Adapter** | Chuyển interface không tương thích thành interface domain cần | Cao | Trùng khái niệm với "Adapter" trong Clean Architecture (skill `clean-architecture-webapp`) — cùng bản chất |
| **Bridge** | Tách abstraction khỏi implementation để cả 2 biến đổi độc lập | Trung bình | Nhiều driver/backend cùng chung 1 interface trừu tượng — liên hệ BPMP: `infra-postgres` vs `infra-raft` cùng implement 1 trait domain |
| **Composite** | Object dạng cây, xử lý đồng nhất node lá và node nhánh | Cao cho BPMP | BPMN workflow (task, gateway, sub-process) là cấu trúc cây/graph điển hình — Composite cho phép duyệt/thực thi đồng nhất mọi loại node |
| **Decorator** | Thêm hành vi quanh object có sẵn mà không sửa code gốc | Cao | AOP (Java), middleware (Go), newtype wrapper (Rust) |
| **Facade** | Expose API đơn giản cho hệ thống con phức tạp | Cao, cẩn thận | Dễ biến thành God Object nếu ôm quá nhiều trách nhiệm — giữ Facade mỏng, chỉ điều phối |
| **Flyweight** | Chia sẻ object bất biến để giảm bộ nhớ khi có nhiều instance giống nhau | Trung bình | Liên hệ trực tiếp skill `memory-optimization` — cache object dùng chung (VD: enum/lookup data lặp lại nhiều) thay vì tạo mới mỗi lần |
| **Proxy** | Object đại diện kiểm soát truy cập tới object thật (lazy load, cache, access control) | Cao | JPA lazy loading chính là Proxy pattern; authorization check trước khi gọi service thật cũng là Proxy |

### Behavioral (giao tiếp & phân chia trách nhiệm giữa object)

| Pattern | Ý định | Liên quan trong backend | Ghi chú |
|---|---|---|---|
| **Chain of Responsibility** | Nhiều bước xử lý tuần tự, mỗi bước có thể chặn/return sớm | Cao | Validation pipeline, middleware chain, approval workflow nhiều cấp |
| **Command** | Đóng gói 1 yêu cầu thành object, hỗ trợ queue/undo/log | Cao | CQRS command object, workflow task command (BPMP), audit log tự nhiên đi kèm vì request đã là object |
| **Interpreter** | Định nghĩa ngữ pháp và interpreter cho 1 ngôn ngữ nhỏ | Thấp trong webapp thường, Cao cho BPMP | BPMN/DMN expression evaluation, rule engine nghiệp vụ — hiếm cần trong CRUD service thông thường |
| **Iterator** | Duyệt tuần tự collection mà không lộ cấu trúc bên trong | Cao nhưng thường built-in | `for-each`/`Iterator` (Java), `range` (Go), `Iterator` trait (Rust) đã có sẵn ở ngôn ngữ — hiếm khi tự viết thủ công, biết để nhận diện khi nào KHÔNG cần tự implement |
| **Mediator** | Tập trung logic giao tiếp giữa nhiều object vào 1 điểm, giảm coupling trực tiếp | Trung bình | Workflow orchestrator/coordinator, Saga orchestration (bên dưới) là dạng Mediator |
| **Memento** | Lưu & khôi phục trạng thái object mà không lộ chi tiết bên trong | Cao cho BPMP | Checkpoint/snapshot của workflow instance — liên hệ trực tiếp checkpoint/resume trong skill `stream-batch-processing` và hướng Shadow Verification/Simulation của BPMP |
| **Observer** | Thông báo nhiều bên khi 1 sự kiện xảy ra, các bên không biết nhau | Cao | `ApplicationEventPublisher` (Spring), channel/pubsub (Go), Kafka cho cross-service |
| **State** | Hành vi object thay đổi theo trạng thái nội tại | Cao | Chính là StatefulEntity/generic state machine đã có trong skill `common-lib-base` — 2 khái niệm cùng bản chất |
| **Strategy** | Chọn 1 trong nhiều thuật toán/hành vi tại runtime | Cao | Interface hành vi + nhiều implementation, chọn qua map/registry thay vì switch |
| **Template Method** | Định nghĩa khung thuật toán cố định, để bước con override | Trung bình | Ít dùng ở Go/Rust (không có inheritance thật); ở Java hợp cho pipeline có bước cố định + bước tuỳ biến qua abstract method |
| **Visitor** | Tách thao tác khỏi cấu trúc object, thêm thao tác mới không sửa class object | Cao cho BPMP | Duyệt/biến đổi WIR (AST của workflow), tương tự compiler pass — thêm 1 loại xử lý mới (VD thêm 1 kiểu export) không cần sửa từng node type |

## Bảng tra nhanh: Triệu chứng → Pattern

| Triệu chứng trong code | Pattern | Ghi chú |
|---|---|---|
| `if/switch` theo loại object, lặp lại ở nhiều chỗ, mỗi lần thêm loại mới phải sửa nhiều nơi | **Strategy** | Định nghĩa interface hành vi, mỗi loại 1 implementation, chọn implementation qua map/registry thay vì switch |
| Khởi tạo object phức tạp, nhiều field optional, hoặc logic khởi tạo khác nhau theo điều kiện | **Builder** / **Factory** | Builder cho object nhiều field optional (dùng named param ở Go/Rust thay được nhiều trường hợp); Factory khi việc chọn implementation cụ thể phụ thuộc input runtime |
| Cần thêm hành vi (logging, cache, retry, audit) quanh 1 hành vi có sẵn mà không sửa code gốc | **Decorator** | Ở Spring: AOP/Interceptor; ở Go: middleware pattern (`func(Handler) Handler`); ở Rust: newtype wrapper implement cùng trait |
| Nhiều bước xử lý tuần tự, mỗi bước có thể chặn/return sớm, danh sách bước có thể thay đổi | **Chain of Responsibility** | Validation pipeline, middleware chain, approval workflow nhiều cấp |
| Cần thông báo nhiều bên khi 1 sự kiện xảy ra, các bên không nên biết nhau | **Observer / Domain Event** | Spring `ApplicationEventPublisher`, Go channel/pubsub nội bộ, Kafka cho cross-service |
| Truy cập dữ liệu cần trừu tượng hoá khỏi chi tiết lưu trữ | **Repository** | Xem chi tiết trong skill `clean-architecture-webapp` — đây là Port ở domain layer |
| Nhiều thay đổi trên nhiều aggregate cần commit cùng lúc hoặc rollback cùng lúc | **Unit of Work** | Java: `@Transactional` đã bao hàm phần lớn; Go/Rust: transaction object truyền qua use case |
| Đọc dữ liệu cần model/tối ưu khác hẳn ghi dữ liệu (nhiều join phức tạp cho báo cáo, nhưng ghi thì đơn giản) | **CQRS** | Chỉ tách khi thật sự có lệch pha đọc/ghi rõ rệt — xem thêm cảnh báo over-engineering ở dưới |
| Nghiệp vụ trải dài nhiều service, cần bù trừ (compensate) khi 1 bước giữa chừng lỗi | **Saga** | Choreography (qua event) cho ít bước; Orchestration (1 coordinator) khi cần kiểm soát trình tự chặt |
| Gọi external service/API hay lỗi hoặc chậm, cần tránh lỗi lan (cascading failure) | **Circuit Breaker** | Resilience4j (Java), `sony/gobreaker` (Go), `tokio` timeout + retry crate (Rust) |
| API/thư viện bên thứ 3 có interface không khớp với domain hiện tại | **Adapter** | Đây cũng chính là "Adapter" trong Clean Architecture — 2 khái niệm trùng tên, cùng bản chất |
| Muốn expose 1 API đơn giản cho hệ thống con phức tạp (nhiều subsystem) | **Facade** | Cẩn thận: Facade dễ biến thành God Object nếu ôm quá nhiều trách nhiệm |

## Quy trình áp dụng khi được hỏi "nên dùng pattern nào?"

1. Hỏi ngược lại (hoặc tự suy luận từ code): triệu chứng cụ thể là gì — tra bảng trên.
2. Kiểm tra Rule of Three: đã có ≥2-3 biến thể thực tế chưa, hay mới có 1 và "dự đoán" sẽ có thêm?
3. Nếu đủ điều kiện, đọc `references/<ngôn ngữ tương ứng>.md` để lấy code mẫu idiomatic.
4. Cảnh báo rõ trade-off (thêm 1 interface + 1-2 file, đổi lại là điểm mở rộng không sửa code cũ) để user tự quyết, không áp đặt.

## CQRS/Saga — cảnh báo over-engineering riêng

Đây là 2 pattern hay bị áp dụng thừa. Trước khi đề xuất:
- **CQRS**: chỉ đáng làm khi mô hình đọc và ghi thực sự khác nhau đáng kể (VD: ghi transaction OLTP nhưng đọc cần denormalized view cho dashboard/báo cáo). Nếu chỉ là 1 service CRUD bình thường, KHÔNG đề xuất CQRS.
- **Saga**: chỉ cần khi giao dịch trải >1 service/database thật sự cần rollback logic (compensating action). Nếu mọi thứ nằm trong 1 database, dùng transaction thường (`@Transactional`/DB transaction) — không cần Saga.

## Tài liệu tham khảo theo ngôn ngữ

Mỗi file tổ chức theo đúng 3 nhóm GoF (Creational/Structural/Behavioral) + 1 mục riêng cho enterprise pattern (Repository/UoW/CQRS/Saga/Circuit Breaker) — có mục lục ở đầu file để nhảy thẳng tới pattern cần tra:
- `references/java-spring.md` — DI của Spring như Factory/Strategy có sẵn, AOP làm Decorator, Spring Event làm Observer, Resilience4j cho Circuit Breaker, ví dụ bám theo context PDMS
- `references/go.md` — functional options thay Builder, higher-order function làm Strategy/Decorator/Command, `sony/gobreaker`, cách Go tránh hầu hết Creational pattern nhờ không có constructor phức tạp
- `references/rust.md` — trait object vs enum cho Strategy/State (exhaustive match), type-state builder, `tower` middleware cho Decorator, ví dụ bám theo context BPMP (Composite cho BPMN node, Visitor cho WIR, Memento cho snapshot)


---


# 📄 design-patterns-apply/references/go.md

# Design Pattern với Go

Go không có inheritance/class hierarchy — phần lớn Creational pattern của GoF (viết cho C++/Smalltalk) đơn giản hoá rất nhiều hoặc biến mất hẳn ở Go. Tập trung công sức thật vào Structural/Behavioral, nơi Go có idiom riêng rõ rệt (interface nhỏ, composition, channel).

## Mục lục
**Creational**: [Factory Function](#factory-function) · [Abstract Factory](#abstract-factory) · [Functional Options (Builder)](#functional-options-builder) · [Prototype](#prototype) · [Singleton](#singleton)
**Structural**: [Adapter](#adapter) · [Bridge](#bridge) · [Composite](#composite) · [Decorator (Middleware)](#decorator-middleware) · [Facade](#facade) · [Flyweight](#flyweight) · [Proxy](#proxy)
**Behavioral**: [Chain of Responsibility](#chain-of-responsibility) · [Command](#command) · [Interpreter](#interpreter) · [Iterator](#iterator) · [Mediator](#mediator) · [Memento](#memento) · [Observer](#observer) · [State](#state) · [Strategy](#strategy) · [Template Method](#template-method) · [Visitor](#visitor)
**Enterprise (ngoài GoF)**: [Unit of Work](#unit-of-work) · [CQRS](#cqrs) · [Saga](#saga) · [Circuit Breaker](#circuit-breaker)

---

## Creational

### Factory Function
```go
// Go không cần "Factory class" — 1 function trả về interface là đủ
func NewDocumentParser(t DocumentType) (DocumentParser, error) {
    switch t {
    case DocumentTypeExcel: return &ExcelParser{}, nil
    case DocumentTypePDF:   return &PDFParser{}, nil
    default: return nil, fmt.Errorf("unsupported type: %v", t)
    }
}
```

### Abstract Factory
```go
// Họ nhiều factory liên quan — chọn cả bộ theo môi trường qua interface
type StorageFactory interface {
    DocumentRepository() DocumentRepository
    BlobStore() BlobStore
}

type S3StorageFactory struct{ /* ... */ }
func (f *S3StorageFactory) DocumentRepository() DocumentRepository { return &S3Repository{} }
func (f *S3StorageFactory) BlobStore() BlobStore                   { return &S3BlobStore{} }

// Wiring ở main.go chọn factory theo config — không cần Spring @Profile, chọn tường minh
var factory StorageFactory
if cfg.Env == "prod" { factory = &S3StorageFactory{} } else { factory = &LocalStorageFactory{} }
```

### Functional Options (Builder)
```go
// Go idiom thay cho Builder pattern truyền thống — xem chi tiết ở skill clean-architecture-webapp
type SearchCriteria struct {
    docType   DocumentType
    fromDate, toDate time.Time
    status    DocumentStatus
}
type SearchOption func(*SearchCriteria)

func WithType(t DocumentType) SearchOption { return func(c *SearchCriteria) { c.docType = t } }
func WithDateRange(from, to time.Time) SearchOption {
    return func(c *SearchCriteria) { c.fromDate, c.toDate = from, to }
}

func NewSearchCriteria(opts ...SearchOption) SearchCriteria {
    c := SearchCriteria{} // default values
    for _, opt := range opts { opt(&c) }
    return c
}
```

### Prototype
```go
// Copy method tường minh — Go không có clone() built-in nên luôn tường minh, tránh nhầm shallow/deep copy
func (w *WorkflowSnapshot) Clone() *WorkflowSnapshot {
    tasks := make([]TaskState, len(w.Tasks))
    copy(tasks, w.Tasks) // shallow copy đủ nếu TaskState không chứa pointer/slice cần deep copy
    return &WorkflowSnapshot{Tasks: tasks}
}
```
Hiếm dùng trong CRUD service; relevant cho snapshot Aggregate — liên hệ [Memento](#memento).

### Singleton
```go
// Go idiom: package-level var + sync.Once, KHÔNG cần class Singleton
var (
    instance *ConfigLoader
    once     sync.Once
)
func GetConfigLoader() *ConfigLoader {
    once.Do(func() { instance = &ConfigLoader{} })
    return instance
}
```
Cân nhắc kỹ trước khi dùng: phần lớn trường hợp truyền dependency tường minh qua constructor (composition root ở `main.go`) rõ ràng và dễ test hơn package-level singleton.

## Structural

### Adapter
```go
type AccountLookupPort interface { Lookup(ctx context.Context, accountID string) (AccountInfo, error) }

type CoreBankingAdapter struct{ sdk *corebanking.Client } // Adapter: SDK -> domain Port
func (a *CoreBankingAdapter) Lookup(ctx context.Context, accountID string) (AccountInfo, error) {
    raw, err := a.sdk.GetAccountDetail(ctx, accountID)
    if err != nil { return AccountInfo{}, err }
    return AccountInfo{ID: raw.AcctNo, Name: raw.AcctName}, nil // map SDK type -> domain type
}
```

### Bridge
```go
type NotificationSender interface { Send(ctx context.Context, to, message string) error }

type EmailSender struct{ /* ... */ }
type SmsSender struct{ /* ... */ }

// Domain chỉ phụ thuộc NotificationSender interface — thêm kênh mới không ảnh hưởng phần đã dùng kênh cũ
type NotificationDispatcher struct {
    sender NotificationSender // "bridge" sang implementation cụ thể, inject qua constructor
}
```
Khác Strategy: Bridge có 2 trục biến đổi độc lập; ở Go 2 pattern này thường trông giống nhau (đều là interface + implementation), khác biệt chủ yếu ở Ý ĐỊNH thiết kế, không phải cú pháp.

### Composite
```go
type WorkflowNode interface { Execute(ctx context.Context) error }

type TaskNode struct{ /* ... */ } // lá
func (n *TaskNode) Execute(ctx context.Context) error { return nil /* thực thi task */ }

type SubProcessNode struct{ Children []WorkflowNode } // nhánh
func (n *SubProcessNode) Execute(ctx context.Context) error {
    for _, child := range n.Children {
        if err := child.Execute(ctx); err != nil { return err } // xử lý đồng nhất lá và nhánh
    }
    return nil
}
```
Relevant trực tiếp cho BPMP: cấu trúc BPMN node (task/gateway/sub-process) là cây/graph điển hình.

### Decorator (Middleware)
```go
type Handler func(ctx context.Context, req Request) (Response, error)
type Middleware func(Handler) Handler

func WithLogging(next Handler) Handler {
    return func(ctx context.Context, req Request) (Response, error) {
        start := time.Now()
        resp, err := next(ctx, req)
        log.Printf("took %v, err=%v", time.Since(start), err)
        return resp, err
    }
}
handler := WithLogging(WithAuth(coreHandler)) // compose nhiều middleware — mỗi cái "bọc" cái trước
```

### Facade
```go
type DocumentOnboardingFacade struct { // mỏng, chỉ điều phối
    validation *ValidationDispatcher
    archiveUC  *ArchiveUseCase
}
func (f *DocumentOnboardingFacade) Onboard(ctx context.Context, doc Document) error {
    if err := f.validation.Validate(ctx, doc); err != nil { return err }
    return f.archiveUC.Handle(ctx, doc.ID)
}
```

### Flyweight
```go
var typeMetaCache sync.Map // Flyweight cache — chia sẻ object bất biến dùng chung

func GetDocumentTypeMeta(code string) *DocumentTypeMeta {
    if v, ok := typeMetaCache.Load(code); ok { return v.(*DocumentTypeMeta) }
    meta := loadFromConfig(code)
    typeMetaCache.Store(code, meta)
    return meta
}
```
Liên hệ trực tiếp skill `memory-optimization` — giảm allocation cho lookup/reference data dùng lặp lại nhiều nơi.

### Proxy
```go
// Access control proxy — kiểm tra quyền trước khi forward tới object thật
type AuthorizedDocumentService struct {
    delegate DocumentService
    authz    AuthorizationChecker
}
func (s *AuthorizedDocumentService) Get(ctx context.Context, id string) (Document, error) {
    if err := s.authz.CheckReadPermission(ctx, id); err != nil { return Document{}, err }
    return s.delegate.Get(ctx, id)
}
```
Go không có lazy-loading proxy tự động như JPA — nếu cần lazy load, viết tường minh qua `sync.Once` hoặc field kiểu function.

## Behavioral

### Chain of Responsibility
```go
type Step func(ctx context.Context, doc *Document) error

func RunPipeline(ctx context.Context, doc *Document, steps ...Step) error {
    for _, step := range steps {
        if err := step(ctx, doc); err != nil { return err } // dừng chain, trả lỗi ngay
    }
    return nil
}
```

### Command
```go
type ArchiveDocumentCommand struct {
    DocumentID string
    Actor      string
}
type ArchiveDocumentHandler struct{ /* ... */ }
func (h *ArchiveDocumentHandler) Handle(ctx context.Context, cmd ArchiveDocumentCommand) error { return nil }
```
Nền tảng của [CQRS](#cqrs) command side — command là struct thuần, dễ serialize để queue/audit log.

### Interpreter
```go
type RuleExpression interface { Evaluate(ctx EvaluationContext) bool }
type AndExpression struct{ Left, Right RuleExpression }
func (e AndExpression) Evaluate(ctx EvaluationContext) bool { return e.Left.Evaluate(ctx) && e.Right.Evaluate(ctx) }
```
Hiếm cần trong webapp thường; cân nhắc thư viện rule engine hoặc closure composition thay vì tự viết đầy đủ.

### Iterator
Go 1.23+ có `iter.Seq`/`range-over-func` built-in — hiếm khi tự viết Iterator kiểu GoF cổ điển:
```go
func (t *WorkflowTree) All() iter.Seq[WorkflowNode] {
    return func(yield func(WorkflowNode) bool) {
        for _, n := range t.depthFirst() { if !yield(n) { return } }
    }
}
// for node := range tree.All() { ... }
```

### Mediator
```go
type DocumentWorkflowMediator struct { // biết tất cả participant
    validation *ValidationDispatcher
    archive    *ArchiveUseCase
}
func (m *DocumentWorkflowMediator) HandleUploadCompleted(ctx context.Context, doc Document) error {
    if err := m.validation.Validate(ctx, doc); err != nil { return err }
    return nil // điều phối bước tiếp theo
}
```

### Memento
```go
type WorkflowMemento struct { // snapshot bất biến
    State     WorkflowState
    Variables map[string]any
}
func (w *WorkflowInstance) SaveState() WorkflowMemento {
    return WorkflowMemento{State: w.state, Variables: maps.Clone(w.variables)} // copy map, tránh alias
}
func (w *WorkflowInstance) Restore(m WorkflowMemento) { w.state, w.variables = m.State, m.Variables }
```
Nền tảng checkpoint/resume (skill `stream-batch-processing`) — trong BPMP, Go gateway/projection service có thể dùng Memento để lưu điểm khôi phục khi replay event từ Kafka.

### Observer
```go
type EventBus struct {
    mu        sync.RWMutex
    listeners map[string][]func(Event)
}
func (b *EventBus) Publish(e Event) {
    b.mu.RLock(); defer b.mu.RUnlock()
    for _, fn := range b.listeners[e.Type()] { go fn(e) }
}
```
Cross-service: dùng Kafka/Redpanda thay vì `EventBus` nội bộ (mất event khi process restart).

### State
```go
// Xem chi tiết generic StateMachine[S] đầy đủ ở skill common-lib-base — đây là dùng lại nó:
if !documentStateMachine.CanTransition(doc.Status, StatusArchived) {
    return ErrInvalidTransition
}
doc.Status = StatusArchived
```

### Strategy
```go
type ValidateFunc func(doc Document) error // function type, idiomatic hơn interface cho hành vi đơn giản

var validators = map[DocumentType]ValidateFunc{
    DocumentTypeExcel: validateExcel,
    DocumentTypePDF:   validatePDF,
}
```
Thêm loại mới = thêm 1 entry vào map, không sửa dispatcher.

### Template Method
```go
// Go không có inheritance — mô phỏng bằng struct chứa function field (composition thay vì override)
type EtlJob struct {
    Extract   func() []RawRecord
    Transform func([]RawRecord) []ValidRecord
    Load      func([]ValidRecord) error
}
func (j EtlJob) Run() error { return j.Load(j.Transform(j.Extract())) } // khung cố định, bước truyền vào
```
Ít dùng ở Go so với Java vì không có "override" thật — function field/Strategy thường rõ ràng hơn.

### Visitor
```go
type NodeVisitor interface {
    VisitTask(n *TaskNode)
    VisitSubProcess(n *SubProcessNode)
}
type WorkflowNode interface { Accept(v NodeVisitor) }
func (n *TaskNode) Accept(v NodeVisitor) { v.VisitTask(n) }

type JsonExportVisitor struct{ /* ... */ } // thêm thao tác mới, không sửa TaskNode/SubProcessNode
func (v *JsonExportVisitor) VisitTask(n *TaskNode) { /* ... */ }
```
Cặp đôi tự nhiên với [Composite](#composite) — relevant cho duyệt/biến đổi cấu trúc BPMN trong BPMP.

## Enterprise patterns (ngoài GoF)

### Unit of Work
```go
// Transaction object truyền qua use case — Go không có @Transactional, phải tường minh
func (uc *ArchiveUseCase) Handle(ctx context.Context, id string) error {
    return uc.db.WithTx(ctx, func(tx *sql.Tx) error { // WithTx = Unit of Work boundary
        repo := uc.repoFactory.WithTx(tx)
        doc, err := repo.FindByID(ctx, id)
        if err != nil { return err }
        if err := doc.Archive(); err != nil { return err }
        return repo.Save(ctx, doc)
    })
}
```

### CQRS
```go
// Query side — bỏ qua domain model, query thẳng ra DTO tối ưu cho read, tách khỏi Repository Port của domain
func (q *DocumentQueryService) SummariesByStatus(ctx context.Context, status string) ([]DocumentSummary, error) {
    rows, err := q.db.QueryContext(ctx, `SELECT id, status, owner_name FROM documents WHERE status=$1`, status)
    // ...
}
```

### Saga
```go
func (s *DocumentMigrationSaga) Execute(ctx context.Context, cmd MigrationCommand) error {
    if err := s.staging.Load(ctx, cmd); err != nil { return err }
    if err := s.validation.Validate(ctx, cmd); err != nil { return err }
    if err := s.production.Commit(ctx, cmd); err != nil {
        s.production.Compensate(ctx, cmd) // rollback bước đã thành công trước đó
        return err
    }
    return nil
}
```

### Circuit Breaker
```go
cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
    Name: "core-banking-api", MaxRequests: 5, Timeout: 10 * time.Second,
    ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures > 5 },
})
result, err := cb.Execute(func() (interface{}, error) { return coreBankingClient.Fetch(ctx, accountID) })
```

## Cảnh báo Go-specific

- Đừng tạo interface chỉ có 1 implementation "phòng khi cần mock" — tạo interface ở nơi CẦN nó (consumer), không tạo trước ở nơi định nghĩa struct.
- Đa số Creational pattern GoF (Factory, Abstract Factory, Builder) ở Go chỉ là function/functional options — đừng cố dựng class hierarchy mô phỏng Java.


---


# 📄 design-patterns-apply/references/java-spring.md

# Design Pattern với Java 21 / Spring Boot

Spring DI container đã "miễn phí" cung cấp hành vi của nhiều Creational pattern (Singleton, Factory) — phần lớn công sức thực tế nằm ở Structural và Behavioral. Đọc mục lục, nhảy thẳng tới pattern cần.

## Mục lục
**Creational**: [Factory Method](#factory-method) · [Abstract Factory](#abstract-factory) · [Builder](#builder) · [Prototype](#prototype) · [Singleton](#singleton)
**Structural**: [Adapter](#adapter) · [Bridge](#bridge) · [Composite](#composite) · [Decorator](#decorator) · [Facade](#facade) · [Flyweight](#flyweight) · [Proxy](#proxy)
**Behavioral**: [Chain of Responsibility](#chain-of-responsibility) · [Command](#command) · [Interpreter](#interpreter) · [Iterator](#iterator) · [Mediator](#mediator) · [Memento](#memento) · [Observer](#observer) · [State](#state) · [Strategy](#strategy) · [Template Method](#template-method) · [Visitor](#visitor)
**Enterprise (ngoài GoF)**: [Unit of Work](#unit-of-work) · [CQRS](#cqrs) · [Saga](#saga) · [Circuit Breaker](#circuit-breaker)

---

## Creational

### Factory Method
```java
// Interface + factory chọn implementation cụ thể theo input runtime
public interface DocumentParser { ParsedDocument parse(InputStream in); }

@Component
class DocumentParserFactory {
    private final Map<DocumentType, DocumentParser> parsers;
    public DocumentParserFactory(List<DocumentParser> beans) {
        this.parsers = beans.stream().collect(toMap(DocumentParser::supportedType, identity()));
    }
    public DocumentParser create(DocumentType type) {
        return Optional.ofNullable(parsers.get(type))
            .orElseThrow(() -> new UnsupportedDocumentTypeException(type));
    }
}
```
Khác biệt ý định với Strategy: Factory Method tập trung việc TẠO đúng object, Strategy tập trung CHỌN đúng hành vi — trong Spring cả 2 thường dùng chung cơ chế `Map<Key, Bean>`.

### Abstract Factory
```java
// Họ nhiều factory liên quan — chọn cả bộ dependency theo môi trường
public interface StorageFactory {
    DocumentRepository documentRepository();
    BlobStore blobStore();
}

@Component @Profile("prod")
class S3StorageFactory implements StorageFactory {
    public DocumentRepository documentRepository() { return new S3DocumentRepository(); }
    public BlobStore blobStore() { return new S3BlobStore(); }
}

@Component @Profile("dev")
class LocalStorageFactory implements StorageFactory {
    public DocumentRepository documentRepository() { return new LocalDocumentRepository(); }
    public BlobStore blobStore() { return new LocalBlobStore(); }
}
```
Spring `@Profile` đã đóng vai trò "chọn cả họ factory" theo môi trường — hiếm khi cần tự viết Abstract Factory tường minh ngoài multi-tenant phức tạp.

### Builder
```java
public class SearchCriteria {
    private final DocumentType type;
    private final LocalDate fromDate, toDate;
    private final DocumentStatus status;

    private SearchCriteria(Builder b) {
        this.type = b.type; this.fromDate = b.fromDate; this.toDate = b.toDate; this.status = b.status;
    }

    public static class Builder {
        private DocumentType type;
        private LocalDate fromDate, toDate;
        private DocumentStatus status;
        public Builder type(DocumentType t) { this.type = t; return this; }
        public Builder dateRange(LocalDate from, LocalDate to) { this.fromDate = from; this.toDate = to; return this; }
        public Builder status(DocumentStatus s) { this.status = s; return this; }
        public SearchCriteria build() { return new SearchCriteria(this); }
    }
}
```
Dùng Lombok `@Builder` để giảm boilerplate — chỉ đáng dùng Builder (tự viết hoặc Lombok) khi ≥4-5 field optional; ít hơn thì constructor thường đơn giản hơn.

### Prototype
```java
// Copy constructor tường minh — KHÔNG dùng Object.clone() của Java (nhiều cạm bẫy shallow copy)
public class WorkflowSnapshot {
    private final List<TaskState> tasks;
    public WorkflowSnapshot(WorkflowSnapshot source) {
        this.tasks = source.tasks.stream().map(TaskState::copy).toList(); // deep copy từng phần tử
    }
}
```
Hiếm dùng trong CRUD service thông thường; relevant khi cần giữ snapshot bất biến của Aggregate tại 1 thời điểm — liên hệ [Memento](#memento).

### Singleton
```java
// Spring bean mặc định (không khai báo @Scope) ĐÃ LÀ Singleton — không cần tự viết
@Service
class DocumentValidationService { ... }

// ❌ Tránh: static instance thủ công — mất khả năng test/mock, che giấu global state
class BadSingleton {
    private static final BadSingleton INSTANCE = new BadSingleton();
    public static BadSingleton getInstance() { return INSTANCE; }
}
```

## Structural

### Adapter
```java
public interface AccountLookupPort { AccountInfo lookup(String accountId); } // Port của domain

@Component
class CoreBankingAdapter implements AccountLookupPort { // Adapter: SDK interface -> domain Port
    private final CoreBankingSdkClient sdk;
    public AccountInfo lookup(String accountId) {
        var raw = sdk.getAccountDetail(accountId);
        return new AccountInfo(raw.getAcctNo(), raw.getAcctName());
    }
}
```
Cùng bản chất với "Adapter" trong skill `clean-architecture-webapp` — 2 khái niệm trùng tên có chủ đích.

### Bridge
```java
public interface NotificationSender { void send(String to, String message); }
class EmailSender implements NotificationSender { ... }
class SmsSender implements NotificationSender { ... }

public abstract class NotificationDispatcher { // abstraction, độc lập với implementation
    protected final NotificationSender sender; // "bridge" sang implementation cụ thể
    protected NotificationDispatcher(NotificationSender sender) { this.sender = sender; }
    public abstract void notify(DomainEvent event);
}
```
Khác Strategy: Bridge có 2 trục biến đổi độc lập (abstraction VÀ implementation đều mở rộng riêng được); Strategy chỉ 1 trục (chọn 1 trong nhiều thuật toán).

### Composite
```java
public interface WorkflowNode { void execute(ExecutionContext ctx); }

class TaskNode implements WorkflowNode { // lá
    public void execute(ExecutionContext ctx) { /* thực thi 1 task cụ thể */ }
}
class SubProcessNode implements WorkflowNode { // nhánh — chứa node con
    private final List<WorkflowNode> children;
    public void execute(ExecutionContext ctx) {
        for (WorkflowNode child : children) child.execute(ctx); // xử lý đồng nhất lá và nhánh
    }
}
```
Dùng khi mô hình hoá cấu trúc phân cấp/workflow (giống BPMN) — caller gọi `execute()` không cần biết đang xử lý Task hay SubProcess.

### Decorator
Thay vì tự viết class Decorator bọc từng method, Spring AOP "decorate" xuyên suốt bằng annotation + `@Aspect`:
```java
@Aspect @Component
class AuditLogAspect {
    @Around("@annotation(Audited)")
    public Object logAround(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.nanoTime();
        try { return pjp.proceed(); }
        finally { log.info("{} took {} ms", pjp.getSignature(), (System.nanoTime() - start) / 1_000_000); }
    }
}
```
Bẫy self-invocation (xem skill `clean-architecture-webapp`): AOP decorator chỉ hoạt động khi gọi qua bean proxy từ class khác, không hoạt động khi gọi nội bộ cùng class.

### Facade
```java
@Service
public class DocumentOnboardingFacade { // mỏng, chỉ điều phối — KHÔNG chứa business logic
    private final ValidationDispatcher validation;
    private final ArchiveDocumentUseCase archiveUseCase;
    public void onboard(Document doc) {
        validation.validate(doc);
        archiveUseCase.handle(new ArchiveDocumentCommand(doc.id()));
    }
}
```
Cẩn thận: Facade dễ biến thành God Object nếu ôm quá nhiều trách nhiệm thay vì chỉ điều phối.

### Flyweight
```java
public class DocumentTypeRegistry { // Flyweight factory — chia sẻ object bất biến dùng chung
    private static final Map<String, DocumentTypeMeta> CACHE = new ConcurrentHashMap<>();
    public static DocumentTypeMeta get(String code) {
        return CACHE.computeIfAbsent(code, DocumentTypeRegistry::loadFromConfig); // tạo 1 lần, dùng chung
    }
}
```
Liên hệ trực tiếp skill `memory-optimization`: giảm allocation khi nhiều chỗ cần cùng 1 object bất biến (lookup/reference data).

### Proxy
```java
// JPA lazy loading CHÍNH LÀ Proxy pattern có sẵn — Hibernate tự sinh proxy class
@Entity
class Document {
    @ManyToOne(fetch = FetchType.LAZY) // trả về proxy, chỉ load thật khi truy cập field
    private Owner owner;
}

// Proxy tự viết cho access control
@Component
class AuthorizedDocumentService implements DocumentService {
    private final DocumentService delegate;
    private final AuthorizationChecker authz;
    public Document get(String id) {
        authz.checkReadPermission(id); // kiểm soát truy cập TRƯỚC khi forward tới object thật
        return delegate.get(id);
    }
}
```

## Behavioral

### Chain of Responsibility
```java
public interface ValidationStep { void validate(Document doc, ValidationContext ctx) throws ValidationException; }

@Component @Order(1) class FormatCheckStep implements ValidationStep { ... }
@Component @Order(2) class SizeCheckStep implements ValidationStep { ... }

@Service
class ValidationChain {
    private final List<ValidationStep> steps; // Spring inject theo thứ tự @Order
    public void run(Document doc) throws ValidationException {
        var ctx = new ValidationContext();
        for (var step : steps) step.validate(doc, ctx); // dừng ngay khi 1 step throw
    }
}
```

### Command
```java
public record ArchiveDocumentCommand(String documentId, String actor) {} // yêu cầu đóng gói thành object

@Component
class ArchiveDocumentCommandHandler {
    void handle(ArchiveDocumentCommand cmd) { /* ... */ }
}
```
Nền tảng của [CQRS](#cqrs) command side — mọi command là object, dễ audit log/queue/replay.

### Interpreter
```java
public interface RuleExpression { boolean evaluate(EvaluationContext ctx); }
class AndExpression implements RuleExpression {
    private final RuleExpression left, right;
    public boolean evaluate(EvaluationContext ctx) { return left.evaluate(ctx) && right.evaluate(ctx); }
}
```
Hầu hết webapp/backend nên dùng thư viện rule engine có sẵn (Drools) hoặc function composition thay vì tự viết Interpreter đầy đủ — chỉ cần khi ngôn ngữ biểu thức phải thay đổi được lúc runtime mà không deploy lại.

### Iterator
Java Collection Framework đã cung cấp `Iterator` — hiếm khi tự implement. Chỉ tự viết khi có cấu trúc dữ liệu tuỳ biến cần duyệt theo thứ tự riêng:
```java
public class DepthFirstNodeIterator implements Iterator<WorkflowNode> { ... } // duyệt cây WorkflowNode
```

### Mediator
```java
@Component
class DocumentWorkflowMediator { // biết TẤT CẢ participant, participant không biết nhau
    private final ValidationDispatcher validation;
    private final ArchiveDocumentUseCase archive;
    void handleUploadCompleted(Document doc) {
        validation.validate(doc);
        // điều phối thứ tự các bước tiếp theo
    }
}
```
Khác Facade: Facade đơn giản hoá API cho client BÊN NGOÀI; Mediator điều phối giao tiếp GIỮA các participant nội bộ.

### Memento
```java
public class WorkflowInstance {
    public WorkflowMemento saveState() { return new WorkflowMemento(this.state, this.variables); }
    public void restore(WorkflowMemento m) { this.state = m.state(); this.variables = m.variables(); }
}
public record WorkflowMemento(WorkflowState state, Map<String, Object> variables) {} // snapshot bất biến
```
Nền tảng cho checkpoint/resume (skill `stream-batch-processing`) và hướng Shadow Verification/Simulation của BPMP — chạy song song 2 instance từ cùng 1 memento để so sánh kết quả.

### Observer
```java
public record DocumentArchived(String documentId) {}
eventPublisher.publishEvent(new DocumentArchived(doc.id())); // publisher không biết ai đang lắng nghe

@Component
class NotifyOwnerOnArchive {
    @EventListener @Async // cân nhắc @TransactionalEventListener(phase=AFTER_COMMIT) nếu cần đợi commit
    void on(DocumentArchived event) { /* gửi notification */ }
}
```
Sự kiện cross-service (không chỉ trong JVM): publish lên Kafka thay vì `ApplicationEventPublisher` — xem PDMS: Kafka-based multi-consumer sync completion guarantee.

### State
Xem chi tiết đầy đủ (generic `StateMachine`) ở skill `common-lib-base`, mục StatefulEntity — cùng bản chất pattern: state hiện tại quyết định transition nào hợp lệ, hành vi thay đổi theo state.

### Strategy
```java
public interface DocumentValidator {
    boolean supports(DocumentType type);
    ValidationResult validate(Document doc);
}

@Component
class ExcelDocumentValidator implements DocumentValidator {
    public boolean supports(DocumentType type) { return type == DocumentType.EXCEL; }
    public ValidationResult validate(Document doc) { /* SAX streaming validation */ }
}

@Service
class ValidationDispatcher {
    private final List<DocumentValidator> validators; // Spring inject TẤT CẢ bean cùng interface
    public ValidationDispatcher(List<DocumentValidator> validators) { this.validators = validators; }
    public ValidationResult validate(Document doc) {
        return validators.stream().filter(v -> v.supports(doc.type())).findFirst()
            .orElseThrow(() -> new UnsupportedDocumentTypeException(doc.type()))
            .validate(doc);
    }
}
```
Thêm loại document mới = thêm 1 `@Component`, không sửa `ValidationDispatcher` — Open/Closed Principle.

### Template Method
```java
public abstract class EtlJobTemplate {
    public final void run() { // final — khung không đổi được
        var raw = extract();
        var valid = transform(raw);
        load(valid);
    }
    protected abstract List<RawRecord> extract();
    protected abstract List<ValidRecord> transform(List<RawRecord> raw);
    protected abstract void load(List<ValidRecord> valid);
}
```
Cân nhắc Strategy (compose nhiều interface nhỏ) trước khi chọn Template Method — chỉ chọn khi thứ tự bước THỰC SỰ cố định.

### Visitor
```java
public interface NodeVisitor<R> {
    R visitTask(TaskNode node);
    R visitSubProcess(SubProcessNode node);
}
public interface WorkflowNode { <R> R accept(NodeVisitor<R> visitor); }
class TaskNode implements WorkflowNode {
    public <R> R accept(NodeVisitor<R> visitor) { return visitor.visitTask(this); }
}
class JsonExportVisitor implements NodeVisitor<JsonNode> { ... } // thêm thao tác mới, KHÔNG sửa TaskNode
```
Cặp đôi tự nhiên với [Composite](#composite) — tách thao tác (export, validate, optimize) khỏi cấu trúc dữ liệu.

## Enterprise patterns (ngoài GoF)

### Unit of Work
Phần lớn đã có sẵn qua `@Transactional` ở tầng Application use case — 1 use case = 1 transaction boundary = 1 unit of work. Chỉ tự viết tường minh khi cần gộp nhiều thao tác qua >1 datasource không share transaction manager.

### CQRS
```java
// Command side — qua Aggregate, đầy đủ business rule (xem ArchiveDocumentUseCase ở skill clean-architecture-webapp)
// Query side — bỏ qua domain model, query thẳng ra DTO tối ưu cho read
@Repository
interface DocumentQueryRepository extends Repository<DocumentJpaEntity, String> {
    @Query("SELECT new com.vpbank.pdms.document.query.DocumentSummaryDto(d.id, d.status, d.ownerName) " +
           "FROM DocumentJpaEntity d WHERE d.status = :status")
    List<DocumentSummaryDto> findSummariesByStatus(DocumentStatus status);
}
```
Query side không đi qua Aggregate/Port của domain — cố ý, vì đọc không cần bảo vệ invariant. Chỉ tách khi đọc/ghi thật sự lệch pha (xem cảnh báo over-engineering ở SKILL.md).

### Saga
```java
@Component
class DocumentMigrationSaga { // Orchestration — 1 coordinator điều khiển trình tự
    void execute(MigrationCommand cmd) {
        try {
            stagingService.load(cmd);
            validationService.validate(cmd);
            productionService.commit(cmd);
        } catch (Exception e) {
            productionService.compensate(cmd); // rollback bước đã thành công trước đó
            throw e;
        }
    }
}
```
Chỉ dùng khi các bước trải qua nhiều transaction/service riêng biệt không share 1 DB transaction.

### Circuit Breaker
```java
@CircuitBreaker(name = "coreBankingApi", fallbackMethod = "fallback")
@TimeLimiter(name = "coreBankingApi")
public CompletableFuture<AccountInfo> getAccountInfo(String accountId) {
    return CompletableFuture.supplyAsync(() -> coreBankingClient.fetch(accountId));
}
private CompletableFuture<AccountInfo> fallback(String accountId, Throwable t) {
    return CompletableFuture.completedFuture(AccountInfo.unavailable(accountId));
}
```
Kinh nghiệm PDMS: cấu hình sai `TimeLimiter` kết hợp Spring Cloud Gateway gây timeout không như mong đợi — luôn set `timeout-duration` NGẮN HƠN timeout tầng gateway phía trước.

## Cảnh báo Java-specific

- Đừng dùng `abstract class` + Template Method khi Strategy (interface + Spring bean list) đơn giản hơn và test dễ hơn.
- Factory pattern trong Spring thường KHÔNG cần tự viết `XxxFactory` class — dùng `ObjectProvider<T>` hoặc `Map<String, T>`/`List<T>` (Spring inject theo bean) để chọn implementation runtime.
- Đừng tự viết Singleton thủ công — dùng Spring bean scope mặc định.


---


# 📄 design-patterns-apply/references/rust.md

# Design Pattern với Rust

Rust không có inheritance; trait + enum thay thế phần lớn cơ chế GoF cổ điển. Câu hỏi mở đầu cho mọi pattern hành vi: tập biến thể **đóng** (dùng enum + exhaustive match) hay **mở** (dùng trait)?

## Mục lục
**Creational**: [Factory Function](#factory-function) · [Abstract Factory](#abstract-factory) · [Builder (Type-State)](#builder-type-state) · [Prototype](#prototype-clone) · [Singleton](#singleton)
**Structural**: [Adapter](#adapter) · [Bridge](#bridge) · [Composite](#composite) · [Decorator](#decorator) · [Facade](#facade) · [Flyweight](#flyweight) · [Proxy](#proxy)
**Behavioral**: [Chain of Responsibility](#chain-of-responsibility) · [Command](#command) · [Interpreter](#interpreter) · [Iterator](#iterator) · [Mediator](#mediator) · [Memento](#memento) · [Observer](#observer) · [State](#state) · [Strategy](#strategy) · [Template Method](#template-method) · [Visitor](#visitor)
**Enterprise (ngoài GoF)**: [Unit of Work](#unit-of-work) · [CQRS](#cqrs) · [Saga](#saga) · [Circuit Breaker](#circuit-breaker)

---

## Creational

### Factory Function
```rust
// Function trả về Box<dyn Trait> hoặc enum — không cần "Factory struct" riêng
fn create_parser(t: DocumentType) -> Box<dyn DocumentParser> {
    match t {
        DocumentType::Excel => Box::new(ExcelParser::default()),
        DocumentType::Pdf => Box::new(PdfParser::default()),
    }
}
```

### Abstract Factory
```rust
// Trait định nghĩa họ factory liên quan
trait StorageFactory {
    fn document_repository(&self) -> Box<dyn DocumentRepository>;
    fn blob_store(&self) -> Box<dyn BlobStore>;
}
struct S3StorageFactory;
impl StorageFactory for S3StorageFactory {
    fn document_repository(&self) -> Box<dyn DocumentRepository> { Box::new(S3Repository::new()) }
    fn blob_store(&self) -> Box<dyn BlobStore> { Box::new(S3BlobStore::new()) }
}
// Composition root chọn factory theo config, tương tự Go — không có DI container tự động
```

### Builder (Type-State)
```rust
// Xem chi tiết đầy đủ ở skill common-lib-base/clean-architecture-webapp — type-state builder
// bắt lỗi thiếu field bắt buộc NGAY LÚC COMPILE, mạnh hơn Builder thường của GoF:
struct WorkflowBuilder<S> { id: S, name: Option<String> }
// WorkflowBuilder::new().id(wid).build()  →  OK
// WorkflowBuilder::new().build()          →  LỖI COMPILE (thiếu id)
```

### Prototype (`Clone`)
```rust
// derive(Clone) là Prototype pattern có sẵn của Rust — deep copy mặc định (trừ khi field là Rc/Arc)
#[derive(Clone)]
struct WorkflowSnapshot {
    tasks: Vec<TaskState>, // Vec::clone() tự động deep-copy từng phần tử nếu TaskState: Clone
}
let snapshot2 = snapshot1.clone(); // Prototype — không cần viết logic copy thủ công
```
Khác Java/Go: Rust có `Clone` trait chuẩn hoá sẵn, chỉ cần `#[derive(Clone)]` nếu mọi field đều `Clone` — ít cạm bẫy shallow-copy hơn nhiều so với `Object.clone()` của Java.

### Singleton
```rust
// once_cell/std::sync::OnceLock — Rust idiom cho lazy static, thread-safe có sẵn
use std::sync::OnceLock;
static CONFIG: OnceLock<Config> = OnceLock::new();
fn config() -> &'static Config { CONFIG.get_or_init(|| Config::load()) }
```
Cân nhắc kỹ: phần lớn trường hợp nên truyền dependency tường minh qua constructor/composition root (`bin/*/main.rs`) thay vì global static — dễ test hơn, không có ordering issue giữa các static.

## Structural

### Adapter
```rust
trait AccountLookupPort { fn lookup(&self, account_id: &str) -> Result<AccountInfo, Error>; }

struct CoreBankingAdapter { sdk: CoreBankingSdkClient }
impl AccountLookupPort for CoreBankingAdapter {
    fn lookup(&self, account_id: &str) -> Result<AccountInfo, Error> {
        let raw = self.sdk.get_account_detail(account_id)?;
        Ok(AccountInfo { id: raw.acct_no, name: raw.acct_name }) // map SDK type -> domain type
    }
}
```

### Bridge
```rust
trait NotificationSender { fn send(&self, to: &str, message: &str) -> Result<(), Error>; }
struct EmailSender; struct SmsSender;

struct NotificationDispatcher<S: NotificationSender> { sender: S } // generic = bridge tại compile-time
// hoặc Box<dyn NotificationSender> nếu cần chọn implementation tại runtime
```

### Composite
```rust
trait WorkflowNode { fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), Error>; }

struct TaskNode { /* ... */ } // lá
impl WorkflowNode for TaskNode {
    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), Error> { Ok(()) }
}
struct SubProcessNode { children: Vec<Box<dyn WorkflowNode>> } // nhánh
impl WorkflowNode for SubProcessNode {
    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), Error> {
        for child in &self.children { child.execute(ctx)?; } // xử lý đồng nhất lá và nhánh
        Ok(())
    }
}
```
Trực tiếp relevant cho BPMP Engine: WIR (Workflow Intermediate Representation) là cấu trúc cây/graph, Composite là mô hình tự nhiên cho node BPMN.

### Decorator
```rust
// Newtype wrapper — forward + thêm hành vi, implement cùng trait
struct LoggingRepository<R> { inner: R }
impl<R: WorkflowRepository> WorkflowRepository for LoggingRepository<R> {
    fn find_by_id(&self, id: &WorkflowId) -> Result<Option<WorkflowInstance>, DomainError> {
        tracing::info!(?id, "find_by_id called");
        self.inner.find_by_id(id)
    }
}
```
Cho HTTP/gRPC layer, dùng `tower::Layer`/`tower::Service` — Decorator chuẩn hoá sẵn của ecosystem Rust.

### Facade
```rust
struct DocumentOnboardingFacade { // mỏng, chỉ điều phối
    validation: ValidationDispatcher,
    archive_uc: ArchiveUseCase,
}
impl DocumentOnboardingFacade {
    fn onboard(&self, doc: &Document) -> Result<(), Error> {
        self.validation.validate(doc)?;
        self.archive_uc.handle(doc.id())
    }
}
```

### Flyweight
```rust
use std::sync::OnceLock;
use dashmap::DashMap; // hoặc std::sync::RwLock<HashMap<...>>

static TYPE_META_CACHE: OnceLock<DashMap<String, Arc<DocumentTypeMeta>>> = OnceLock::new();

fn get_document_type_meta(code: &str) -> Arc<DocumentTypeMeta> {
    let cache = TYPE_META_CACHE.get_or_init(DashMap::new);
    cache.entry(code.to_string())
        .or_insert_with(|| Arc::new(load_from_config(code)))
        .clone() // Arc::clone rẻ — chỉ tăng refcount, không copy dữ liệu
}
```
`Arc<T>` chính là cơ chế chia sẻ object bất biến tự nhiên của Rust cho Flyweight — liên hệ skill `memory-optimization`.

### Proxy
```rust
struct AuthorizedDocumentService<D: DocumentService> {
    delegate: D,
    authz: AuthorizationChecker,
}
impl<D: DocumentService> DocumentService for AuthorizedDocumentService<D> {
    fn get(&self, id: &str) -> Result<Document, Error> {
        self.authz.check_read_permission(id)?; // kiểm soát truy cập TRƯỚC khi forward
        self.delegate.get(id)
    }
}
```
Rust không có lazy-loading proxy tự động như JPA — nếu cần, dùng `OnceCell`/`Lazy` field tường minh trong struct.

## Behavioral

### Chain of Responsibility
```rust
type Step = fn(&mut Document, &mut ValidationContext) -> Result<(), ValidationError>;

fn run_pipeline(doc: &mut Document, steps: &[Step]) -> Result<(), ValidationError> {
    let mut ctx = ValidationContext::default();
    for step in steps { step(doc, &mut ctx)?; } // dừng ngay khi 1 step lỗi (nhờ ?)
    Ok(())
}
```

### Command
```rust
struct ArchiveDocumentCommand { document_id: String, actor: String }

trait CommandHandler<C> { fn handle(&self, cmd: C) -> Result<(), Error>; }
impl CommandHandler<ArchiveDocumentCommand> for ArchiveDocumentCommandHandler {
    fn handle(&self, cmd: ArchiveDocumentCommand) -> Result<(), Error> { Ok(()) }
}
```
Nền tảng của [CQRS](#cqrs) command side — command là struct thuần (derive `Serialize` để queue qua Kafka dễ dàng).

### Interpreter
```rust
trait RuleExpression { fn evaluate(&self, ctx: &EvaluationContext) -> bool; }
struct And(Box<dyn RuleExpression>, Box<dyn RuleExpression>);
impl RuleExpression for And {
    fn evaluate(&self, ctx: &EvaluationContext) -> bool { self.0.evaluate(ctx) && self.1.evaluate(ctx) }
}
```
Hiếm cần trong webapp thường; **Cao cho BPMP** — đánh giá biểu thức DMN/điều kiện gateway trong workflow chính là Interpreter pattern áp dụng thật, không phải lý thuyết.

### Iterator
`Iterator` là trait built-in cực kỳ trung tâm của Rust — hầu như luôn implement thay vì tránh:
```rust
struct DepthFirstNodes<'a> { stack: Vec<&'a dyn WorkflowNode> }
impl<'a> Iterator for DepthFirstNodes<'a> {
    type Item = &'a dyn WorkflowNode;
    fn next(&mut self) -> Option<Self::Item> { self.stack.pop() /* + push children */ }
}
// for node in workflow.depth_first() { ... } — tận dụng toàn bộ iterator adapter chain có sẵn
```

### Mediator
```rust
struct DocumentWorkflowMediator {
    validation: ValidationDispatcher,
    archive: ArchiveUseCase,
}
impl DocumentWorkflowMediator {
    fn handle_upload_completed(&self, doc: &Document) -> Result<(), Error> {
        self.validation.validate(doc)?;
        Ok(()) // điều phối bước tiếp theo
    }
}
```

### Memento
```rust
#[derive(Clone)] // Clone (Prototype) + struct riêng = Memento hoàn chỉnh
struct WorkflowMemento { state: WorkflowState, variables: HashMap<String, Value> }

impl WorkflowInstance {
    fn save_state(&self) -> WorkflowMemento {
        WorkflowMemento { state: self.state.clone(), variables: self.variables.clone() }
    }
    fn restore(&mut self, m: WorkflowMemento) { self.state = m.state; self.variables = m.variables; }
}
```
Nền tảng trực tiếp cho hướng **Shadow Execution / Verification / Simulation** của BPMP: chạy Engine trên 1 adapter khác (in-memory) từ cùng 1 Memento để verify song song với Camunda 7 mà không đổi code domain.

### Observer
```rust
let (tx, _rx) = tokio::sync::broadcast::channel::<DomainEvent>(1024);
tx.send(DomainEvent::WorkflowCompleted { id })?; // publisher không biết ai đang lắng nghe

let mut rx1 = tx.subscribe();
tokio::spawn(async move { while let Ok(event) = rx1.recv().await { /* xử lý */ } });
```
Cho event cần persist/replay, ghi vào Kafka/Redpanda thay vì chỉ broadcast in-memory (mất event khi restart).

### State
```rust
// Xem chi tiết đầy đủ (StateMachine trait + associated type) ở skill common-lib-base
// enum + match cho state machine là State pattern kiểu Rust — compiler enforce exhaustive:
match (self.status, to) {
    (DocumentStatus::Active, DocumentStatus::Archived) => { self.status = to; Ok(()) }
    (from, to) => Err(DomainError::InvalidTransition { from, to }),
}
```

### Strategy
```rust
// Tập đóng — enum + match (compiler bắt lỗi thiếu case)
enum ValidationStrategy { Excel, Pdf }
impl ValidationStrategy {
    fn validate(&self, doc: &Document) -> Result<(), ValidationError> {
        match self { Self::Excel => validate_excel(doc), Self::Pdf => validate_pdf(doc) }
    }
}
// Tập mở — trait + Box<dyn Trait> khi cần plugin từ ngoài crate
trait Validator { fn validate(&self, doc: &Document) -> Result<(), ValidationError>; }
```

### Template Method
```rust
// Trait với default method = khung cố định; method con override phần cần tuỳ biến
trait EtlJob {
    fn extract(&self) -> Vec<RawRecord>;
    fn transform(&self, raw: Vec<RawRecord>) -> Vec<ValidRecord>;
    fn load(&self, valid: Vec<ValidRecord>) -> Result<(), Error>;
    fn run(&self) -> Result<(), Error> { // default method — khung không override được
        self.load(self.transform(self.extract()))
    }
}
```
Ít dùng hơn Strategy/trait composition thuần trong Rust — chỉ hợp khi thứ tự bước thực sự cố định.

### Visitor
```rust
trait NodeVisitor<R> {
    fn visit_task(&self, n: &TaskNode) -> R;
    fn visit_sub_process(&self, n: &SubProcessNode) -> R;
}
trait WorkflowNode { fn accept<R>(&self, visitor: &dyn NodeVisitor<R>) -> R; }

struct JsonExportVisitor; // thêm thao tác mới, không sửa TaskNode/SubProcessNode
impl NodeVisitor<JsonValue> for JsonExportVisitor { /* ... */ }
```
Trực tiếp relevant cho BPMP: duyệt/biến đổi WIR giống compiler pass (type-check pass, optimize pass, codegen pass đều có thể là Visitor riêng biệt trên cùng cấu trúc AST).

## Enterprise patterns (ngoài GoF)

### Unit of Work
```rust
// Transaction truyền qua use case tường minh — Rust không có transaction annotation
async fn handle(&self, id: &WorkflowId) -> Result<(), Error> {
    let mut tx = self.pool.begin().await?; // Unit of Work boundary
    let mut wf = self.repo.find_by_id(&mut tx, id).await?.ok_or(Error::NotFound)?;
    wf.complete()?;
    self.repo.save(&mut tx, &wf).await?;
    tx.commit().await?; // mọi thay đổi trong boundary commit cùng lúc
    Ok(())
}
```

### CQRS
```rust
// Query side — struct/query riêng, tối ưu cho read, tách khỏi trait Repository của domain
async fn summaries_by_status(pool: &PgPool, status: &str) -> Result<Vec<DocumentSummary>, Error> {
    sqlx::query_as!(DocumentSummary, "SELECT id, status, owner_name FROM documents WHERE status = $1", status)
        .fetch_all(pool).await.map_err(Into::into)
}
```

### Saga
```rust
async fn execute(&self, cmd: MigrationCommand) -> Result<(), Error> {
    self.staging.load(&cmd).await?;
    self.validation.validate(&cmd).await?;
    if let Err(e) = self.production.commit(&cmd).await {
        self.production.compensate(&cmd).await?; // rollback bước đã thành công trước đó
        return Err(e);
    }
    Ok(())
}
```

### Circuit Breaker
Ưu tiên crate có sẵn (`tower::retry`, `tower::timeout`, `failsafe-rs`) thay vì tự viết state machine circuit breaker — dễ sai edge case (half-open state, concurrent trip). Xem chi tiết ở skill `common-lib-base`/`stream-batch-processing`.

## Cảnh báo Rust-specific

- Đừng dùng `Box<dyn Trait>` mặc định mọi nơi — ưu tiên generic + trait bound (static dispatch) trước, chỉ chuyển `dyn Trait` khi cần heterogeneous collection hoặc chọn implementation lúc runtime.
- Nhiều pattern GoF (Iterator, Prototype qua `Clone`, Singleton qua `OnceLock`) đã có sẵn trong std — không cần tự implement lại từ đầu như Java.
