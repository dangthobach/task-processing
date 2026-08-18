# Skill: clean-architecture-webapp

> Thiết kế kiến trúc clean/hexagonal/layered cho webapp và backend service — phân tách domain/application/infrastructure/interface, Dependency Rule, ports & adapters, DDD tactical patterns (Entity, Value Object, Aggregate, Repository interface, Domain Event), module boundary design. LUÔN dùng skill này khi bắt đầu một service/module mới, khi refactor code đang rối (business logic lẫn trong controller/handler), khi thiết kế package/crate/module structure, hoặc khi user nhắc tới "clean architecture", "hexagonal", "kiến trúc", "tách layer", "domain model", "bounded context" — kể cả khi không dùng đúng từ đó. Áp dụng cho Java/Spring Boot, Go và Rust.

# Clean Architecture cho Webapp/Backend

## Mục tiêu

Đảm bảo mọi service/module mới (hoặc được refactor) tuân theo **Dependency Rule**: các lớp phụ thuộc chỉ được trỏ vào trong (về phía domain), không bao giờ ngược lại. Domain logic không biết gì về framework, DB, hay giao thức HTTP.

```
┌─────────────────────────────────────────┐
│  Interface (HTTP handler / gRPC / CLI)   │  ──▶ gọi Application
│  ┌─────────────────────────────────────┐ │
│  │ Infrastructure (DB, MQ, external API)│ │  ──▶ implement Ports của Domain
│  │  ┌───────────────────────────────┐  │ │
│  │  │  Application (use case)       │  │ │  ──▶ điều phối Domain, không chứa business rule
│  │  │  ┌─────────────────────────┐  │  │ │
│  │  │  │  Domain (entity, rule,  │  │  │ │  ──▶ KHÔNG import framework/DB/HTTP
│  │  │  │  ports = interfaces)    │  │  │ │
│  │  │  └─────────────────────────┘  │  │ │
│  │  └───────────────────────────────┘  │ │
│  └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

Mọi mũi tên phụ thuộc code (import) chỉ được đi **vào trong**. Infrastructure implement interface do Domain định nghĩa (Dependency Inversion) — đây là điểm hay bị làm sai nhất: Domain định nghĩa `Port` (interface), Infrastructure viết `Adapter` (implementation), không phải ngược lại.

## Quy trình thiết kế (áp dụng khi bắt đầu module/service mới)

1. **Xác định bounded context**: module này sở hữu những khái niệm nghiệp vụ nào, ranh giới với module khác ở đâu (VD trong PDMS: `document-lifecycle` khác `authz` khác `migration-etl`).
2. **Mô hình hoá Domain trước, không nghĩ tới DB/API**:
   - **Entity**: có identity, vòng đời, business rule đi kèm (không phải chỉ getter/setter).
   - **Value Object**: bất biến, so sánh theo giá trị (VD: `DocumentCode`, `Money`, `DateRange`).
   - **Aggregate**: cụm entity/VO có 1 root chịu trách nhiệm giữ invariant; mọi thay đổi đi qua root.
   - **Domain Event**: sự kiện đã xảy ra trong domain (VD: `DocumentArchived`), dùng để tách side-effect ra khỏi transaction chính.
3. **Định nghĩa Port (interface) mà Domain/Application cần**: repository interface, gateway interface (VD: `NotificationPort`, `DocumentRepository`) — định nghĩa ở tầng Domain/Application, KHÔNG ở tầng Infrastructure.
4. **Viết Application use case**: điều phối domain object + port, xử lý transaction boundary, KHÔNG chứa business rule (business rule nằm trong Entity/Aggregate/Domain Service).
5. **Viết Adapter ở Infrastructure**: implement Port bằng JPA/sqlc/sqlx/Kafka client/HTTP client cụ thể.
6. **Viết Interface layer** (REST controller/gRPC handler): chỉ map request ↔ DTO ↔ use case input/output, không chứa logic.
7. **Review lại hướng dependency**: chạy thử "nếu đổi Postgres sang thứ khác, hoặc đổi HTTP sang gRPC, Domain có phải sửa dòng nào không?" — nếu có, dependency đang sai hướng.

## Checklist chống Anemic Domain Model

- [ ] Entity/Aggregate có method thể hiện business rule (`document.archive()`, không phải `document.setStatus(ARCHIVED)` gọi từ Service)
- [ ] Validation bất biến (invariant) nằm trong Aggregate, không nằm rải rác ở Service/Controller
- [ ] Use case (Application layer) ngắn: load aggregate → gọi method domain → save qua Port → publish event nếu cần
- [ ] Domain object không có annotation của framework (không `@Entity` của JPA lẫn trực tiếp vào model nghiệp vụ nếu muốn tách triệt để — xem trade-off trong `references/java-spring.md`)

## Anti-pattern cần tránh

| Anti-pattern | Vấn đề | Cách sửa |
|---|---|---|
| Fat Controller/Handler | Business logic nằm trong HTTP handler | Chuyển vào Application use case |
| God Service | 1 service class làm mọi thứ cho cả module | Tách theo use case, mỗi use case 1 class/function nhỏ |
| Repository trả về DB row trực tiếp cho tầng trên | Domain phụ thuộc ngược vào Infrastructure | Repository (adapter) map row → Domain entity trước khi trả về |
| Leaky abstraction qua exception của driver DB | Domain phải catch `SQLException`/`sql.ErrNoRows` | Adapter wrap thành domain error/sentinel error riêng |
| Transaction script xuyên suốt nhiều aggregate | Khó giữ invariant, khó test | Domain Event + eventual consistency giữa aggregate, hoặc gom vào 1 aggregate |

## Khi nào KHÔNG cần full layering

Với script nhỏ, CLI tool, hoặc prototype throwaway — áp dụng đầy đủ 4 lớp là over-engineering. Ngưỡng hợp lý: nếu module có >1 nguồn dữ liệu, hoặc business rule sẽ được test riêng, hoặc dự kiến sống >6 tháng — nên tách layer. Nếu không, giữ đơn giản, ghi chú lại lý do bỏ qua.

## Tài liệu tham khảo theo ngôn ngữ

Đọc file tương ứng khi cần chi tiết implement cụ thể:
- `references/java-spring.md` — package structure Spring Boot, JPA entity vs domain model, cách tránh JPA leak vào domain, DI qua constructor, ví dụ theo context PDMS
- `references/go.md` — package layout `internal/`, interface ở consumer side, không có inheritance nên compose qua interface nhỏ, ví dụ theo context Go microservices
- `references/rust.md` — workspace crate layout, trait làm Port, ownership ảnh hưởng thế nào tới ranh giới Aggregate, ví dụ theo context BPMP (Rust engine)


---


# 📄 clean-architecture-webapp/references/go.md

# Clean Architecture với Go

## Nguyên tắc khác biệt so với Java

Go không có inheritance và interface là **implicit** (structural typing) — điều này thay đổi cách áp dụng Dependency Inversion:
- Không cần `implements` tường minh; struct tự động thoả interface nếu có đủ method.
- **Interface nên định nghĩa ở phía consumer (Domain/Application), không ở phía provider (Infrastructure)** — ngược lại thói quen từ Java/C#. "Accept interfaces, return structs."
- Không cố bắt chước 4-layer Java 1:1; Go idiomatic hơn với package layout phẳng nhưng ranh giới rõ qua `internal/`.

## Package layout đề xuất

```
bpmp-gateway/                       (Go service trong hệ BPMP, hoặc 1 webapp Go độc lập)
├── cmd/
│   └── server/main.go              (wiring: khởi tạo adapter, inject vào use case, start HTTP)
├── internal/
│   ├── document/                   (bounded context, không export ra ngoài module)
│   │   ├── domain.go                (struct Aggregate + method nghiệp vụ + Port interface)
│   │   ├── usecase.go                (Application: điều phối domain qua Port)
│   │   ├── postgres/repository.go    (Adapter: implement Port bằng pgx/sqlc)
│   │   └── http/handler.go           (Interface layer: map HTTP ↔ usecase)
│   └── platform/                    (common lib nội bộ — xem skill common-lib-base)
└── pkg/                              (chỉ đặt code thật sự dùng được từ ngoài module, thường để trống)
```

`internal/` là cơ chế Go compiler enforce — package ngoài `internal/` không import được, đây là cách Go "khoá" module boundary mà không cần access modifier phức tạp như Java.

## Domain + Port + Adapter

```go
// internal/document/domain.go
package document

type Status string
const (
    StatusActive   Status = "ACTIVE"
    StatusArchived Status = "ARCHIVED"
)

type Document struct {
    ID     string
    Status Status
}

// Archive là business rule — method trên Aggregate, không phải free function trong service
func (d *Document) Archive() error {
    if d.Status != StatusActive {
        return ErrNotActive // sentinel error định nghĩa trong domain.go
    }
    d.Status = StatusArchived
    return nil
}

// Port — định nghĩa TẠI ĐÂY (consumer side), interface nhỏ, chỉ đủ dùng
type Repository interface {
    FindByID(ctx context.Context, id string) (*Document, error)
    Save(ctx context.Context, d *Document) error
}
```

```go
// internal/document/usecase.go
package document

type ArchiveUseCase struct {
    repo   Repository        // phụ thuộc interface (Port), không phụ thuộc *postgres.Repository
    events EventPublisher
}

func NewArchiveUseCase(repo Repository, events EventPublisher) *ArchiveUseCase {
    return &ArchiveUseCase{repo: repo, events: events} // constructor injection thủ công, không DI framework
}

func (uc *ArchiveUseCase) Handle(ctx context.Context, id string) error {
    doc, err := uc.repo.FindByID(ctx, id)
    if err != nil {
        return fmt.Errorf("find document %s: %w", id, err) // wrap, giữ nguyên lỗi gốc để %w unwrap được
    }
    if err := doc.Archive(); err != nil {
        return err
    }
    if err := uc.repo.Save(ctx, doc); err != nil {
        return fmt.Errorf("save document %s: %w", id, err)
    }
    uc.events.Publish(ctx, DocumentArchived{ID: id})
    return nil
}
```

```go
// internal/document/postgres/repository.go — Adapter, implement Port ngầm định (không khai báo implements)
package postgres

type Repository struct{ db *pgxpool.Pool }

func (r *Repository) FindByID(ctx context.Context, id string) (*document.Document, error) {
    var d document.Document
    err := r.db.QueryRow(ctx, `SELECT id, status FROM documents WHERE id=$1`, id).
        Scan(&d.ID, &d.Status)
    if errors.Is(err, pgx.ErrNoRows) {
        return nil, document.ErrNotFound // map lỗi driver → domain error, không để leak pgx.ErrNoRows lên trên
    }
    return &d, err
}
```

Compiler tự kiểm tra `*postgres.Repository` thoả `document.Repository` tại chỗ gọi `document.NewArchiveUseCase(&postgres.Repository{...}, ...)` — không cần annotation.

## Wiring ở `cmd/server/main.go`

Đây là nơi DUY NHẤT được phép "biết" cả domain lẫn infrastructure — main() đóng vai trò composition root:
```go
func main() {
    db := connectPostgres()
    repo := &postgres.Repository{DB: db}
    uc := document.NewArchiveUseCase(repo, kafkaPublisher)
    handler := httpapi.NewDocumentHandler(uc)
    http.ListenAndServe(":8080", handler.Routes())
}
```

## Anti-pattern riêng của Go cần tránh

| Anti-pattern | Vấn đề | Cách sửa |
|---|---|---|
| Interface to lớn kiểu `DocumentService` gồm 10 method | Khó mock, khó tách trách nhiệm | Tách nhiều interface nhỏ 1-3 method (Interface Segregation) |
| Định nghĩa interface ở package `postgres` rồi domain import ngược | Domain phụ thuộc Infrastructure | Chuyển interface về package domain |
| Trả `error` chung chung từ Adapter, không wrap | Use case không phân biệt được lỗi để xử lý (retry, 404, ...) | Dùng sentinel error hoặc custom error type, `errors.Is`/`errors.As` |
| Package `models/` hoặc `types/` dùng chung cho mọi thứ | Không có bounded context, mọi domain trộn lẫn | Tách theo `internal/<context>/domain.go` |

## Liên hệ BPMP

Trong BPMP, Go dùng cho gateway/human-runtime/projection services — các service này nên coi Rust Engine (qua gRPC/Kafka) như một Port/Adapter bên ngoài, KHÔNG để domain model của Go service phụ thuộc trực tiếp vào struct protobuf sinh ra từ Rust WIR; map sang domain type riêng của Go service trước khi dùng.


---


# 📄 clean-architecture-webapp/references/java-spring.md

# Clean Architecture với Java 21 / Spring Boot 3.x

## Package structure đề xuất (theo module, không theo layer toàn cục)

Tránh cấu trúc `controller/`, `service/`, `repository/` phẳng ở root — nó khuyến khích God Service. Thay vào đó tổ chức theo module nghiệp vụ, mỗi module tự có 4 layer bên trong:

```
com.vpbank.pdms
├── document/                      (bounded context)
│   ├── domain/
│   │   ├── Document.java          (Aggregate root — record hoặc class có method nghiệp vụ)
│   │   ├── DocumentStatus.java    (Value Object / enum)
│   │   ├── DocumentRepository.java (Port — interface, KHÔNG có annotation Spring/JPA)
│   │   └── event/DocumentArchived.java
│   ├── application/
│   │   ├── ArchiveDocumentUseCase.java
│   │   └── dto/ArchiveDocumentCommand.java
│   ├── infrastructure/
│   │   ├── persistence/DocumentJpaEntity.java   (annotation @Entity nằm ở ĐÂY, không ở domain)
│   │   ├── persistence/DocumentRepositoryAdapter.java  (implements DocumentRepository)
│   │   └── messaging/DocumentEventPublisherAdapter.java
│   └── interfaces/
│       └── rest/DocumentController.java
└── shared/kernel/                 (common lib — xem skill common-lib-base)
```

## Domain model tách khỏi JPA entity — khi nào đáng làm

Có 2 lựa chọn, chọn theo độ phức tạp nghiệp vụ:

**Option A — JPA entity = Domain model (dùng khi CRUD đơn giản, ít business rule):**
Gắn `@Entity` trực tiếp lên domain class nhưng vẫn giữ method nghiệp vụ trong đó (Rich Domain Model), tránh anemic. Nhanh, ít mapping code, chấp nhận domain "biết" JPA tồn tại.

**Option B — Tách domain model / persistence model (dùng khi PDMS-style: nhiều business rule, nhiều tầng authorization, cần test domain không cần Spring context):**
```java
// domain/Document.java — record/class thuần Java, không annotation
public final class Document {
    private final DocumentId id;
    private DocumentStatus status;

    public void archive() {
        if (status != DocumentStatus.ACTIVE)
            throw new IllegalStateException("Chỉ archive được document đang ACTIVE");
        this.status = DocumentStatus.ARCHIVED;
    }
}

// infrastructure/persistence/DocumentJpaEntity.java — có @Entity, chỉ dùng để map DB
@Entity @Table(name = "documents")
class DocumentJpaEntity { /* fields khớp cột DB */ }

// infrastructure/persistence/DocumentRepositoryAdapter.java
@Repository
class DocumentRepositoryAdapter implements DocumentRepository {
    private final DocumentJpaRepository jpa; // Spring Data JPA interface
    public Optional<Document> findById(DocumentId id) {
        return jpa.findById(id.value()).map(this::toDomain);
    }
    private Document toDomain(DocumentJpaEntity e) { /* mapping thủ công hoặc MapStruct */ }
}
```
Chi phí: phải viết mapping 2 chiều. Đổi lại: domain test được bằng unit test thuần (không cần `@SpringBootTest`), business rule không bị JPA lifecycle (dirty checking, lazy loading) can thiệp bất ngờ.

**Kinh nghiệm PDMS**: với 5-layer authorization platform (Identity/RBAC/Resource/ABAC/Data Filter) — mức độ business rule cao — nên dùng Option B cho domain `document` và `authz`; các module tra cứu đơn giản (lookup/reference data) dùng Option A.

## Port ở domain, Adapter ở infrastructure — Spring DI

```java
// domain/DocumentRepository.java — Port
public interface DocumentRepository {
    Optional<Document> findById(DocumentId id);
    void save(Document document);
}

// application/ArchiveDocumentUseCase.java
@Service
public class ArchiveDocumentUseCase {
    private final DocumentRepository repository; // phụ thuộc vào Port, không phải Adapter
    private final ApplicationEventPublisher events;

    public ArchiveDocumentUseCase(DocumentRepository repository, ApplicationEventPublisher events) {
        this.repository = repository; // constructor injection — bắt buộc, không field injection
        this.events = events;
    }

    @Transactional
    public void handle(ArchiveDocumentCommand cmd) {
        Document doc = repository.findById(cmd.documentId())
            .orElseThrow(() -> new DocumentNotFoundException(cmd.documentId()));
        doc.archive();               // business rule chạy trong domain
        repository.save(doc);
        events.publishEvent(new DocumentArchived(cmd.documentId()));
    }
}
```
Spring tự inject `DocumentRepositoryAdapter` vào chỗ cần `DocumentRepository` nhờ component scan — Application layer không hề import package `infrastructure`.

## AOP và self-invocation — bẫy hay gặp khi tách layer

`@Transactional`, `@Cacheable`, `@Async` chạy qua Spring AOP proxy. Nếu use case gọi method `@Transactional` khác **trong cùng class** (`this.methodKhac()`), proxy bị bỏ qua — transaction không áp dụng. Khi tách use case thành nhiều class nhỏ theo Clean Architecture, vấn đề này giảm tự nhiên vì mỗi use case là 1 bean riêng, gọi nhau qua injection thay vì self-invocation.

## Checklist review PR cho module mới

- [ ] Domain package không có import `org.springframework.*` (trừ khi cố tình chọn Option A)
- [ ] Domain package không có import `jakarta.persistence.*`
- [ ] Interface layer (Controller) không có `if`/business rule, chỉ map DTO
- [ ] Mọi Port có ít nhất 1 Adapter implement, đặt tên rõ `XxxAdapter`
- [ ] Use case method ngắn gọn (load → domain logic → save/publish), không lặp business rule đã có trong Entity


---


# 📄 clean-architecture-webapp/references/rust.md

# Clean Architecture với Rust

## Nguyên tắc khác biệt

Rust không có GC, ownership/borrowing ảnh hưởng trực tiếp tới cách vẽ ranh giới Aggregate: dữ liệu di chuyển (move) qua các layer thay vì được tham chiếu tự do như Java. Trait đóng vai trò Port (giống interface), nhưng cần quyết định sớm **static dispatch (generic + trait bound)** hay **dynamic dispatch (`dyn Trait`)** vì ảnh hưởng tới ký hiệu kiểu xuyên suốt use case.

## Workspace layout đề xuất (phù hợp BPMP — nhiều crate)

```
bpmp-platform/
├── Cargo.toml                     (workspace root)
├── crates/
│   ├── domain/                    (crate thuần logic, KHÔNG phụ thuộc tokio/sqlx/axum)
│   │   ├── src/
│   │   │   ├── workflow.rs        (Aggregate: WorkflowInstance + method nghiệp vụ)
│   │   │   ├── ports.rs           (trait WorkflowRepository, trait EventPublisher)
│   │   │   └── error.rs           (thiserror enum riêng của domain)
│   ├── application/               (use case, phụ thuộc domain, không phụ thuộc infra cụ thể)
│   ├── infra-postgres/            (implement trait từ domain::ports bằng sqlx)
│   ├── infra-raft/                (implement trait bằng OpenRaft + RocksDB — đặc thù BPMP)
│   └── api-grpc/                  (Interface layer: tonic gRPC service, gọi application)
└── bin/
    └── engine/main.rs             (composition root — wiring toàn bộ)
```
Crate `domain` không có dependency tới `tokio`, `sqlx`, `axum`, `tonic` trong `Cargo.toml` — đây là cách Rust **enforce** Dependency Rule ở mức compile: nếu ai lỡ `use sqlx::...` trong `domain`, build sẽ thất bại vì thiếu dependency, buộc phải chủ động thêm (và bị review bắt lỗi).

## Trait làm Port, static dispatch cho hot path

```rust
// crates/domain/src/ports.rs
pub trait WorkflowRepository {
    fn find_by_id(&self, id: &WorkflowId) -> Result<Option<WorkflowInstance>, DomainError>;
    fn save(&self, wf: &WorkflowInstance) -> Result<(), DomainError>;
}

// crates/domain/src/workflow.rs
pub struct WorkflowInstance {
    id: WorkflowId,
    state: WorkflowState,
}

impl WorkflowInstance {
    // business rule là method, trả Result thay vì panic
    pub fn transition(&mut self, event: WorkflowEvent) -> Result<(), DomainError> {
        match (&self.state, event) {
            (WorkflowState::Running, WorkflowEvent::Complete) => {
                self.state = WorkflowState::Completed;
                Ok(())
            }
            (state, event) => Err(DomainError::InvalidTransition {
                from: state.clone(), event,
            }),
        }
    }
}
```

```rust
// crates/application/src/complete_workflow.rs
// Static dispatch: generic + trait bound — không có chi phí vtable, phù hợp Engine hot path của BPMP
pub struct CompleteWorkflowUseCase<R: WorkflowRepository> {
    repo: R,
}

impl<R: WorkflowRepository> CompleteWorkflowUseCase<R> {
    pub fn handle(&mut self, id: &WorkflowId) -> Result<(), DomainError> {
        let mut wf = self.repo.find_by_id(id)?.ok_or(DomainError::NotFound)?;
        wf.transition(WorkflowEvent::Complete)?;
        self.repo.save(&wf)
    }
}
```

Khi cần thay Adapter tại runtime (VD: chọn `infra-postgres` hay `infra-raft` theo config) thay vì compile-time, dùng `dyn WorkflowRepository` thay cho generic — đánh đổi lấy linh hoạt, chấp nhận chi phí vtable indirection (thường không đáng kể trừ code path cực nóng).

## Composition root

```rust
// bin/engine/main.rs
fn main() {
    let repo = infra_postgres::PostgresWorkflowRepository::new(pool);
    let mut usecase = CompleteWorkflowUseCase { repo };
    // ... wiring gRPC server gọi usecase
}
```

## Ownership và ranh giới Aggregate — điểm khác biệt lớn nhất với Java/Go

- Aggregate root nên **own** các entity con bên trong nó (`Vec<TaskNode>` sở hữu trực tiếp), tránh dùng `Rc<RefCell<>>` tràn lan chỉ để né ownership — đó thường là dấu hiệu ranh giới Aggregate vẽ sai (quá lớn hoặc quá rời rạc).
- Nếu 2 aggregate cần tham chiếu nhau, dùng **ID reference** (giống DDD khuyến nghị chuẩn), không dùng con trỏ/`Arc` trỏ thẳng object — giữ đúng nguyên tắc "mỗi transaction chỉ sửa 1 aggregate".
- Domain error nên implement qua `thiserror`, KHÔNG dùng `anyhow` ở tầng domain (anyhow phù hợp tầng application/binary nơi không cần match cụ thể từng loại lỗi).

## Anti-pattern riêng của Rust cần tránh

| Anti-pattern | Vấn đề | Cách sửa |
|---|---|---|
| Domain crate phụ thuộc `sqlx`/`tokio` "cho tiện" | Mất lợi ích compile-time enforcement của Dependency Rule | Tách trait ở domain, implement async ở infra crate |
| Lạm dụng `Rc<RefCell<T>>` giữa các aggregate | Che giấu vi phạm ranh giới ownership, dễ panic runtime borrow | Dùng ID reference giữa aggregate, owned data trong aggregate |
| `unwrap()`/`expect()` trong domain logic | Panic thay vì Result — vi phạm nguyên tắc domain trả lỗi tường minh | Trả `Result<T, DomainError>`, propagate bằng `?` |
| Struct data + `impl` rỗng, logic nằm hết ở service function rời | Anemic domain model kiểu Rust | Method nghiệp vụ nằm trong `impl WorkflowInstance` |

## Liên hệ BPMP

`domain` + `application` crate của Rust Engine nên hoàn toàn độc lập với `infra-raft` (OpenRaft/RocksDB) — điều này cho phép viết unit test cho business rule của workflow (state machine transition) mà không cần khởi động Raft cluster, và mở đường cho hướng "Shadow Execution / Simulation" đang cân nhắc: chạy cùng domain logic trên 1 adapter khác (in-memory) để verify song song với Camunda 7 mà không đổi code domain.
