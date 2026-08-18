# Skill: common-lib-base

> Thiết kế thư viện dùng chung (common/shared lib) cho webapp — base entity (Audit/SoftDelete/StatefulEntity), generic repository, error/exception hierarchy thống nhất, logging/tracing convention, validation, config loading, testing utility. LUÔN dùng skill này khi user muốn "tạo base module", "shared library", "common lib", "tránh lặp code giữa các service", khi thiết kế entity mới cần audit/soft-delete/versioning, khi chuẩn hoá error response, hoặc khi bắt đầu 1 service mới cần bootstrap từ template có sẵn. Áp dụng cho Java/Spring Boot, Go và Rust.

# Common Lib Base cho Webapp

## Mục tiêu và ranh giới

Common lib chứa những gì **thật sự lặp lại ở ≥2 module/service** và **không chứa business rule cụ thể của module nào**. Đây là ranh giới quan trọng nhất — vi phạm nó là nguyên nhân số 1 khiến common lib biến thành "God Package" khó version, khó thay đổi vì mọi thứ phụ thuộc vào nó.

**Nên đưa vào common lib:**
- Base entity pattern (audit fields, soft delete, state machine khung)
- Generic repository abstraction (không phải business repository cụ thể)
- Generic type conversion framework (raw input → typed field, theo annotation/tag — xem mục riêng bên dưới)
- Error/Exception hierarchy + cách map ra HTTP/gRPC response thống nhất
- Logging/tracing convention (correlation ID, structured log field chuẩn)
- Validation helper thuần kỹ thuật (không phải business validation rule)
- Testing utility (test fixture builder, testcontainer setup chung)
- Config loading convention

**KHÔNG đưa vào common lib:**
- Bất kỳ business rule nào (VD: quy tắc archive document là gì) — cái đó thuộc domain của module sở hữu nó
- DTO cụ thể của 1 module — chỉ đưa base/generic type
- Logic gọi 1 external service cụ thể của riêng 1 module

## Quy trình thiết kế 1 thành phần common lib mới

1. Xác nhận đã có **≥2 chỗ dùng thật** (không phải "dự đoán sẽ dùng") — Rule of Three áp dụng ở đây mạnh hơn cả design pattern, vì cái giá của common lib sai là toàn bộ service phụ thuộc phải sửa theo.
2. Thiết kế API tối thiểu (minimal surface) — dễ thêm sau, khó bớt đi vì đã có người dùng.
3. Version hoá rõ ràng (semantic versioning cho package/crate/module riêng) — service dùng version nào, upgrade khi nào là quyết định của service đó, không auto-force.
4. Viết test cho common lib độc lập, không phụ thuộc bối cảnh của bất kỳ service cụ thể nào.
5. Document breaking change rõ trong changelog khi sửa base class/interface đã có người dùng.

## Base Entity pattern — 4 khối kinh điển

| Base type | Field/khả năng chuẩn | Khi dùng |
|---|---|---|
| **AuditEntity** | `createdAt`, `createdBy`, `updatedAt`, `updatedBy` | Mọi entity cần biết ai/khi nào tạo-sửa (gần như luôn cần trong hệ thống banking) |
| **SoftDeleteEntity** | `deletedAt`/`isDeleted`, query mặc định lọc bỏ record đã xoá | Entity không được xoá vật lý (audit trail, khôi phục được) |
| **StatefulEntity** | field `state` + generic state machine (allowed transitions) | Entity có vòng đời rõ ràng qua nhiều trạng thái (document lifecycle, workflow instance) |
| **VersionedEntity** | `version` (optimistic lock) | Entity có thể bị sửa đồng thời bởi nhiều request/service |

Chi tiết implement theo ngôn ngữ ở `references/`.

## Generic Type Conversion Framework

Mục tiêu: **1 framework convert kiểu dữ liệu dùng chung cho nhiều struct đích khác nhau** — raw input (String từ Excel/CSV/form/API) → field đã có kiểu đúng (Date, BigDecimal/Decimal, Enum...) — mà KHÔNG phải viết converter riêng cho từng struct. Đây chính là pattern "annotation-driven type conversion" đã dùng ở PDMS cho ETL 3 loại sheet khác nhau.

Tách 2 trách nhiệm rõ ràng:
1. **Converter** — biết cách convert 1 kiểu cụ thể (VD `String → LocalDate` theo pattern, `String → BigDecimal` theo scale/rounding, `String → Enum` theo mapping tuỳ chỉnh). Mỗi converter độc lập, test riêng được.
2. **Registry/Dispatcher** — đọc metadata gắn trên field đích (annotation ở Java, struct tag ở Go, attribute/derive macro ở Rust) để biết field nào cần converter nào, rồi áp dụng qua reflection/generic — struct nghiệp vụ chỉ cần khai báo metadata, không cần viết logic convert thủ công cho từng field.

Nguyên tắc thiết kế:
- **Field-level metadata đi cùng struct đích**, không tách rời ở nơi khác — dễ đọc, dễ maintain khi struct thay đổi.
- **Lỗi convert trả theo field** (tên field, giá trị gốc, lý do lỗi) — gộp thẳng vào cơ chế báo lỗi theo dòng của skill `stream-batch-processing` (1 row lỗi 1 field không dừng cả batch).
- **Registry mở rộng được** (đăng ký thêm converter cho kiểu mới) mà không sửa code core — bản chất là Strategy pattern (xem skill `design-patterns-apply`, converter = 1 strategy, registry = strategy dispatcher).
- **Converter core nằm trong common lib**, annotation/tag gắn trên struct nghiệp vụ nằm ở module sở hữu struct đó — common lib không biết gì về struct nghiệp vụ cụ thể.

## Error/Exception Hierarchy thống nhất

Nguyên tắc chung across ngôn ngữ: phân biệt rõ 3 tầng lỗi để mapping ra response code nhất quán toàn hệ thống:
1. **Domain error** — vi phạm business rule (VD: "document đang ACTIVE, không thể archive lần nữa") → map HTTP 409/422
2. **Validation error** — input sai định dạng/thiếu field → map HTTP 400
3. **Infrastructure error** — DB timeout, service phụ thuộc lỗi → map HTTP 502/503, thường kèm retry hoặc circuit breaker (xem skill `design-patterns-apply`)

Common lib nên cung cấp: base error type/class cho mỗi tầng + 1 global handler/middleware map error → response format chuẩn (khuyến nghị theo RFC 7807 Problem Details cho REST).

## Logging/Tracing convention

- Correlation ID / Trace ID sinh ở entry point (gateway/first handler), propagate xuyên suốt qua context (MDC ở Java, `context.Context` ở Go, tracing span ở Rust) — không để mỗi service tự sinh ID riêng cho cùng 1 request.
- Structured logging (JSON), field tên thống nhất giữa các service (`trace_id`, `service`, `module`, không phải mỗi service đặt tên khác nhau) để search/aggregate log tập trung được (ELK/Loki).
- KHÔNG log sensitive data (PII, số tài khoản, token) — common lib nên cung cấp helper mask sẵn cho các field nhạy cảm phổ biến trong banking domain.

## Tài liệu tham khảo theo ngôn ngữ

- `references/java-spring.md` — implement chi tiết AuditEntity/SoftDeleteEntity/StatefulEntity bằng JPA `@MappedSuperclass`, generic repository, annotation-driven type conversion framework (custom annotation + reflection registry), `@ControllerAdvice` cho error mapping (dựa trên base repository framework đã xây ở PDMS)
- `references/go.md` — struct embedding cho base fields, generic repository qua Go generics, struct-tag-driven type conversion framework, `errors` package convention, middleware cho tracing
- `references/rust.md` — trait-based base behavior (không có inheritance), generic repository qua trait + associated type, type conversion framework qua `TryFrom`/derive macro, `thiserror` hierarchy theo crate


---


# 📄 common-lib-base/references/go.md

# Common Lib Base với Go

## Base fields qua struct embedding

Go không có inheritance, nhưng struct embedding cho phép "kế thừa field" (không kế thừa method override như OOP thật):

```go
// internal/platform/base.go — package dùng chung
package platform

type AuditFields struct {
    CreatedAt time.Time
    CreatedBy string
    UpdatedAt time.Time
    UpdatedBy string
}

type SoftDeleteFields struct {
    DeletedAt *time.Time // nil = chưa xoá
}

func (s SoftDeleteFields) IsDeleted() bool { return s.DeletedAt != nil }

// Entity cụ thể embed base struct
type Document struct {
    ID     string
    Status DocumentStatus
    AuditFields      // embedding — Document.CreatedAt truy cập trực tiếp được
    SoftDeleteFields
}
```
Vì không có method override thật, logic set giá trị audit (VD: gán `CreatedBy` từ context user hiện tại) phải làm ở 1 hàm chung gọi tường minh trong Adapter/Repository layer — không tự động như Hibernate `@EntityListeners`.

```go
// platform/audit.go — helper dùng chung, gọi tường minh trước khi insert
func StampCreate(ctx context.Context, a *AuditFields) {
    now := time.Now()
    a.CreatedAt, a.UpdatedAt = now, now
    a.CreatedBy, a.UpdatedBy = UserFromContext(ctx), UserFromContext(ctx)
}
```

## StatefulEntity — generic qua Go generics + map transition

```go
package platform

type State comparable

type StateMachine[S State] struct {
    transitions map[S]map[S]bool
}

func NewStateMachine[S State](allowed map[S][]S) *StateMachine[S] {
    sm := &StateMachine[S]{transitions: map[S]map[S]bool{}}
    for from, tos := range allowed {
        sm.transitions[from] = map[S]bool{}
        for _, to := range tos {
            sm.transitions[from][to] = true
        }
    }
    return sm
}

func (sm *StateMachine[S]) CanTransition(from, to S) bool {
    return sm.transitions[from][to]
}
```
```go
// document package — khai báo transition hợp lệ, tận dụng generic base
var documentStateMachine = platform.NewStateMachine(map[DocumentStatus][]DocumentStatus{
    StatusActive:   {StatusArchived, StatusDeleted},
    StatusArchived: {StatusActive},
})

func (d *Document) Archive() error {
    if !documentStateMachine.CanTransition(d.Status, StatusArchived) {
        return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, d.Status, StatusArchived)
    }
    d.Status = StatusArchived
    return nil
}
```

## Generic Repository qua Go generics

```go
package platform

// Repository chung cho mọi entity có ID kiểu string — chỉ chứa thao tác THẬT SỰ chung
type Repository[T any] interface {
    FindByID(ctx context.Context, id string) (*T, error)
    Save(ctx context.Context, entity *T) error
}
```
Generic repository ở Go nên dừng lại ở mức tối thiểu (FindByID/Save) — query nghiệp vụ cụ thể (`FindActiveDocumentsByOwner`) khai báo ở interface riêng của từng bounded context (xem skill `clean-architecture-webapp`), không cố nhét vào generic interface dùng chung vì Go generics không hỗ trợ tốt query builder linh hoạt kiểu Java Criteria API.

## Error hierarchy — sentinel error + custom error type

```go
package platform

// Sentinel errors cho case đơn giản, so sánh bằng errors.Is
var (
    ErrNotFound          = errors.New("resource not found")
    ErrInvalidTransition = errors.New("invalid state transition")
)

// Custom error type khi cần mang thêm data, so sánh bằng errors.As
type ValidationError struct {
    Field   string
    Message string
}
func (e *ValidationError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Message) }

// Infrastructure error wrap error gốc, giữ nguyên chain qua %w
func WrapInfra(op string, err error) error {
    return fmt.Errorf("infra error during %s: %w", op, err)
}
```

```go
// Middleware map error → HTTP response thống nhất
func ErrorMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        defer func() {
            if err := recover(); err != nil { /* log + 500 */ }
        }()
        next.ServeHTTP(w, r)
    })
}

func WriteError(w http.ResponseWriter, err error) {
    var ve *ValidationError
    switch {
    case errors.As(err, &ve):
        writeProblem(w, http.StatusBadRequest, ve.Error())
    case errors.Is(err, ErrNotFound):
        writeProblem(w, http.StatusNotFound, err.Error())
    case errors.Is(err, ErrInvalidTransition):
        writeProblem(w, http.StatusConflict, err.Error())
    default:
        writeProblem(w, http.StatusInternalServerError, "internal error")
    }
}
```

## Generic Type Conversion Framework — struct-tag-driven

Go không có annotation như Java, nhưng struct tag + `reflect` đạt cùng mục đích: struct đích chỉ khai báo tag, framework tự đọc và convert.

```go
package platform

// Struct nghiệp vụ khai báo tag — không viết logic convert thủ công
type CreditRecord struct {
    DisbursementDate  time.Time `convert:"date" pattern:"02/01/2006" required:"true"`
    OutstandingBalance decimal.Decimal `convert:"decimal" required:"true"`
}

// Converter interface — mỗi kiểu 1 implementation
type FieldConverter interface {
    Convert(raw string, pattern string) (any, error)
}

type DateConverter struct{}
func (DateConverter) Convert(raw, pattern string) (any, error) {
    layout := pattern
    if layout == "" { layout = "2006-01-02" }
    t, err := time.Parse(layout, raw)
    if err != nil { return nil, fmt.Errorf("không parse được ngày %q theo layout %q: %w", raw, layout, err) }
    return t, nil
}

type DecimalConverter struct{}
func (DecimalConverter) Convert(raw, _ string) (any, error) {
    d, err := decimal.NewFromString(strings.ReplaceAll(raw, ",", ""))
    if err != nil { return nil, fmt.Errorf("không parse được số %q: %w", raw, err) }
    return d, nil
}

// Registry — map tên convert (trong tag) → converter, mở rộng bằng cách thêm entry
var registry = map[string]FieldConverter{
    "date":    DateConverter{},
    "decimal": DecimalConverter{},
}

// Engine — dùng reflect để đọc tag và set field, áp dụng cho BẤT KỲ struct đích nào
func ConvertRow[T any](raw map[string]string) (*T, []FieldError) {
    var target T
    var errs []FieldError
    v := reflect.ValueOf(&target).Elem()
    t := v.Type()

    for i := 0; i < t.NumField(); i++ {
        field := t.Field(i)
        convertTag, ok := field.Tag.Lookup("convert")
        if !ok { continue } // field không có tag → bỏ qua

        rawVal, present := raw[field.Name]
        if !present || rawVal == "" {
            if field.Tag.Get("required") == "true" {
                errs = append(errs, FieldError{Field: field.Name, Reason: "thiếu giá trị bắt buộc"})
            }
            continue
        }
        converter, ok := registry[convertTag]
        if !ok {
            errs = append(errs, FieldError{Field: field.Name, Reason: "không tìm thấy converter: " + convertTag})
            continue
        }
        converted, err := converter.Convert(rawVal, field.Tag.Get("pattern"))
        if err != nil {
            errs = append(errs, FieldError{Field: field.Name, Value: rawVal, Reason: err.Error()}) // lỗi theo field, không dừng cả row
            continue
        }
        v.Field(i).Set(reflect.ValueOf(converted))
    }
    return &target, errs
}
```
Thêm converter mới = thêm 1 entry vào `registry`, không sửa `ConvertRow` — cùng bản chất Strategy pattern như bản Java, chỉ khác cơ chế lookup (map thay vì DI container). Generic `ConvertRow[T any]` cho phép dùng chung 1 hàm cho mọi struct đích, giữ type-safety ở compile time cho phần return type dù bản thân việc set field vẫn qua `reflect` (không tránh được nếu muốn field-tag-driven approach — nếu cần triệt để tránh reflect vì performance, cân nhắc code generation lúc build thay vì reflect lúc runtime).

## Trace ID qua `context.Context`

```go
type traceIDKey struct{}

func WithTraceID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, traceIDKey{}, id)
}
func TraceIDFromContext(ctx context.Context) string {
    if id, ok := ctx.Value(traceIDKey{}).(string); ok { return id }
    return ""
}

// Middleware entry point
func TracingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        traceID := r.Header.Get("X-Trace-Id")
        if traceID == "" { traceID = uuid.NewString() }
        ctx := WithTraceID(r.Context(), traceID)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```
Luôn truyền `ctx` xuyên suốt mọi hàm (kể cả sang gRPC call sang Rust Engine của BPMP) để trace ID không bị đứt gãy giữa các service.

## Cảnh báo Go-specific

- Đừng cố mô phỏng `@MappedSuperclass` bằng cách gán method lên embedded struct rồi mong override như Java — Go dùng "method promotion" chứ không dispatch động; nếu cần polymorphism thật, dùng interface, không dùng embedding.


---


# 📄 common-lib-base/references/java-spring.md

# Common Lib Base với Java 21 / Spring Boot (JPA)

> Dựa trên framework base repository đã xây cho PDMS: AuditEntity, SoftDeleteEntity, StatefulEntity với generic state machine, CQRS + Event Sourcing patterns.

## AuditEntity — `@MappedSuperclass` + Spring Data Auditing

```java
@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class AuditEntity {
    @CreatedDate
    @Column(updatable = false)
    private Instant createdAt;

    @CreatedBy
    @Column(updatable = false)
    private String createdBy;

    @LastModifiedDate
    private Instant updatedAt;

    @LastModifiedBy
    private String updatedBy;
}
```
Bật auditing 1 lần ở config: `@EnableJpaAuditing(auditorAwareRef = "springSecurityAuditorAware")` — `AuditorAware` implementation lấy user hiện tại từ `SecurityContext`, dùng chung cho mọi entity kế thừa.

## SoftDeleteEntity — kết hợp `@SQLDelete` + `@Where` (Hibernate)

```java
@MappedSuperclass
public abstract class SoftDeleteEntity extends AuditEntity {
    private Instant deletedAt;
    public boolean isDeleted() { return deletedAt != null; }
}

// Ở entity cụ thể kế thừa:
@Entity
@SQLDelete(sql = "UPDATE documents SET deleted_at = now() WHERE id = ?")
@Where(clause = "deleted_at IS NULL") // mọi query qua Hibernate tự động lọc bỏ record đã xoá
class DocumentJpaEntity extends SoftDeleteEntity { ... }
```
Lưu ý: `@Where` không áp dụng cho native query — nếu module dùng nhiều native SQL (như ETL migration của PDMS), phải tự thêm điều kiện `deleted_at IS NULL` thủ công, common lib nên cung cấp constant tên cột chuẩn để tránh gõ sai.

## StatefulEntity — generic state machine

```java
@MappedSuperclass
public abstract class StatefulEntity<S extends Enum<S>> extends AuditEntity {
    protected S state;

    // Base method dùng chung: kiểm tra transition hợp lệ trước khi đổi state
    protected void transitionTo(S newState, StateMachine<S> stateMachine) {
        if (!stateMachine.canTransition(this.state, newState)) {
            throw new InvalidStateTransitionException(this.state, newState);
        }
        this.state = newState;
    }
}

// StateMachine định nghĩa transition hợp lệ — cấu hình 1 lần cho mỗi loại entity
public interface StateMachine<S> {
    boolean canTransition(S from, S to);
}

@Component
class DocumentStateMachine implements StateMachine<DocumentStatus> {
    private static final Map<DocumentStatus, Set<DocumentStatus>> TRANSITIONS = Map.of(
        DocumentStatus.ACTIVE, Set.of(DocumentStatus.ARCHIVED, DocumentStatus.DELETED),
        DocumentStatus.ARCHIVED, Set.of(DocumentStatus.ACTIVE)
    );
    public boolean canTransition(DocumentStatus from, DocumentStatus to) {
        return TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }
}
```
Entity cụ thể (`Document extends StatefulEntity<DocumentStatus>`) chỉ cần gọi `transitionTo(newState, stateMachine)` — business rule "trạng thái nào chuyển được sang trạng thái nào" khai báo tập trung, dễ audit, dễ vẽ sơ đồ.

## Generic Repository

```java
public interface BaseRepository<T extends AuditEntity, ID> extends JpaRepository<T, ID> {
    // Thêm method chung cho mọi entity — VD: tìm theo khoảng thời gian tạo
    List<T> findByCreatedAtBetween(Instant from, Instant to);
}
```
Cẩn thận: generic repository chỉ nên chứa query THẬT SỰ chung (theo audit field, theo soft-delete). Query nghiệp vụ cụ thể (`findByDocumentTypeAndStatus`) vẫn khai báo ở repository interface riêng của module, KHÔNG nhét vào `BaseRepository` dùng chung.

## Error hierarchy + `@ControllerAdvice` thống nhất

```java
public abstract class DomainException extends RuntimeException {
    public DomainException(String message) { super(message); }
}
public class InvalidStateTransitionException extends DomainException { ... } // → 409
public class ValidationException extends RuntimeException { ... }             // → 400
public class InfrastructureException extends RuntimeException { ... }         // → 502/503

@RestControllerAdvice
class GlobalExceptionHandler {
    @ExceptionHandler(DomainException.class)
    ProblemDetail handleDomain(DomainException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }
    @ExceptionHandler(ValidationException.class)
    ProblemDetail handleValidation(ValidationException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }
    // InfrastructureException: log đầy đủ stack trace nội bộ, trả message chung chung cho client
}
```
`ProblemDetail` (Spring 6+) implement sẵn RFC 7807 — không cần tự định nghĩa format response lỗi.

## Generic Type Conversion Framework — annotation-driven

Mục tiêu: 1 struct đích (VD `CreditRecordDto` cho migration, `ExcelRowDto` cho ETL) chỉ cần khai báo annotation trên field, không viết code convert thủ công cho từng field/từng struct.

```java
// common lib — annotation định nghĩa metadata convert
@Retention(RUNTIME) @Target(FIELD)
public @interface ConvertAs {
    Class<? extends FieldConverter<?>> converter();
    String pattern() default "";      // VD date pattern "dd/MM/yyyy"
    boolean required() default true;
}

// common lib — interface converter, mỗi kiểu 1 implementation
public interface FieldConverter<T> {
    T convert(String raw, String pattern) throws FieldConversionException;
}

@Component
class DateFieldConverter implements FieldConverter<LocalDate> {
    public LocalDate convert(String raw, String pattern) throws FieldConversionException {
        try {
            return LocalDate.parse(raw, DateTimeFormatter.ofPattern(pattern.isEmpty() ? "yyyy-MM-dd" : pattern));
        } catch (DateTimeParseException e) {
            throw new FieldConversionException(raw, "Không parse được ngày theo pattern " + pattern);
        }
    }
}

@Component
class DecimalFieldConverter implements FieldConverter<BigDecimal> {
    public BigDecimal convert(String raw, String pattern) throws FieldConversionException {
        try { return new BigDecimal(raw.replace(",", "")); }
        catch (NumberFormatException e) { throw new FieldConversionException(raw, "Không parse được số"); }
    }
}
```

```java
// common lib — Registry đọc annotation qua reflection, áp dụng cho BẤT KỲ struct đích nào
@Component
public class TypeConversionEngine {
    private final ApplicationContext context; // để lấy converter bean theo Class

    public <T> ConversionResult<T> convertRow(Map<String, String> rawRow, Class<T> targetType) {
        T instance = instantiate(targetType);
        List<FieldError> errors = new ArrayList<>();

        for (Field field : targetType.getDeclaredFields()) {
            ConvertAs meta = field.getAnnotation(ConvertAs.class);
            if (meta == null) continue; // field không có annotation → bỏ qua, không ép convert
            String raw = rawRow.get(field.getName());
            if (raw == null || raw.isBlank()) {
                if (meta.required()) errors.add(new FieldError(field.getName(), null, "Thiếu giá trị bắt buộc"));
                continue;
            }
            try {
                FieldConverter<?> converter = context.getBean(meta.converter());
                field.setAccessible(true);
                field.set(instance, converter.convert(raw, meta.pattern()));
            } catch (FieldConversionException e) {
                errors.add(new FieldError(field.getName(), raw, e.getMessage())); // lỗi theo field, KHÔNG throw dừng cả row
            }
        }
        return new ConversionResult<>(instance, errors);
    }
}
```

```java
// Struct nghiệp vụ CHỈ khai báo metadata — không có logic convert nào ở đây
public class CreditRecordDto {
    @ConvertAs(converter = DateFieldConverter.class, pattern = "dd/MM/yyyy")
    private LocalDate disbursementDate;

    @ConvertAs(converter = DecimalFieldConverter.class)
    private BigDecimal outstandingBalance;
}
```

Thêm converter mới (VD cho 1 kiểu enum đặc thù) = thêm 1 `@Component implements FieldConverter<X>`, không sửa `TypeConversionEngine` — đúng Open/Closed Principle, bản chất là Strategy pattern với `ApplicationContext` đóng vai trò registry tra cứu theo `Class`. `ConversionResult` gộp lỗi từng field, đưa thẳng vào cơ chế báo lỗi theo dòng của ETL (skill `stream-batch-processing`).

## Correlation ID / MDC

```java
@Component
class CorrelationIdFilter extends OncePerRequestFilter {
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String traceId = Optional.ofNullable(req.getHeader("X-Trace-Id")).orElse(UUID.randomUUID().toString());
        MDC.put("trace_id", traceId); // mọi log statement sau đó tự động kèm trace_id nếu log pattern có %X{trace_id}
        try { chain.doFilter(req, res); } finally { MDC.clear(); }
    }
}
```


---


# 📄 common-lib-base/references/rust.md

# Common Lib Base với Rust

## Base behavior qua trait (không có inheritance, không có struct embedding tự động như Go)

Rust không có cách "nhúng field + kế thừa" tiện như Go embedding hay Java `@MappedSuperclass`. Cách chuẩn: trait định nghĩa behavior chung, struct cụ thể tự chứa field và implement trait (đôi khi lặp field — chấp nhận được, đổi lấy tường minh).

```rust
// crates/common/src/audit.rs
pub trait Audited {
    fn created_at(&self) -> DateTime<Utc>;
    fn created_by(&self) -> &str;
    fn touch(&mut self, actor: &str); // cập nhật updated_at/updated_by
}

// Macro giảm boilerplate khi nhiều struct cần cùng field — cân nhắc dùng derive macro riêng
// nếu số lượng entity lớn (>10), nếu ít hơn thì viết tay impl rõ ràng hơn là thêm 1 proc-macro crate
#[derive(Debug, Clone)]
pub struct AuditMeta {
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

pub struct Document {
    pub id: DocumentId,
    pub status: DocumentStatus,
    pub audit: AuditMeta, // composition thay vì embedding — field lồng, không phẳng
}
```

## SoftDelete qua `Option<DateTime<Utc>>` — tận dụng type system thay vì boolean flag

```rust
pub struct SoftDeletable {
    pub deleted_at: Option<DateTime<Utc>>,
}

impl SoftDeletable {
    pub fn is_deleted(&self) -> bool { self.deleted_at.is_some() }
    pub fn delete(&mut self, at: DateTime<Utc>) { self.deleted_at = Some(at); }
}
```
`Option<T>` thay cho `bool isDeleted` buộc mọi nơi đọc giá trị phải xử lý cả 2 case (compiler bắt buộc qua `match`/`if let`), khó quên check hơn so với 1 field boolean rời rạc.

## StatefulEntity — generic qua trait + associated type, enforce exhaustive match

```rust
pub trait StateMachine {
    type State: PartialEq + Copy;
    fn can_transition(&self, from: Self::State, to: Self::State) -> bool;
}

pub struct DocumentStateMachine;

impl StateMachine for DocumentStateMachine {
    type State = DocumentStatus;
    fn can_transition(&self, from: DocumentStatus, to: DocumentStatus) -> bool {
        matches!(
            (from, to),
            (DocumentStatus::Active, DocumentStatus::Archived)
                | (DocumentStatus::Archived, DocumentStatus::Active)
        )
    }
}

impl Document {
    pub fn transition_to(&mut self, to: DocumentStatus, sm: &impl StateMachine<State = DocumentStatus>)
        -> Result<(), DomainError>
    {
        if !sm.can_transition(self.status, to) {
            return Err(DomainError::InvalidTransition { from: self.status, to });
        }
        self.status = to;
        Ok(())
    }
}
```
Với BPMP Engine — nơi state machine của workflow phức tạp hơn nhiều (nhiều state, nhiều event) — cân nhắc enum `WorkflowEvent` cho input thay vì chỉ `to: State`, để `match (state, event)` bắt exhaustive ngay cả khi thêm event mới (compiler báo lỗi thiếu case, giá trị lớn nhất của Rust cho state machine).

## Generic Repository qua trait + associated type

```rust
// crates/common/src/repository.rs
pub trait Repository {
    type Entity;
    type Id;
    type Error;

    fn find_by_id(&self, id: &Self::Id) -> Result<Option<Self::Entity>, Self::Error>;
    fn save(&self, entity: &Self::Entity) -> Result<(), Self::Error>;
}
```
Giữ tối thiểu giống khuyến nghị bên Go — query nghiệp vụ cụ thể định nghĩa ở trait riêng của domain crate (`WorkflowRepository` trong ví dụ skill `clean-architecture-webapp`), KHÔNG cố tổng quát hoá query phức tạp vào trait chung này.

## Error hierarchy — `thiserror` theo tầng, `anyhow` ở binary

```rust
// crates/domain/src/error.rs — lỗi domain, match được từng case
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("invalid transition from {from:?} to {to:?}")]
    InvalidTransition { from: DocumentStatus, to: DocumentStatus },
    #[error("document not found")]
    NotFound,
}

// crates/infra-postgres/src/error.rs — lỗi infra, wrap lỗi driver
#[derive(Debug, thiserror::Error)]
pub enum InfraError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

// bin/engine/main.rs — binary dùng anyhow để gộp mọi loại lỗi, không cần match cụ thể ở top-level
fn run() -> anyhow::Result<()> {
    let doc = repo.find_by_id(&id)?; // ? tự convert nhờ #[from] hoặc From impl
    Ok(())
}
```
Nguyên tắc: **library crate (domain/application/infra) dùng `thiserror`** (lỗi cụ thể, người gọi match được), **binary crate dùng `anyhow`** (chỉ cần propagate + log, không cần match).

## Generic Type Conversion Framework — `TryFrom` + trait, không cần reflection

Rust không có reflection runtime (cố ý, đổi lấy hiệu năng và an toàn kiểu) — cách idiomatic để có "1 framework convert dùng chung cho nhiều struct" là qua **trait chuẩn hoá + derive macro** (hoặc field-by-field `TryFrom` viết tay khi số lượng struct chưa đủ lớn để đáng viết proc-macro).

**Cách 1 — viết tay qua `TryFrom<RawRow>` (đủ dùng khi không quá nhiều struct đích):**
```rust
// common lib — trait chuẩn hoá lỗi convert theo field
pub trait RowConvert: Sized {
    fn try_from_row(raw: &HashMap<String, String>) -> Result<Self, Vec<FieldError>>;
}

pub struct FieldError { pub field: String, pub value: Option<String>, pub reason: String }

// common lib — converter thuần cho từng kiểu, tái dùng ở mọi struct
pub fn parse_date(raw: &str, pattern: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(raw, pattern).map_err(|e| format!("không parse được ngày: {e}"))
}
pub fn parse_decimal(raw: &str) -> Result<Decimal, String> {
    Decimal::from_str(&raw.replace(',', "")).map_err(|e| format!("không parse được số: {e}"))
}
```
```rust
// Struct nghiệp vụ — implement TryFrom bằng cách gọi lại converter chung của common lib,
// KHÔNG viết lại logic parse, chỉ khai báo field nào dùng converter nào
pub struct CreditRecord {
    pub disbursement_date: NaiveDate,
    pub outstanding_balance: Decimal,
}

impl RowConvert for CreditRecord {
    fn try_from_row(raw: &HashMap<String, String>) -> Result<Self, Vec<FieldError>> {
        let mut errors = Vec::new();

        let disbursement_date = raw.get("disbursement_date")
            .ok_or_else(|| errors.push(FieldError { field: "disbursement_date".into(), value: None, reason: "thiếu giá trị".into() }))
            .ok()
            .and_then(|v| parse_date(v, "%d/%m/%Y").map_err(|e| errors.push(FieldError { field: "disbursement_date".into(), value: Some(v.clone()), reason: e })).ok());

        // ... tương tự cho outstanding_balance

        if !errors.is_empty() { return Err(errors); }
        Ok(CreditRecord {
            disbursement_date: disbursement_date.unwrap(),
            outstanding_balance: /* ... */ Decimal::ZERO,
        })
    }
}
```

**Cách 2 — derive macro riêng (chỉ đáng viết khi ≥10-15 struct đích lặp lại pattern này, xem cảnh báo proc-macro ở phần Base Entity):**
```rust
// Mục tiêu hướng tới nếu volume đủ lớn — struct chỉ cần attribute, macro tự sinh impl TryFrom
#[derive(RowConvert)]
struct CreditRecord {
    #[convert(date, pattern = "%d/%m/%Y")]
    disbursement_date: NaiveDate,
    #[convert(decimal)]
    outstanding_balance: Decimal,
}
```
Macro `#[derive(RowConvert)]` sinh code tương đương Cách 1 tại compile time — giữ được cả 2 lợi ích: khai báo ngắn gọn như Java annotation/Go tag, VÀ vẫn type-safe + zero-cost tại runtime (không có `reflect`/`Field.setAccessible` như Java/Go, mọi field access đã resolve lúc compile).

Lựa chọn Cách 1 hay Cách 2 phụ thuộc số lượng struct đích thực tế — bắt đầu Cách 1, chỉ đầu tư viết proc-macro khi thấy boilerplate lặp lại đủ nhiều (đúng nguyên tắc Rule of Three đã nêu ở phần đầu skill).

## Trace ID qua `tracing` span

```rust
use tracing::instrument;

#[instrument(skip(repo), fields(trace_id = %trace_id))]
async fn handle_archive(trace_id: String, repo: &impl WorkflowRepository, id: WorkflowId) -> Result<(), DomainError> {
    // mọi tracing::info!/warn! bên trong hàm này tự động kèm trace_id vào span
    ...
}
```
Với gRPC (tonic), gắn interceptor đọc header trace ID từ Go gateway (nếu có) và tạo span kế thừa — giữ liền mạch trace_id giữa Go service và Rust Engine trong BPMP.

## Cảnh báo Rust-specific

- Đừng cố dựng 1 proc-macro derive phức tạp cho base entity ngay từ đầu — proc-macro tăng chi phí bảo trì và compile time đáng kể; chỉ đáng làm khi số lượng entity lặp lại pattern đủ lớn (>10-15) và đã thấy rõ boilerplate thật sự đau.
