# Ward로 정의하는 알람: Condition의 종류, 신호 보내는 법, 예제

Ward에서 어떤 알람을 만들 수 있는지, 알람마다 프로그램이 신호를 어떻게 보낸다고
가정했는지를 정리한다. 유형마다 시나리오와 설정 예제를 같이 둔다.

이 문서의 규칙 예제는 모두 실제 검증을 통과한다 (`docs/conditions_test.go`).
신호 쪽 규약을 더 자세히 보려면 [signals.md](signals.md)를 본다.

## 1. 용어

| 용어 | 뜻 | 누가 정하나 |
| --- | --- | --- |
| **Rule** (규칙) | 알람의 정의. 어떤 신호를 어떻게 판정할지 적는다 | provider 파일 |
| **Condition** | (Rule, 노드, Key) 하나의 지금 상태 | Ward |
| **Issue** | Condition이 active였던 한 구간. **알람 한 번**이다. 열리고(open) 닫히며(close), 닫힌 이유가 남는다 | Ward |
| **Signal** (신호) | OTel 로그 레코드, 메트릭 data point | 프로그램, Collector |

"알람을 정의한다"는 규칙을 쓰는 것이고, "알람이 울린다"는 Issue가 열리는
것이다. 같은 규칙이라도 노드와 Key가 다르면 Condition이 따로 생긴다. 예를 들어
`ingest.decode_failed` 규칙은 노드마다, 스트림(Key)마다 Condition을 하나씩 갖는다.

```
프로그램 ──OTLP──▶ 노드의 Collector ──▶ Ward ──규칙──▶ Condition ──▶ Issue ──▶ gRPC API
                                         ▲
                                         └─ 증거: canary, 생존 counter, system.uptime
```

Condition의 상태:

| 상태 | 뜻 |
| --- | --- |
| `unobserved` | Ward가 기대하는 Condition인데 아직 아무 증거도 없다 ([8장](#8-정상-상태와-인벤토리)) |
| `inactive` | 열린 Issue가 없다. 판정 보류(`held`)가 아니면 정상이다 |
| `pending` | 활성화 조건의 일부만 찼다 (예: 2초 안에 3번 중 1번) |
| `active` | 문제가 있다. Issue가 열려 있다 |
| `stale` | 마지막으로는 active였지만 지금은 확인할 수 없다. Issue는 열려 있다 |
| `cleared` | latch 규칙에서 조건은 끝났고, Issue가 운영자의 reset을 기다린다 |

Issue가 닫힌 이유(`close_reason`):

| 이유 | 뜻 | 해결인가 |
| --- | --- | --- |
| `resolved.silence` | 반복 신호가 멈췄고, 그동안 소스가 살아 있었다 | 예 |
| `resolved.explicit` | 해제 레코드(`release`)가 왔다 | 예 |
| `resolved.condition` | 메트릭·노드·조합 규칙의 해제 조건을 만족했다 | 예 |
| `quiet` | 조용해졌지만, 해결됐다는 증거는 없다 | 아니오 |
| `interrupted.source_restart` | 소스가 재시작했다 | 아니오 |
| `interrupted.reboot` | 노드가 재부팅했다 | 아니오 |
| `interrupted.diag_gap` | Ward가 오래 꺼져 있어서 이어서 판정할 수 없다 | 아니오 |
| `rule_changed` | 규칙이 바뀌거나 지워졌다 | 아니오 |

## 2. 한눈에 보기

| # | 알람 유형 | 이럴 때 | 규칙 | 프로그램이 보낼 것 | 닫히는 이유 |
| --- | --- | --- | --- | --- | --- |
| 1 | 반복 오류 | 문제가 있는 동안 계속 실패한다 | log, `resolve_after` | 실패할 때마다 ERROR 로그, 생존 counter | `resolved.silence` |
| 2 | 한 번 오류와 해제 | 문제가 생길 때 한 번만 알 수 있다 | log, `forget_after`, `release` | 시작할 때 ERROR 한 번, 끝날 때 해제 레코드 | `resolved.explicit`, `quiet` |
| 3 | 계측 없는 ERROR | 아무 준비 없는 서비스의 ERROR | log, 심각도만 | ERROR 로그면 된다 | `quiet` |
| 4 | 표준 attribute로 거른 오류 | `error.type` 같은 semconv로 구분한다 | log, `attributes`, `key` | semconv attribute | `resolved.silence` |
| 5 | 프로세스 다운 | 프로세스가 죽었다 | metric, `points` | 생존 counter | `resolved.condition` |
| 6 | 재시작 반복 | 죽고 살아나기를 반복한다 | metric, `resets` | 생존 counter | `resolved.condition` |
| 7 | 멈춤(hang) | 살아 있지만 일을 안 한다 | metric, `rate` | 메인 루프에서 올리는 counter | `resolved.condition` |
| 8 | 처리율 저하 | 초당 처리량이 떨어졌다 | metric, `rate`, 단계 | 누적 counter | `resolved.condition` |
| 9 | 값의 단계 | 배터리, 온도, 디스크 사용률 | metric, `last`/`avg`/`min`/`max`, 단계 | gauge | `resolved.condition` |
| 10 | 급변 | 값이 빠르게 변한다 | metric, `change` | gauge | `resolved.condition` |
| 11 | 기준 대비 편차 | 측정값이 목표값에서 벗어났다 | metric, `relative_to` | gauge 두 개 (값, 목표) | `resolved.condition` |
| 12 | 이산 상태 | 장치가 failed, degraded다 | metric, `state` | 상태별 series 또는 상태 코드 | `resolved.condition` |
| 13 | 노드 관측 불가 | 노드의 신호가 Ward까지 오는지 모른다 | node | 없음 (Collector 설정) | `resolved.condition` |
| 14 | 조합 | 여러 알람이 함께일 때만 의미가 있다 | composite | 없음 (다른 규칙) | `resolved.condition` |
| 15 | 억제 | 원인 알람이 있을 때 결과 알람을 표시한다 | `suppress_when` | 없음 | (표시만) |

어떤 유형을 고를지는 [10장](#10-어떤-유형을-고를까)에 정리했다. 알람이 없을 때 "정상"을
보이는 법은 [8장](#8-정상-상태와-인벤토리)에 있다.

## 3. 프로그램 쪽 공통 규약

모든 유형에 공통인 것. 자세한 이유는 [signals.md](signals.md)에 있다.

- **`service.name`은 고정한다.** 소스 하나가 `service.name` 하나다. 같은 노드에서
  같은 `service.name`은 프로세스 하나로 본다. `service.instance.id`는 실행마다 바뀌어도 된다.
- **`host.name`은 프로그램이 붙이지 않는다.** 노드의 Collector가 붙이고, Ward는
  정적 설정의 `nodes.*.match`로 노드를 정한다.
- **생존 counter를 하나 낸다.** cumulative counter를 시작할 때 `Add(0)`으로 만들고,
  메인 루프에서 올린다. 반복 오류의 "해결"과 프로세스 다운, 재시작 판정이 모두 이것을 근거로 한다.
- **export 간격을 짧게 한다.** 메트릭은 1초 정도. Go SDK 기본값은 60초이므로
  `OTEL_METRIC_EXPORT_INTERVAL=1000`으로 줄인다.
- **attribute 타입을 지킨다.** 규칙은 타입까지 비교한다. bool `false`와 문자열
  `"false"`는 다르다. 키는 `diag.condition.type`처럼 점으로 이은 평평한 이름을 쓴다.
- **Body는 규칙이 읽지 않는다.** 사람을 위한 문장이다. 판정에 필요한 것은 attribute로 보낸다.
- **값을 모르면 data point를 보내지 않는다.** gauge에 0을 보내면 "0"이고, 보내지
  않으면 "모름"이다. Ward는 둘을 다르게 판정한다.

예제에서 쓰는 OTel Go 준비 코드:

```go
import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	logger = otelslog.NewLogger("ingest") // slog 레코드가 OTel 로그로 나간다
	meter  = otel.Meter("ingest")
)
```

## 4. 로그 규칙

로그 규칙은 레코드를 `match`로 고르고, `key`로 Condition을 나누고, `activate`로
켜고, `resolve_after`나 `forget_after`로 끈다.

### 4.1 반복 오류: 문제가 있는 동안 계속 실패한다

**이럴 때**: 프레임 디코딩 실패, 재시도할 때마다 실패하는 연결, 매 주기 실패하는 작업.
문제가 계속되면 신호도 계속 나오므로, 신호가 멈추면 해결로 볼 수 있다.

**프로그램이 보내는 것**: 실패할 때마다 ERROR 로그 한 건. 그리고 생존 counter.

```go
loops, _ := meter.Int64Counter("ingest.loops")
loops.Add(ctx, 0) // 시작할 때 series를 만든다

for {
	loops.Add(ctx, 1) // 메인 루프 한 바퀴마다
	frame, err := decode(stream)
	if err != nil {
		// 실패할 때마다 남긴다. throttle하더라도 resolve_after보다 짧은 간격으로만.
		logger.LogAttrs(ctx, slog.LevelError, "decode failed",
			slog.String("diag.condition.type", "ingest.decode_failed"),
			slog.String("diag.condition.key", stream.Name), // 예: "cam0"
			slog.Any("error", err))
		continue
	}
	// ...
}
```

**규칙**:

```yaml
- id: ingest.decode_failed
  kind: log
  source: ingest
  severity: error
  match: {attributes: {diag.condition.type: ingest.decode_failed}}
  key: [{attribute: diag.condition.key}]   # 스트림마다 Condition 하나
  activate: {count: 3, within: 2s}         # 2초 안에 3번이면 active
  resolve_after: 5s                        # 5초 조용하면 해결 판정
  on_source_restart: close
  operator: {ack: none, latch: false}
```

**시나리오** (노드 edge-1, 스트림 cam0):

| 이벤트 시각 | 신호 | 결과 |
| --- | --- | --- |
| 10:00:00.0 | 실패 | pending (1/3) |
| 10:00:00.5 | 실패 | pending (2/3) |
| 10:00:01.0 | 실패 | **active**, Issue 열림. `date_opened` 10:00:01.0 |
| 10:00:01.5 ~ 04.0 | 실패가 계속 | `count`가 늘고, `date_seen`은 10:00:04.0 |
| 10:00:09.0 | (5초 동안 없음) | 침묵이 끝났다. 아래 순서로 판정한다 |
| | 생존 counter가 10:00:09 뒤에도 같은 프로세스에서 온다 | **closed, `resolved.silence`**. `date_closed`는 마지막 신호 시각 10:00:04.0 |
| | 생존 counter가 3초(`max_gap`) 넘게 안 온다 | `stale`. 프로세스가 죽었을 수 있다 |
| | 노드를 관측할 수 없다 | 판정 보류. 60초가 넘으면 `stale` |
| | 10:00:04 뒤에 재시작했다 | **closed, `interrupted.source_restart`** |

**주의**:
- 침묵만으로는 해결이 아니다. 마지막 오류 직후 프로세스가 죽어도 로그는 멈추기
  때문이다. 그래서 침묵이 끝난 **뒤에** 같은 프로세스의 생존 counter가 와야 해결이다.
- 판정은 이벤트 시각으로 한다. 늦게 온 로그도 제 시각에 들어간다. 로컬 환경에서
  활성화는 신호 뒤 약 0.8–1.6초, 해결은 침묵이 끝난 뒤 약 2.5초에 API에 보인다.
- `on_source_restart: keep`이면 재시작해도 Issue를 닫지 않고 `restarts`만 올린다.
  재시작이 반복되는 동안 Issue가 매번 새로 열리지 않게 할 때 쓴다.

### 4.2 한 번 오류와 해제: 문제가 생길 때 한 번만 알 수 있다

**이럴 때**: "장치를 찾을 수 없음" 뒤에 재연결을 기다리는 경우처럼, 문제가 계속돼도
신호가 다시 나오지 않는다. 침묵은 해결을 뜻하지 않는다.

**프로그램이 보내는 것**: 문제가 생길 때 ERROR 한 번. 끝나면 같은 type과 key에
`diag.condition.active=false`(bool)를 단 해제 레코드.

```go
// 끊겼을 때 한 번
logger.LogAttrs(ctx, slog.LevelError, "device disconnected",
	slog.String("diag.condition.type", "ingest.device_disconnected"),
	slog.String("diag.condition.key", dev.ID)) // 예: "usb0"

// 다시 붙었을 때 한 번
logger.LogAttrs(ctx, slog.LevelInfo, "device reconnected",
	slog.String("diag.condition.type", "ingest.device_disconnected"),
	slog.String("diag.condition.key", dev.ID),
	slog.Bool("diag.condition.active", false)) // 반드시 bool
```

**규칙**:

```yaml
- id: ingest.device_disconnected
  kind: log
  source: ingest
  severity: error
  match: {attributes: {diag.condition.type: ingest.device_disconnected}}
  release: {attributes: {diag.condition.type: ingest.device_disconnected, diag.condition.active: false}}
  key: [{attribute: diag.condition.key}]
  activate: {count: 1}
  forget_after: 1h            # 해제가 오지 않으면 1시간 뒤 quiet
  on_source_restart: close
  operator: {ack: required, latch: false}
```

**시나리오** (장치 usb0):

| 이벤트 시각 | 신호 | 결과 |
| --- | --- | --- |
| 10:00:00 | disconnected | **active**, Issue 열림 |
| 10:05:00 | `diag.condition.active=false` (bool) | **closed, `resolved.explicit`**. `date_closed` 10:05:00 |
| (해제 대신) | `diag.condition.active="false"` (문자열) | 해제가 아니다. Issue는 그대로이고, `StatusService`에 타입 위반으로 보고된다 |
| (해제가 끝내 없음) | | 11:00:00에 **closed, `quiet`**. 해결이 아니다 |
| (해제 전에 재시작) | | **closed, `interrupted.source_restart`** |

**주의**:
- 해제 레코드는 발생으로 세지 않는다. `release`는 자기 조건만 본다. `match`의 심각도 조건 같은 것은 보지 않는다.
- `ack: required`이므로 닫혀도 운영자가 확인할 때까지 `attention`이 참이다.
- 해제는 `diag.*`가 아니어도 된다. `EventName`이나 다른 attribute로 `release`를 쓸 수 있다.

### 4.3 계측 없는 ERROR: 준비하지 않은 서비스

**이럴 때**: attribute를 하나도 붙이지 않은 서비스, 주기적으로 도는 배치 작업.
ERROR 로그가 반복되는지 알 수 없다.

**프로그램이 보내는 것**: OTLP로 나가는 ERROR 로그면 된다. 생존 counter가 없으면
소스를 `liveness: none`으로 선언한다.

**규칙**:

```yaml
- id: report-job.error
  kind: log
  source: report-job
  severity: error
  match: {severity: {gte: error}, attributes: {diag.condition.type: {exists: false}}}
  key: []
  activate: {count: 1}
  forget_after: 10m
  operator: {ack: none, latch: false}
```

**시나리오**: 02:00:00에 ERROR가 오면 active가 된다. 02:10:00까지 조용하면
`quiet`로 닫힌다. 그 사이에 또 오면 그 시각부터 다시 10분을 센다.

**주의**: 생존 증거가 없으므로 `resolve_after`를 써도 `resolved.silence`가 아니라
`quiet`로 닫힌다 (검증이 경고한다). `liveness: none` 소스에는 `on_source_restart`를 쓰지 않는다.

### 4.4 표준 attribute로 거른 오류

**이럴 때**: `diag.*` 없이 OTel semantic convention만으로 Condition을 만든다.
예를 들어 업로드 대상 서버마다 timeout을 따로 본다.

**프로그램이 보내는 것**: 오류 로그에 semconv attribute를 단다.

```go
logger.LogAttrs(ctx, slog.LevelError, "upload timed out",
	slog.String("error.type", "timeout"),
	slog.String("server.address", "storage.internal:443"))
```

**규칙**:

```yaml
- id: uploader.timeout
  kind: log
  source: uploader
  severity: warning
  match:
    severity: {gte: error}
    attributes: {error.type: timeout}
  key: [{attribute: server.address}]   # 서버마다 Condition 하나
  activate: {count: 3, within: 10s}
  resolve_after: 30s
  on_source_restart: keep
  operator: {ack: none, latch: false}
```

**주의**:
- 한 레코드가 여러 규칙에 맞으면 Issue도 여러 개 열린다.
- 본문에만 정보가 있으면 Collector의 transform processor로 attribute를 만든다
  ([signals.md](signals.md)의 "Without `diag.*`"). Ward는 Body를 읽지 않는다.
- `match`의 조건: 같음(`key: value`), `{in: [...]}`, `{exists: true|false}`,
  `{gt|gte|lt|lte: n}`. 심각도에는 `trace`, `debug`, `info`, `warn`, `error`, `fatal` 이름을 쓴다.

## 5. 메트릭 규칙

메트릭 규칙은 series를 고르고(`metric`), Key마다 값 하나로 줄이고(`value`), 그 값을
단계(`limit`)나 상태(`state`)로 판정한다.

| `value.of` | 값 | 프로그램이 낼 것 |
| --- | --- | --- |
| `last`, `avg`, `min`, `max` | 창 안의 마지막 값, 평균, 최소, 최대 | gauge |
| `rate` | 초당 증가량 | 누적 counter |
| `change` | 초당 변화량 | gauge |
| `points` | 창 안에 도착한 data point 수 | 아무 counter (보통 생존 counter) |
| `resets` | 창 안에서 다시 시작한 횟수 | 누적 counter |

공통 동작:
- 단계(`limit.levels`)는 심한 것부터 적는다. 위에서부터 처음 맞는 단계가 지금 단계다.
- `deadband`: 단계를 벗어나려면 경계에서 이만큼 더 돌아와야 한다. 경계 근처에서
  알람이 켜졌다 꺼졌다 하는 것을 막는다. 심한 단계에서 내려올 때는 지나가는 같은
  방향의 단계에도 적용된다 (예: low_low에서 21이면 low의 deadband 안이므로 low).
- `activate.for`: 정상이 아닌 값이 이만큼 이어져야 열린다. 더 심한 단계로 오를 때도 같다.
- `resolve.for`: 덜 심한 값이 이만큼 이어져야 내려간다. 정상이면 `resolved.condition`으로 닫힌다.
- 단계가 바뀌면 같은 Issue의 `level`과 `severity`가 바뀐다 (`update`). 더 심한
  단계로 오르면 확인(ack)이 풀린다.
- `startup_grace`: 소스가 재시작했거나 노드가 부팅한 뒤 이만큼은 판정하지 않는다.
- 창 안에 data point가 없으면 값을 모른다. active였다면 `stale`이 된다.

### 5.1 프로세스 다운

**이럴 때**: 프로세스가 죽었다. 죽은 프로세스는 로그를 남기지 못하므로, 와야 할
data point가 오지 않는 것으로 안다.

**프로그램이 보내는 것**: 생존 counter. cumulative counter는 값이 그대로여도 export
간격마다 data point가 나가므로, data point가 끊기면 프로세스나 그 전송이 멈춘 것이다.

**규칙**:

```yaml
- id: ingest.down
  kind: metric
  source: ingest
  severity: error
  metric: {name: ingest.loops}
  key: []
  value: {of: points, window: 10s, across: sum}   # 10초 동안 온 data point 수
  limit: {levels: [{name: down, below: 1, severity: error}], deadband: 0}
  activate: {for: 0s}
  resolve: {for: 10s}
  startup_grace: 10s
  operator: {ack: required, latch: false}
```

**시나리오**:

| 이벤트 시각 | 일 | 결과 |
| --- | --- | --- |
| 10:00:00 | 마지막 data point 뒤 프로세스 종료 | |
| 10:00:10 | 10초 창이 비었다 | **active**, `level` down (판정은 `eval_lag.metric` 3초 뒤) |
| 10:01:00 | 다시 시작 | 10초(`startup_grace`) 동안 판정하지 않는다 |
| 10:01:10 ~ 20 | data point가 계속 온다 | 10초(`resolve.for`) 정상이면 **closed, `resolved.condition`**. `restarts` 1 |

**주의**:
- 0개를 "안 왔다"로 보는 것은 노드 게이트가 정상일 때뿐이다. 노드가 꺼지거나
  네트워크가 끊기면 이 규칙은 판정을 보류하고, 노드 규칙(6.1)이 알린다.
- Ward가 시작한 뒤 한 번도 본 적 없는 소스는 "와야 할 곳"을 모른다. 처음 본 뒤부터 판정한다.

### 5.2 재시작 반복 (crash loop)

**이럴 때**: 죽고 다시 뜨기를 반복한다. 매번 금방 다시 뜨면 프로세스 다운 규칙으로는 보이지 않는다.

**프로그램이 보내는 것**: 생존 counter. 재시작은 counter의 StartTimeUnixNano나
Resource의 `service.instance.id`가 바뀌는 것으로 안다. 프로그램은 따로 할 일이 없다.

**규칙**:

```yaml
- id: ingest.crash_loop
  kind: metric
  source: ingest
  severity: error
  metric: {name: ingest.loops}
  key: []
  value: {of: resets, window: 5m, across: sum}   # 5분 안에 다시 시작한 횟수
  limit: {levels: [{name: crash_loop, above: 1, severity: error}], deadband: 0}   # 2번 이상
  activate: {for: 0s}
  resolve: {for: 5m}
  startup_grace: 0s
  operator: {ack: required, latch: false}
```

**주의**:
- 노드 재부팅에 따른 재시작은 세지 않는다. Ward가 처음 본 실행도 세지 않는다.
- 같은 노드에 같은 `service.name`으로 프로세스를 여럿 띄우면 서로를 재시작으로
  본다. 여럿이면 `service.name`을 나눈다.

### 5.3 멈춤 (hang)

**이럴 때**: 프로세스는 살아 있고 data point도 오지만, 일을 하지 않는다.

**프로그램이 보내는 것**: 생존 counter를 **메인 루프에서** 올린다. 메인 루프가
멈추면 counter가 평평해진다. SDK의 export는 별도 스레드라 data point는 계속 나가므로,
다운이 아니라 멈춤으로 보인다.

**규칙**:

```yaml
- id: ingest.stalled
  kind: metric
  source: ingest
  severity: error
  metric: {name: ingest.loops}
  key: []
  value: {of: rate, window: 5s, across: sum}
  limit: {levels: [{name: stalled, below: 1, severity: error}], deadband: 0}   # 초당 1번도 못 돈다
  activate: {for: 5s}
  resolve: {for: 5s}
  startup_grace: 10s
  operator: {ack: none, latch: false}
```

### 5.4 처리율 저하

**이럴 때**: 초당 메시지 수, 처리한 요청 수가 기준보다 떨어졌다.

**프로그램이 보내는 것**: 누적 counter. 스트림마다 attribute로 나누고, 시작할 때
`Add(0)`으로 series를 만든다. 그래야 아직 하나도 안 보낸 스트림이 "모름"이 아니라 0으로 보인다.

```go
sent, _ := meter.Int64Counter("uploader.messages")
for _, s := range streams {
	sent.Add(ctx, 0, metric.WithAttributes(attribute.String("stream", s)))
}
// 보낼 때마다
sent.Add(ctx, 1, metric.WithAttributes(attribute.String("stream", "main")))
```

**규칙**:

```yaml
- id: uploader.throughput_low
  kind: metric
  source: uploader
  severity: warning
  metric: {name: uploader.messages}
  key: [{attribute: stream}]                    # 스트림마다
  value: {of: rate, window: 5s, across: sum}
  limit: {levels: [{name: slow, below: 8, severity: warning}], deadband: 1}
  activate: {for: 5s}
  resolve: {for: 10s}
  startup_grace: 10s
  operator: {ack: none, latch: false}
  suppress_when: {rule: ingest.down}            # 원인이 따로 있으면 억제 표시 (6.3)
```

**시나리오**: 10/s에서 6/s로 떨어진 채 5초가 지나면 active가 된다. 다시 9/s 이상
(경계 8 + deadband 1)으로 10초가 지나면 `resolved.condition`으로 닫힌다. publish만
멈춰 counter가 평평하면 rate는 0이고 active다. 값을 내던 프로세스가 죽어 series가
없으면 이 규칙은 `stale`이 되고, 죽은 것은 5.1 규칙이 알린다.

**주의**:
- counter는 cumulative여야 한다. delta는 측정이 없는 구간에 data point를 내지 않아
  "0"과 "모름"을 구분할 수 없다.
- rate는 창 안에 data point가 2개 이상이고 그 간격이 창의 절반 이상일 때만
  계산한다. 그래서 export 간격은 창의 절반보다 짧아야 한다.
- 앱이 Hz를 직접 gauge로 내도 된다. 그때는 `value: {of: avg, ...}`로 같은 단계를 쓴다.

### 5.5 값의 단계: 배터리, 온도, 디스크

**이럴 때**: gauge 값이 단계(낮음·아주 낮음, 높음·아주 높음)를 넘었다.

**프로그램이 보내는 것**: gauge. 값을 읽어 오는 것은 콜백(observable gauge)으로
내는 것이 좋다. 콜백은 export할 때마다 불리고, 값을 못 읽으면 관측하지 않아
data point가 나가지 않는다.

```go
meter.Float64ObservableGauge("battery.charge",
	metric.WithUnit("%"),
	metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
		v, err := readBattery()
		if err != nil {
			return nil // 모르면 내지 않는다. 0을 내면 "0%"다
		}
		o.Observe(v, metric.WithAttributes(attribute.String("battery.id", "main")))
		return nil
	}))
```

**규칙**:

```yaml
- id: power.battery_low
  kind: metric
  source: power-agent
  severity: warning                   # 단계가 있으면 단계의 severity를 쓴다
  metric: {name: battery.charge, match: {battery.id: main}}
  key: []
  value: {of: last, window: 10s, across: min}
  limit:
    levels:                           # 심한 것부터
      - {name: low_low, below: 10, severity: error}
      - {name: low, below: 20, severity: warning}
    deadband: 2
  activate: {for: 30s}
  resolve: {for: 60s}
  startup_grace: 30s
  operator: {ack: required, latch: true}   # 회복해도 운영자가 reset할 때까지 cleared
```

**시나리오**:

| 이벤트 시각 | 값 | 결과 |
| --- | --- | --- |
| 10:00:00 | 25 → 18 | `low` 후보 |
| 10:00:30 | 18이 30초 이어짐 | **active**, `level` low, `severity` warning |
| 10:01:00 | 운영자가 확인 | `acked` |
| 10:02:00 | 9 | `low_low` 후보 |
| 10:02:30 | 30초 이어짐 | **update**, `level` low_low, `severity` error. 더 심해졌으므로 확인이 풀린다 |
| 10:03:00 | 11 | 아직 low_low다. low_low를 벗어나려면 경계 10에서 deadband 2만큼 더 올라 12 이상이어야 한다 |
| 10:03:10 | 21 | low_low는 벗어났지만 low의 deadband 안이다 (22 미만). low 후보가 되고, 60초(`resolve.for`) 이어지면 low로 내려간다 |
| 10:03:40 | 30 | 정상 후보 |
| 10:04:40 | 30이 60초 이어짐 | latch 규칙이므로 **cleared** (`resolved.condition`). 운영자가 `IssueService/Reset`하면 closed |

호스트 메트릭(서비스가 아니라 노드가 내는 값)에는 `source` 대신 `nodes`를 쓴다.
Collector의 hostmetrics가 내는 디스크 사용률이다. `system.filesystem.utilization`은
hostmetrics에서 기본으로 꺼져 있으므로 Collector 설정에서 켠다
(`scrapers: {filesystem: {metrics: {system.filesystem.utilization: {enabled: true}}}}`).

```yaml
- id: node.disk_full
  kind: metric
  nodes: [edge-1, edge-2]
  severity: warning
  metric: {name: system.filesystem.utilization, match: {mountpoint: /}}
  key: []
  value: {of: last, window: 30s, across: max}
  limit:
    levels:
      - {name: full, above: 0.95, severity: error}
      - {name: high, above: 0.9, severity: warning}
    deadband: 0.02
  activate: {for: 1m}
  resolve: {for: 5m}
  startup_grace: 0s
  operator: {ack: none, latch: false}
```

### 5.6 급변

**이럴 때**: 값의 크기보다 변하는 속도가 문제다. 예를 들어 온도가 빠르게 오른다.

**프로그램이 보내는 것**: gauge (5.5와 같다).

**규칙**:

```yaml
- id: cooling.temperature_rising
  kind: metric
  source: cooling
  severity: warning
  metric: {name: cooling.temperature}
  key: []
  value: {of: change, window: 30s, across: max}   # 초당 변화량
  limit: {levels: [{name: rising, above: 0.5, severity: warning}], deadband: 0.1}   # 초당 0.5도 넘게
  activate: {for: 0s}
  resolve: {for: 30s}
  startup_grace: 30s
  operator: {ack: none, latch: false}
```

### 5.7 기준 대비 편차

**이럴 때**: 측정값이 목표값(설정값)에서 벗어났다. 목표가 바뀌면 경계도 따라 바뀐다.

**프로그램이 보내는 것**: 측정값과 목표값을 각각 gauge로. 같은 Key로 짝지어진다.

```go
meter.Float64ObservableGauge("cooling.temperature", metric.WithFloat64Callback(readTemperature))
meter.Float64ObservableGauge("cooling.setpoint", metric.WithFloat64Callback(readSetpoint))
```

**규칙**:

```yaml
- id: cooling.off_setpoint
  kind: metric
  source: cooling
  severity: warning
  metric: {name: cooling.temperature}
  key: []
  value: {of: last, window: 10s, across: avg}
  limit:
    relative_to: {metric: {name: cooling.setpoint}, value: {of: last, window: 10s, across: avg}}
    levels:
      - {name: too_hot, above: 5, severity: warning}    # 목표보다 5도 넘게 높다
      - {name: too_cold, below: -5, severity: warning}  # 목표보다 5도 넘게 낮다
    deadband: 1
  activate: {for: 30s}
  resolve: {for: 30s}
  startup_grace: 30s
  operator: {ack: none, latch: false}
```

`relative_to`에는 상수도 쓸 수 있다: `relative_to: 20`. 목표값을 모르면(창 안에
목표 metric이 없으면) 판정하지 않는다.

### 5.8 이산 상태

**이럴 때**: 값이 크기가 아니라 상태다. 장치가 ok, degraded, failed다.

**프로그램이 보내는 것**: 두 가지 모양 중 하나.

1. **상태별 series** (OTel hardware 규약의 `hw.status` 모양): 상태마다 series 하나.
   지금 상태의 series만 1이고 나머지는 0이다.

   ```go
   states := []string{"ok", "degraded", "failed"}
   meter.Int64ObservableGauge("hw.status", metric.WithInt64Callback(
   	func(_ context.Context, o metric.Int64Observer) error {
   		for _, d := range disks {
   			cur := d.State() // 예: "degraded"
   			for _, s := range states {
   				v := int64(0)
   				if s == cur {
   					v = 1
   				}
   				o.Observe(v, metric.WithAttributes(
   					attribute.String("hw.id", d.ID),
   					attribute.String("hw.type", "physical_disk"),
   					attribute.String("hw.state", s)))
   			}
   		}
   		return nil
   	}))
   ```

2. **상태 코드**: gauge 값 자체가 상태 번호다 (예: 0 run, 1 idle, 2 fault).

**규칙** (상태별 series):

```yaml
- id: node.hardware
  kind: metric
  nodes: [edge-1, edge-2]
  severity: warning
  metric: {name: hw.status}
  key: [{attribute: hw.id}]                        # 장치마다
  value: {of: last, window: 30s, across: max}
  state:
    from: {attribute: hw.state}                    # 값이 0이 아닌 series의 hw.state
    normal: [ok]
    abnormal: {failed: error, degraded: warning}
    other: warning                                 # 목록에 없는 상태
  activate: {for: 0s}
  resolve: {for: 10s}
  startup_grace: 0s
  operator: {ack: none, latch: false}
```

**규칙** (상태 코드):

```yaml
- id: ingest.pump_state
  kind: metric
  source: ingest
  severity: warning
  metric: {name: pump.state}
  key: []
  value: {of: last, window: 10s, across: max}
  state:
    from: value                  # 값이 상태 이름이다: "0", "1", "2"
    normal: ["0", "1"]
    abnormal: {"2": error}
    other: warning
  activate: {for: 0s}
  resolve: {for: 10s}
  startup_grace: 10s
  operator: {ack: none, latch: false}
```

`level`에는 상태 이름(`degraded`, `"2"`)이 들어간다. 상태가 바뀌면 같은 Issue가
`update`되고, `normal`이 `resolve.for` 이어지면 닫힌다.

## 6. 노드, 조합, 억제

### 6.1 노드 관측 불가

**이럴 때**: 노드가 꺼졌거나, 네트워크가 끊겼거나, 노드의 Collector가 Ward로
보내지 못한다. 그 노드의 신호가 Ward에 다 도착하는지 알 수 없다.

**프로그램이 보내는 것**: 없다. Ward가 노드마다 canary(로그와 메트릭 한 건씩)를
그 노드의 Collector로 보내고, diag 파이프라인을 거쳐 돌아오는지 본다. 필요한 것은
Collector 설정이다 (`deploy/collector/collector.yaml`): `host.name`을 붙이고,
hostmetrics로 `system.uptime`을 낸다.

**규칙**:

```yaml
- id: node.unobservable
  kind: node
  nodes: [edge-1, edge-2]
  severity: error
  activate: {unobservable_for: 10s}
  resolve: {observable_for: 15s}
  operator: {ack: none, latch: false}
```

**동작**:
- `level`에 이유가 들어간다. 예: `log: unreachable`(canary를 보낼 수 없다),
  `log: missing`(canary가 돌아오지 않는다), `metric: error`(Collector가 오류로 답했다),
  `log: slow`(응답이 `max_rtt`보다 늦다).
- canary 하나가 한 번 늦은 것은 노드 장애로 세지 않는다.
- 이 규칙을 쓰지 않아도 관측 상태는 판정 보류에 쓰이고 `NodeService`로 보인다.
  이 규칙은 그것을 Issue로 남길지 정한다.
- 노드를 관측할 수 없는 동안 다른 규칙은 침묵이나 부재로 해결하지 않는다. 받은
  신호로 활성화하는 것은 계속한다.

### 6.2 조합

**이럴 때**: 하나하나는 흔하지만 함께일 때만 의미가 있다. 예를 들어 업로드가 느리면서
입력도 실패하면 파이프라인 전체가 막힌 것이다.

**규칙**:

```yaml
- id: pipeline.degraded
  kind: composite
  scope: node                  # 노드마다 따로. 참조도 같은 노드의 Condition을 본다
  severity: critical
  when:
    all:
      - {rule: uploader.throughput_low}
      - any:
          - {rule: ingest.decode_failed}
          - {rule: ingest.stalled}
  activate: {for: 5s}
  resolve: {for: 10s}
  operator: {ack: required, latch: false}
```

판정은 참·거짓·모름 세 값이다. Ward는 모름을 해결로 바꾸지 않는다.

| 대상 | 참 | 거짓 | 모름 |
| --- | --- | --- | --- |
| Condition 하나 | `active` | `inactive`, `pending`, `cleared` | `stale`, 판정 보류 |
| `{rule: id}` | 하나라도 참 | 모두 거짓이거나 없음 | 그 밖 |
| `{rule: id, key: k}` | 그 Key가 참 | 그 Key가 거짓이거나 없음 | 그 Key가 모름 |
| `all` | 모두 참 | 하나라도 거짓 | 그 밖 |
| `any` | 하나라도 참 | 모두 거짓 | 그 밖 |
| `not` | 거짓 | 참 | 모름 |

식이 모름이면 판정을 보류한다. active인 채로 60초(`gate.unobservable_to_stale`)
넘게 모르면 `stale`이 된다. `scope: global`이면 노드를 가리지 않고 하나만 만든다.

### 6.3 억제

**이럴 때**: 원인 알람이 있으면 그 결과로 생긴 알람은 따로 볼 필요가 없다. 예를 들어
입력 프로세스가 죽으면 업로드 처리율도 떨어진다.

모든 규칙에 `suppress_when`을 달 수 있다 (5.4의 `uploader.throughput_low` 참고).
식이 참인 동안 그 규칙의 Condition과 열린 Issue에 `suppressed`가 표시된다.

- **표시만 한다.** 판정과 기록은 그대로다. 숨길지는 API를 쓰는 쪽이 정한다.
- **모름이면 억제하지 않는다.** 알람을 숨기는 쪽으로 틀리지 않게 한다.
- 참조는 같은 노드의 Condition을 본다.

## 7. 알람에 붙는 속성과 운영자 처리

모든 규칙에 `operator`를 쓴다.

| 필드 | 값 | 뜻 |
| --- | --- | --- |
| `ack` | `required` | 확인해야 한다. 닫혀도 확인할 때까지 `attention`이 참이다 |
| | `none` | 확인하지 않는다 |
| `latch` | `true` | 해제 조건이 맞아도 닫지 않고 `cleared`로 둔다. 운영자가 reset해야 닫힌다 |
| | `false` | 해제 조건이 맞으면 닫는다 |

운영자 조작 (gRPC):

| 조작 | RPC | 효과 |
| --- | --- | --- |
| 확인 | `IssueService/Acknowledge` | `acked`. 이미 확인했으면 거절한다 |
| 메모 | `IssueService/Comment` | 전환 `comment`로 남는다 |
| latch 해제 | `IssueService/Reset` | `cleared`인 Issue를 닫는다. active면 거절한다 |
| 보류 | `ConditionService/Shelve` | `timed`(기간, 최대 `operator.max_shelve`) 또는 `one_shot`(지금 Issue가 닫힐 때까지). Issue에 `shelved` 표시 |
| 서비스 제외 | `ConditionService/RemoveFromService`, `PlaceInService` | 기한 없이 `out_of_service` 표시. 고장 난 센서처럼 오래 무시할 것에 쓴다 |

- `attention`이 참인 Issue가 "누군가 봐야 하는 알람"이다. 닫히지 않았거나,
  `ack: required`인데 아직 확인하지 않았다.
- 보류, 서비스 제외, 억제는 판정을 바꾸지 않는다. 표시만 한다.
- 모든 전환은 `IssueEventService/Follow`로 순서대로 흐른다. 알림 전달은 이 피드를 받아서 한다.

## 8. 정상 상태와 인벤토리

규칙은 비정상만 정의한다. 그래서 알람이 없으면 화면이 비기 쉽다. 대시보드가
"cam0 디코딩 정상", "disk1 보이지 않음", "배터리 정상 (54%)"처럼 정상과 부재까지
보이려면 두 가지가 필요하다: **무엇이 있어야 하는지**(인벤토리)와 **지금 정상이라는 증거**.

### 기대하는 Condition

Condition은 보통 신호가 와야 생긴다. Key(`cam0`, `disk1`)는 신호의 attribute에서
오므로, 신호가 오기 전에는 Ward도 그것이 있는지 모른다. 그래서 있어야 할 것을
Component member로 적는다.

- member가 규칙, 노드, Key를 모두 지정하면 Ward가 **기대하는 Condition**이다.
  규칙의 `key`가 `[]`이면 Key는 적지 않아도 지정한 것으로 본다.
- Ward는 정의가 반영될 때 그 Condition을 만든다. 증거가 오기 전에는 `unobserved`이고,
  메모리에서 버리지 않는다 (`declared: true`).
- 노드나 Key를 비운 member는 match일 뿐이다. 이미 있는 Condition을 Component에 넣지만
  아무것도 기대하지 않는다.
- 검증: Key가 없는 규칙에 Key를 쓰거나, `nodes`로 노드를 정한 규칙에 다른 노드를 쓰면
  거절한다. 그런 Condition은 영원히 관측되지 않기 때문이다.

이 문서 예제의 인벤토리:

```yaml
components:
  edge-1:
    parent: ""
    members:
      - {rule: node.unobservable, node: edge-1}
      - {rule: node.disk_full, node: edge-1}
  edge-1-ingest:
    parent: edge-1
    members:
      - {rule: ingest.down, node: edge-1}                          # 입력 프로세스가 있어야 한다
      - {rule: ingest.stalled, node: edge-1}
      - {rule: ingest.crash_loop, node: edge-1}
      - {rule: ingest.decode_failed, node: edge-1, key: cam0}      # 스트림 cam0, cam1
      - {rule: ingest.decode_failed, node: edge-1, key: cam1}
      - {rule: ingest.device_disconnected, node: edge-1, key: usb0}
  edge-1-disks:
    parent: edge-1
    members:
      - {rule: node.hardware, node: edge-1, key: disk0}            # 디스크 disk0, disk1의 hw.status
      - {rule: node.hardware, node: edge-1, key: disk1}
  edge-2-power:
    parent: ""
    members:
      - {rule: power.battery_low, node: edge-2}
      - {rule: cooling.temperature_rising, node: edge-2}
      - {rule: cooling.off_setpoint, node: edge-2}
  uploads:
    parent: ""
    members:
      - {rule: uploader.throughput_low}                            # 모든 노드와 스트림: match만
      - {rule: uploader.timeout}
      - {rule: pipeline.degraded}
```

### 무엇이 정상의 증거인가

| 규칙 | 기대하는 Condition이 `inactive`가 되는 때 | 정상인 동안 | 증거가 끊기면 |
| --- | --- | --- | --- |
| 메트릭 | 그 series의 data point가 처음 왔을 때 | `value`(판정한 값)와 `date_evaluated`가 갱신된다 | `held: true` (`no data`) |
| 로그 | 그 노드에서 소스의 생존 counter나 맞는 레코드가 처음 왔을 때 | 맞는 레코드가 없고, 소스의 생존 counter가 `max_gap` 안에 온다 | `held: true` (`source not alive`) |
| 노드, 조합 | 처음부터 있다 | | `held: true` |

**로그 규칙만으로는 장치가 있는지 알 수 없다.** 위의 `cam1` 스트림이 실제로는 없어도,
입력 프로세스가 살아 있고 `cam1` 오류 로그가 없으면 정상으로 보인다. 장치가 있는지를
보이려면 장치마다 메트릭 series를 내고(예: 스트림별 처리 프레임 수, 장치별 `hw.status`),
그 메트릭 규칙을 member로 둔다. 그러면 series가 오지 않는 장치는 `unobserved`로 남는다
(위의 `disk1`).

### 대시보드가 읽는 법

`ComponentService/List`는 Component마다 다음을 돌려준다:
- `health`
- `rules`: 그 아래에서 열릴 수 있는 알람의 종류
- `conditions`: 기대하는 것은 관측 전이어도 들어간다

Condition 하나는 이렇게 그리면 된다.

| Condition | 표시 |
| --- | --- |
| `unobserved` | 관측 안 됨 (있어야 하는데 증거가 없다) |
| `inactive`, `held: false` | 정상. 메트릭이면 `value`나 `value_state`를 같이 (예: "정상 (10.0 Hz)", "ok") |
| `inactive`, `held: true` | 모름 (`hold_reason`). 예: 소스가 죽었다 |
| `pending` | 주의. 활성화 조건이 일부 찼다 |
| `active` | 비정상. `level`, `severity`, `issue_id` |
| `stale` | 모름. 마지막으로는 비정상이었다 |
| `cleared` | 회복됐고 운영자의 reset을 기다린다 |

`date_signaled`는 마지막 신호 시각이다. 증거가 얼마나 최근인지 보일 때 쓴다.

`date_value_changed`는 읽은 것이 마지막으로 바뀐 시각이다. 상태 규칙은 읽은 상태를,
단계 규칙은 판정한 단계를 보며, 정상끼리 바뀐 것도 포함한다. "running, 2초 전부터"처럼
보일 때 쓴다.

`source`는 그 Condition의 신호를 내는 서비스, 곧 규칙의 `source`다. `RuleService`의
규칙에도 `source`와 `nodes`가 있다. 같은 이름으로 `SourceService`를 보면 그 서비스의
프로세스가 언제 시작했는지(`date_started`), 살아 있는지(`alive`)를 노드별로 알 수 있다.
그래서 신호가 아직 없는 Condition 옆에 그 생산자의 상태를 같이 보일 수 있다. 예를 들어
"cam1 디코딩: 관측 안 됨 / 생산자 ingest: 2초 전 시작"이다. 생산자의 시작 시각은
생존 counter의 첫 data point가 와야 알 수 있다.
본 적도 기대한 적도 없는 Condition을 `ConditionService/Get`하면 `NotFound`다.
"정상"이라고 답하지 않는다.

Component의 health는 아래 순서로 가장 나쁜 것을 고르고, 상위는 하위 중 가장 나쁜 값을 갖는다.

1. `unhealthy`: error·critical인 active Issue가 있다
2. `unknown`: stale이나 판정 보류인 Condition이 있거나, 노드를 관측할 수 없다
3. `degraded`: warning인 active Issue가 있다
4. `no_data`: `unobserved`인 Condition이 있거나, 속한 Condition이 없다
5. `healthy`: 위에 해당하지 않는다

member 없이 하위만 묶는 Component는 하위를 따른다.

## 9. 저절로 생기는 것과 판정이 보류되는 때

규칙에 쓰지 않아도 Ward가 하는 것:

| 상황 | Ward가 하는 일 |
| --- | --- |
| 노드를 관측할 수 없다 (게이트 실패) | 침묵·부재 판정을 보류한다. 60초 넘으면 active Condition을 `stale`로 바꾼다 |
| 노드의 부팅 시각을 아직 모른다 | 침묵 판정을 보류한다 (`system.uptime`이 필요하다) |
| 조용한 로그 Condition의 소스가 생존 counter를 보내지 않는다 | 정상이라고 하지 않고 판정 보류(`source not alive`)로 둔다. 조합과 억제에서 모름이다 |
| 노드가 재부팅했다 | 그 노드의 열린 로그·메트릭 Issue를 `interrupted.reboot`로 닫는다 |
| Ward가 `recovery_window`(120초)보다 오래 꺼져 있었다 | 열린 Issue를 `interrupted.diag_gap`으로 닫는다 |
| Ward가 짧게 꺼졌다 | 이어서 판정한다. Collector가 다시 보낸 신호는 두 번 세지 않는다 |
| 같은 레코드가 여러 번 왔다 | 하나로 센다 (레코드 전체가 같으면 같은 신호) |
| 판정이 끝난 구간에 신호가 늦게 왔다 | 판정을 뒤집지 않고 새 Issue로 연다. `StatusService`의 `late`가 오른다 |
| 규칙의 Key가 너무 많거나 Issue가 너무 빨리 열린다 | Key `*`, `level` `key_flood`인 Issue 하나로 대신한다 (`engine.limits`) |
| 어느 규칙에도 맞지 않는 레코드 | 버리고, 소스와 `diag.condition.type`별로 세어 `StatusService`로 보고한다 |
| 규칙이 보는 attribute가 다른 타입으로 왔다 | `StatusService`에 타입 위반으로 보고한다 |
| 어느 노드에도 맞지 않는 Resource | `StatusService`의 `unknown_hosts`로 보고한다 |
| 규칙의 의미가 바뀌었거나 지워졌다 | 열린 Issue를 `rule_changed`로 닫는다. severity만 바뀌면 열린 Issue에 반영한다 |

## 10. 어떤 유형을 고를까

1. **로그로 알 수 있나?**
   - 문제가 있는 동안 계속 로그가 나온다 → [4.1 반복 오류](#41-반복-오류-문제가-있는-동안-계속-실패한다).
     해결까지 판정하려면 생존 counter가 필요하다.
   - 시작할 때 한 번만 나온다 → [4.2 한 번 오류와 해제](#42-한-번-오류와-해제-문제가-생길-때-한-번만-알-수-있다).
     끝날 때 해제 레코드를 보낼 수 있으면 보낸다.
   - attribute를 붙일 수 없다 → [4.3](#43-계측-없는-error-준비하지-않은-서비스) 또는
     [4.4](#44-표준-attribute로-거른-오류). 본문밖에 없으면 Collector transform으로 attribute를 만든다.
2. **값으로 알 수 있나?**
   - 크기 → [5.5 단계](#55-값의-단계-배터리-온도-디스크)
   - 변하는 속도 → [5.6 급변](#56-급변)
   - 목표와의 차이 → [5.7 편차](#57-기준-대비-편차)
   - 상태 → [5.8 이산 상태](#58-이산-상태)
   - 횟수의 속도 → [5.4 처리율](#54-처리율-저하)
3. **프로세스 자체가 문제인가?**
   - 죽었다 → [5.1](#51-프로세스-다운)
   - 반복해서 죽는다 → [5.2](#52-재시작-반복-crash-loop)
   - 멈췄다 → [5.3](#53-멈춤-hang)
4. **노드가 문제인가?** → [6.1](#61-노드-관측-불가)
5. **여러 알람의 관계인가?** → [6.2 조합](#62-조합), [6.3 억제](#63-억제)

## 11. 설정 예제

### 정적 설정 (`ward.yaml`)

노드와 provider는 정적 설정에 쓴다. 모든 값을 적어야 한다. 전체는 저장소의
`ward.yaml`과 `deploy/compose/ward.yaml`에 있다.

```yaml
nodes:                        # 신호의 Resource attribute로 노드를 정한다
  edge-1: {match: {host.name: edge-1}, collector: "edge-1:4317"}   # collector는 canary를 보낼 곳
  edge-2: {match: {host.name: edge-2}, collector: "edge-2:4317"}

providers:                    # sources와 rules를 담은 파일. 바꾸면 재시작 없이 반영된다
  - {name: base, type: file, path: /etc/ward/providers/base.yaml, poll_interval: 10s, debounce: 200ms}
  - {name: apps, type: file, path: /etc/ward/providers/apps.yaml, poll_interval: 10s, debounce: 200ms}
```

### provider 파일

provider 파일은 `version`, `sources`, `rules`, `components`로 이루어진다. 위의 규칙
예제는 모두 `rules:` 목록의 항목이다. 이 문서의 예제가 쓰는 소스:

```yaml
sources:                      # key는 OTel service.name
  ingest:
    liveness: {metric: ingest.loops, max_gap: 3s}       # 생존 counter. max_gap은 export 간격의 3배쯤
  uploader:
    liveness: {metric: uploader.batches, max_gap: 3s}
  power-agent:
    liveness: {metric: power.samples, max_gap: 3s}
  cooling:
    liveness: {metric: cooling.samples, max_gap: 3s}
  report-job:
    liveness: none                                     # 생존 counter를 내지 않는다
```

Component 트리(인벤토리)의 예는 [8장](#8-정상-상태와-인벤토리)에 있다. 위 규칙, 소스와
함께 한 provider 파일에 넣으면 된다.

### 실제로 돌려 보기

`deploy/compose`는 노드 2개(Collector)와 예제 프로그램(emitter)을 띄운다. emitter는 위
신호 모양을 명령으로 낸다 (`example/emitter/README.md`).

```sh
cd deploy/compose && docker compose up -d --build

# 반복 오류 (4.1): 300 ms마다 실패, 멈추면 5초 뒤 resolved.silence
curl -X POST 'localhost:18081/condition/repeat?type=emitter.overheat&key=left&every=300ms'
curl -X POST 'localhost:18081/condition/stop?type=emitter.overheat&key=left'

# 한 번 오류와 해제 (4.2). as=string이면 타입 위반이라 해제되지 않는다
curl -X POST 'localhost:18081/condition/once?type=emitter.disconnected&key=cam'
curl -X POST 'localhost:18081/condition/release?type=emitter.disconnected&key=cam'

# 단계 (5.5), 상태 (5.8), 프로세스 다운 (5.1), 재시작 반복 (5.2)
curl -X POST 'localhost:18082/gauge?name=battery.charge&value=15&attr.battery.id=main'
curl -X POST 'localhost:18082/hw?id=disk0&state=degraded'
docker compose stop emitter-2
curl -X POST 'localhost:18081/exit?code=1&after=100ms'
```

자동 시나리오: `go test -tags e2e -count=1 -v ./deploy/compose/` (`deploy/README.md`).

로봇 한 대를 예로 든 샘플은 `example/robot/`에 있다. 역할이 다른 프로그램 넷이 각각 어떤
신호와 attribute를 내는지와, 그 신호로 Ward가 내리는 결론을 시뮬레이션으로 보인다.
