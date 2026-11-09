# 샘플: 로봇 한 대의 진단

로봇 한 대에서 도는 프로그램들이 OpenTelemetry로 신호를 내고, Ward가 그 신호로
"로봇을 못 쓴다", "성능이 떨어졌다", "상태가 바뀌었다"를 판정하는 예다.

| 파일 | 내용 |
| --- | --- |
| `README.md` | 어떤 프로그램이 어떤 신호를 내는지, Ward가 무엇을 판정하는지 |
| `rules.yaml` | Ward의 provider 파일: 소스, 규칙, 인벤토리 |
| `robot_test.go` | 프로그램들의 모형이 이 신호를 내고 Ward가 판정하는 시뮬레이션. 시나리오마다 열리고 닫히는 알람을 확인한다 |

```sh
go test -v ./example/robot/
```

등급은 이렇게 쓴다.

| 등급 | 뜻 |
| --- | --- |
| error | 로봇을 못 쓴다 |
| warning | 쓸 수는 있지만 성능이 떨어졌거나 곧 문제가 생긴다 |
| info | 상태 변화다 |

## 1. 구성

```
로봇 robot-1
┌─────────────────────────────────────────────────────────────┐
│ robot-controller ─┐                                         │
│ power-manager ────┤ OTLP                                    │
│ sensor-hub ───────┼──────▶ Collector ── host.name=robot-1 ──┼──▶ Ward ──▶ gRPC API
│ fault-reporter ───┘        (system.uptime)                  │      │
│                               ▲                             │      │
└───────────────────────────────┼─────────────────────────────┘      │
                                └──────── canary ────────────────────┘
```

| 프로그램 (`service.name`) | 역할 | 내는 신호 |
| --- | --- | --- |
| `robot-controller` | 실시간 제어. 모터, 관절, 비상정지, 보드, 로봇 SDK를 다룬다 | 메트릭: 비상정지, 보드 폴트, 관절 잠금, 모터 상태와 온도, 관절 갱신, RT 사이클, SDK 큐 drop |
| `power-manager` | 배터리 관리 시스템(BMS)과 이야기한다 | 메트릭: 잔량, 연결, 전원 상태 |
| `sensor-hub` | 카메라, IMU 같은 센서 드라이버를 묶는다 | 메트릭: 센서마다 상태 |
| `fault-reporter` | 하드웨어와 SDK의 에러 코드를 모아 보고한다 | 로그: 에러 코드마다 발생, 반복, 해제 |
| 노드의 Collector | 프로그램들의 신호를 받아 Ward로 보낸다 | `host.name`을 붙인다. hostmetrics로 `system.uptime`을 낸다 |

네 프로그램 모두 메인 루프가 한 바퀴 돌 때마다 `robot.loop`을 올린다. Ward는 이
counter로 프로그램마다 살아 있는지, 멈췄는지, 재시작했는지를 안다.

## 2. 모든 프로그램이 지키는 것

**Resource attribute**

| attribute | 값 |
| --- | --- |
| `service.name` | 위 표의 이름. 고정한다 |
| `service.instance.id` | 실행마다 새 값 (예: UUID). 바뀌면 Ward는 재시작으로 본다 |
| `host.name` | 프로그램이 붙이지 않는다. 노드의 Collector가 `robot-1`을 붙이고, Ward는 이것으로 로봇을 구분한다 |

**전송**: OTLP로 노드의 Collector에 보낸다. 메트릭은 2초마다 export하고
(`OTEL_METRIC_EXPORT_INTERVAL=2000`), temporality는 cumulative다.

**신호의 모양**

| 신호의 성격 | 계측기 | 이 샘플의 예 |
| --- | --- | --- |
| 지금 켜졌나 꺼졌나 | ObservableGauge, 0 또는 1 | `robot.estop.active` |
| 여러 상태 중 하나 | ObservableGauge, 상태마다 series 하나, 지금 상태만 1 | `robot.motor.state` |
| 측정값 | ObservableGauge (실수) | `robot.motor.temperature` |
| 세는 사건 | Counter, ObservableCounter (누적) | `robot.sdk.push_drops`, `robot.loop` |
| 맥락이 있는 사건 | 로그 | `fault-reporter`의 에러 코드 |

지속되는 상태(비상정지, 폴트, 모터와 센서 상태)는 켜짐, 꺼짐 로그가 아니라 메트릭으로
낸다. 로그는 하나를 잃으면 상태가 틀린 채로 남고, 프로그램이 죽으면 마지막 상태가 계속
참인 것처럼 보인다. 메트릭은 매 export가 지금 상태를 다시 말하고, 프로그램이 죽어 data
point가 끊기면 Ward는 그 상태를 "모름"으로 본다.

**함께 지킬 것**

- 시작할 때 모든 series를 만든다. counter는 `add(0)`, 상태 series는 모든 상태를 0과 1로
  낸다. "아직 한 번도 안 일어남"이 "모름"이 아니라 0으로 보이게 하려는 것이다.
- 모르는 값은 내지 않는다. 0을 내면 "0"이다.
- attribute 값은 모두 문자열이고, 타입을 바꾸지 않는다.
- `robot.loop`은 반드시 메인 루프 안에서 올린다. 별도 스레드에서 올리면 루프가 멈춰도 늘어난다.
- 실시간 스레드에서는 OTel API를 부르지 않는다. 잠금이나 메모리 할당이 생길 수 있다.
  실시간 스레드는 원자 변수만 갱신하고, 비동기 계측기의 콜백이 export 때 읽는다.

## 3. 프로그램별 신호

메트릭 이름은 모두 `robot.`으로 시작한다.

### robot-controller

| 메트릭 | 계측기 | 값 | attribute | 뜻과 내는 법 |
| --- | --- | --- | --- | --- |
| `robot.loop` | Counter | 정수, `{cycle}` | | 제어 노드의 메인 루프가 돈 횟수 |
| `robot.estop.active` | ObservableGauge | 0, 1 | `estop.source`: `wired`, `software`, `wireless` | 1이면 비상정지가 걸렸다. 세 source를 항상 모두 낸다 |
| `robot.board.fault` | ObservableGauge | 0, 1 | `board.id`: `main`, `power` | 1이면 보드 폴트. 모든 보드를 항상 낸다 |
| `robot.joint_lock.locked` | ObservableGauge | 0, 1 | | 1이면 관절이 잠겼다 |
| `robot.motor.state` | ObservableGauge | 0, 1 | `motor.id`, `motor.state`: `ok`, `off`, `disconnected`, `error` | 모터마다 네 series. 지금 상태만 1 |
| `robot.motor.temperature` | ObservableGauge | 실수, `Cel` | `motor.id` | 모터 온도. 못 읽으면 내지 않는다 |
| `robot.joint.stale` | ObservableGauge | 0, 1 | `joint.id` | 지난 export 이후 그 관절의 갱신이 0번이면 1 |
| `robot.rt_cycle.count` | ObservableCounter | 정수, `{cycle}` | | 실시간 사이클의 누적 수. 실시간 스레드는 원자 변수만 올린다 |
| `robot.rt_cycle.period_max` | ObservableGauge | 실수, `us` | | 지난 export 이후 가장 긴 사이클 주기. 콜백이 읽고 0으로 되돌린다 |
| `robot.sdk.push_drops` | Counter | 정수, `{message}` | `queue.name`: `cmd`, `state` | SDK 큐가 버린 메시지의 누적 수 |

### power-manager

| 메트릭 | 계측기 | 값 | attribute | 뜻과 내는 법 |
| --- | --- | --- | --- | --- |
| `robot.loop` | Counter | 정수, `{cycle}` | | 메인 루프가 돈 횟수 |
| `robot.battery.soc` | ObservableGauge | 실수 0–1, `1` | | 잔량. BMS가 값을 줄 때만 낸다. 분리되면 내지 않는다 |
| `robot.battery.connected` | ObservableGauge | 0, 1 | | 0이면 배터리가 분리됐다 |
| `robot.battery.power_state` | ObservableGauge | 0, 1 | `battery.power_state`: `idle`, `discharging`, `charging`, `shutdown` | 상태마다 series 하나. 지금 상태만 1 |

### sensor-hub

| 메트릭 | 계측기 | 값 | attribute | 뜻과 내는 법 |
| --- | --- | --- | --- | --- |
| `robot.loop` | Counter | 정수, `{cycle}` | | 메인 루프가 돈 횟수 |
| `robot.sensor.state` | ObservableGauge | 0, 1 | `sensor.id`, `sensor.kind`, `sensor.state`: `ok`, `missing`, `error` | 센서마다 세 series. 지금 상태만 1 |

### fault-reporter

`robot.loop`(Counter)에 더해 에러 코드를 로그로 남긴다.

| 언제 | 레코드 |
| --- | --- |
| 폴트가 생길 때 | 아래 attribute, 등급은 그 코드의 등급 |
| 폴트가 이어지는 동안 | 5초마다 같은 레코드를 다시 |
| 폴트가 풀릴 때 | 같은 type과 key에 `diag.condition.active=false`, 등급 INFO |

| 필드 | 값 |
| --- | --- |
| SeverityNumber | ERROR(17)이면 로봇을 못 쓴다. WARN(13)이면 성능이 떨어졌다. INFO(9)는 해제다 |
| Body | 사람이 읽을 문장. Ward는 읽지 않는다 |
| `diag.condition.type` | `robot.fault` (문자열) |
| `diag.condition.key` | 에러 코드 (문자열, 예: `E1203`) |
| `diag.condition.active` | 해제 레코드에만 `false`. **반드시 bool**이다. 문자열 `"false"`는 해제가 아니다 |

반복과 해제를 둘 다 하는 이유는 이렇다.
- 해제 레코드가 오면 바로 닫힌다.
- 해제 레코드를 잃어도 반복이 멈추면, `fault-reporter`가 살아 있다는 증거가 있을 때 15초 뒤 닫힌다.
- `fault-reporter`가 재시작했으면 해결로 닫지 않는다.

### 노드의 Collector

| 하는 일 | 왜 |
| --- | --- |
| 모든 신호에 `host.name=robot-1`을 붙인다 | Ward는 이것으로 신호가 어느 로봇에서 왔는지 정한다 (`nodes.robot-1.match`) |
| hostmetrics로 `system.uptime`을 낸다 | Ward가 로봇의 부팅 시각을 알고, 재부팅 뒤에는 열린 알람을 `interrupted.reboot`로 닫는다 |
| Ward의 canary를 받아 Ward로 돌려보낸다 | Ward가 "이 로봇의 신호가 다 오고 있다"를 확인한다. 안 되면 로봇을 "관측 불가"로 보고, 침묵으로 판정하는 것을 보류한다 |

설정은 Ward 저장소의 `deploy/collector/collector.yaml`과 같다.

## 4. 시나리오와 Ward의 결론

| 시나리오 | 신호 | 알람 (규칙, 등급) | 끝나는 법 | 알람이 API에 보이기까지 |
| --- | --- | --- | --- | --- |
| 제어 노드가 아무것도 안 보냄 | `robot.loop` (robot-controller) | `controller.down`, error | 다시 보내고 16초 (재시작 직후 판정 유예 10초 포함) | 7초 |
| 제어 노드 루프가 멈춤 | `robot.loop` | `controller.stalled`, error | 다시 돌고 6초 | 7초 |
| 배터리 관리 프로그램만 죽음 | `robot.loop` (power-manager) | `power-manager.down`, error. 배터리 알람은 열지 않고 "모름" | 다시 보내고 16초 | 7초 |
| 로봇 자체가 안 보임 (전원, 네트워크) | 없음. Ward의 canary | `robot.unobservable`, error | 다시 보이고 15초 | 11초 |
| 비상정지 | `robot.estop.active` | `robot.estop`, error, source마다 | 풀리면 바로 | 3초 |
| 보드 폴트 | `robot.board.fault` | `robot.board_fault`, error, 보드마다 | 풀리면 바로 | 3초 |
| 관절 잠금 | `robot.joint_lock.locked` | `robot.joint_locked`, error | 풀리면 바로 | 3초 |
| 모터 이상 | `robot.motor.state` | `robot.motor_fault`, error, 모터마다 (단계 `disconnected`, `error`) | `ok`가 되면 바로 | 3초 |
| 모터가 10초 넘게 꺼짐 | `robot.motor.state` (`off`) | `robot.motor_off`, info | `ok`가 되면 바로 | 꺼지고 13초 |
| 모터 온도 | `robot.motor.temperature` | `robot.motor_temperature`: 65 넘게 10초면 warning, 80 넘게 10초면 error | 정상 10초 | 13초 |
| 관절 갱신 끊김 | `robot.joint.stale` | `robot.joint_stale`, error, 관절마다 | 갱신되면 바로 | 3초 |
| RT 사이클 느림 (900 Hz 미만 10초) | `robot.rt_cycle.count` | `robot.rt_cycle_slow`, warning | 정상 10초 | 17초 |
| RT 주기 지연 (1500 us 초과) | `robot.rt_cycle.period_max` | `robot.rt_cycle_late`, warning | 정상 10초 | 3초 |
| SDK 큐 drop | `robot.sdk.push_drops` | `robot.sdk_drops`, warning, 큐마다 | drop 없이 16초 | 3초 |
| 배터리 잔량 (0.2 미만, 0.1 미만) | `robot.battery.soc` | `robot.battery_low`: warning, error | 정상 30초 | 3초 |
| 배터리 분리 | `robot.battery.connected` | `robot.battery_disconnected`, error. 잔량은 "모름" | 붙으면 바로 | 3초 |
| 배터리 shutdown | `robot.battery.power_state` | `robot.battery_power_state`, error | 다른 상태가 되면 바로 | 3초 |
| 센서 이상 | `robot.sensor.state` | `robot.sensor_fault`, warning, 센서마다 | `ok`가 되면 바로 | 3초 |
| 에러 코드 (ERROR, WARN) | fault-reporter 로그 | `robot.fault_code` error, `robot.warning_code` warning, 코드마다 | 해제 레코드 | 1초 |
| 에러 코드의 해제를 잃음 | | | 마지막 반복 뒤 15초, `resolved.silence` | |
| fault-reporter가 재시작 | `robot.loop`의 시작 시각 | 열려 있던 알람은 `interrupted.source_restart` | 다시 남는 같은 코드는 새 알람 | |

"알람이 API에 보이기까지"는 시뮬레이션에서 일이 생긴 뒤 Ward API에 알람이 보이기까지
걸린 시간이다. export 2초와 Ward의 판정 지연(메트릭 3초, 로그 2초)을 포함한다.

정상일 때는 알람이 하나도 열리지 않는다 (시뮬레이션에서 6분). 프로그램 하나가 죽으면
그 프로그램의 `*.down` 하나만 열리고, 그 프로그램이 내던 신호에 걸린 규칙은 알람 대신
"모름"이 된다.

## 5. 인벤토리

`rules.yaml`의 `components`는 이 로봇에 있어야 할 것의 목록이다. 비상정지 source 셋,
보드 둘, 모터 넷, 센서 셋, 그리고 네 프로그램을 적었다. Ward는 이것들을 신호가 오기
전부터 보여 준다. 신호를 한 번도 내지 않은 것은 `unobserved`(관측 안 됨)로 보인다
(`docs/conditions.md` 8장). 대시보드는 `ComponentService`로 이 트리를 받아, 알람이
없을 때도 "모터 m1–m4 정상, 센서 imu 관측 안 됨"처럼 그릴 수 있다.

## 6. 내 로봇에 맞추기

- **이름:** 프로그램 이름(`service.name`)과 메트릭 이름을 실제 것으로 바꾼다. 규칙의 `source`와 `metric.name`도 같이 바꾼다.
- **임계값:** 모터 온도 65/80, RT 900 Hz와 1500 us, 배터리 0.2/0.1은 예다.
- **id 목록:** 모터, 관절, 보드, 센서, 큐의 실제 id로 `components`를 고친다.
- **확인 정책:** 이 샘플은 error만 운영자 확인이 필요하다(`ack: required`). warning과 info는 필요 없다.
- **노드 설정:** Ward 정적 설정에 로봇을 노드로 둔다.
  `nodes: {robot-1: {match: {host.name: robot-1}, collector: "<로봇 주소>:4317"}}`

신호를 보내는 일반 규약은 `docs/signals.md`에 있다. 규칙의 종류와 보는 법은 `docs/conditions.md`에 있다.
