module github.com/lesomnus/ward/collector/wardexporter

go 1.27.0

replace github.com/lesomnus/ward => ../..

require (
	github.com/lesomnus/otx v0.0.0-20261013182912-d2af55df512e
	github.com/lesomnus/ward v0.0.0-20261109102609-3026679ff937
	github.com/lesomnus/xli/cfg v0.1.2
	go.opentelemetry.io/collector/component v1.71.0
	go.opentelemetry.io/collector/component/componenttest v0.164.0
	go.opentelemetry.io/collector/consumer v1.71.0
	go.opentelemetry.io/collector/exporter v1.71.0
	go.opentelemetry.io/collector/pdata v1.71.0
	go.uber.org/zap v1.28.0
	go.uber.org/zap/exp v0.3.0
	google.golang.org/grpc v1.85.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	ariga.io/atlas v1.3.0 // indirect
	cel.dev/cel-go v0.32.0 // indirect
	cel.dev/expr v0.25.3 // indirect
	filippo.io/age v1.3.2 // indirect
	filippo.io/hpke v0.4.0 // indirect
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/apparentlymart/go-textseg/v17 v17.0.1 // indirect
	github.com/bmatcuk/doublestar v1.3.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/inflect v1.0.0 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/hcl/v2 v2.24.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/lesomnus/flob v0.0.0-20261010120939-21f89e60609e // indirect
	github.com/lesomnus/mkot v0.0.0-20261013182918-46d8b05a79b3 // indirect
	github.com/lesomnus/mkot/mkotx v0.0.0-20261013182918-46d8b05a79b3 // indirect
	github.com/lesomnus/mkot/pretty v0.0.0-20261013182918-46d8b05a79b3 // indirect
	github.com/lesomnus/otx/otxgrpc v0.0.0-20261013182912-d2af55df512e // indirect
	github.com/lesomnus/payday v0.0.0-20261106035225-688547d9aad7 // indirect
	github.com/lesomnus/protobuf-patch v0.0.0-20260803175157-e1b7a0c2804f // indirect
	github.com/lesomnus/pwr v0.0.0-20261015171245-2ec220d490d1 // indirect
	github.com/lesomnus/pwr/cel v0.0.0-20261015172325-de464a5ef1fa // indirect
	github.com/lesomnus/xli v0.0.0-20261009121920-4ce270c7448c // indirect
	github.com/lesomnus/z v0.0.0-20260531102454-3f1853bb4278 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.23 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/ncruces/go-sqlite3 v0.35.3 // indirect
	github.com/ncruces/go-sqlite3-wasm/v3 v3.2.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/protobuf-orm/ent v0.0.0-20261024205432-0dd7dadc81bc // indirect
	github.com/protobuf-orm/protobuf-orm v0.0.0-20261017212716-3430237a8656 // indirect
	github.com/protobuf-orm/protoc-gen-orm-ent/runtime v0.0.0-20261022151422-5b2cd8279e07 // indirect
	github.com/zclconf/go-cty v1.19.0 // indirect
	github.com/zclconf/go-cty-yaml v1.2.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/featuregate v1.71.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.165.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.71.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelslog v0.21.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.70.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/term v0.46.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260928230214-8a89bd6388cc // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
