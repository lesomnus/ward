package def

// Holds reports whether a value meets the predicate. v is what an attribute
// holds -- string, int64, float64, bool or a list -- and present says whether
// the attribute was there at all. Equality is by type as well as value: the
// string "500" is not the int 500.
func (p Pred) Holds(v any, present bool) bool {
	switch p.Op {
	case "exists":
		return present == p.Value.(bool)
	case "eq":
		return present && equal(p.Value, v)
	case "in":
		if !present {
			return false
		}
		for _, x := range p.Value.([]any) {
			if equal(x, v) {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		if !present {
			return false
		}
		a, ok := number(v)
		b, ok2 := number(p.Value)
		if !ok || !ok2 {
			return false
		}
		return compare(p.Op, a, b)
	}
	return false
}

// HoldsSeverity is Holds for a severity predicate, whose operands are names,
// against an OTel SeverityNumber.
func (p Pred) HoldsSeverity(n int32) bool {
	b := SeverityBucket(n)
	if b == 0 {
		return false
	}
	switch p.Op {
	case "eq":
		return b == LogSeverity(p.Value.(string))
	case "in":
		for _, x := range p.Value.([]any) {
			if b == LogSeverity(x.(string)) {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		return compare(p.Op, float64(b), float64(LogSeverity(p.Value.(string))))
	}
	return false
}

// Lookup answers an attribute's value and whether it was there.
type Lookup func(key string) (any, bool)

// Record is what a Match reads of a log record.
type Record struct {
	SeverityNumber int32
	EventName      string
	Attrs          Lookup
	Resource       Lookup
}

// Holds reports whether every condition of m holds for r.
func (m *Match) Holds(r Record) bool {
	if m == nil {
		return true
	}
	if m.Severity != nil && !m.Severity.HoldsSeverity(r.SeverityNumber) {
		return false
	}
	if m.EventName != nil && !m.EventName.Holds(r.EventName, r.EventName != "") {
		return false
	}
	for k, p := range m.Attrs {
		v, ok := r.Attrs(k)
		if !p.Holds(v, ok) {
			return false
		}
	}
	for k, p := range m.Resource {
		v, ok := r.Resource(k)
		if !p.Holds(v, ok) {
			return false
		}
	}
	return true
}

// HoldsAttrs reports whether every predicate holds for attrs.
func HoldsAttrs(ps map[string]Pred, attrs Lookup) bool {
	for k, p := range ps {
		v, ok := attrs(k)
		if !p.Holds(v, ok) {
			return false
		}
	}
	return true
}

func equal(a, b any) bool {
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case int64:
		y, ok := b.(int64)
		return ok && x == y
	case float64:
		y, ok := b.(float64)
		return ok && x == y
	}
	return false
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func compare(op string, a, b float64) bool {
	switch op {
	case "gt":
		return a > b
	case "gte":
		return a >= b
	case "lt":
		return a < b
	case "lte":
		return a <= b
	}
	return false
}
