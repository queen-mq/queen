package feedrt

import (
	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// Compiled is a chain on a feed, compiled (Stream._compileFeed): the steps in
// chain order, the where conditions the broker filters, the sink, and the
// chain hash (configHashOf(shape).slice(0, 16)).
type Compiled struct {
	Source    Source
	Steps     []Step
	Pushdown  []feed.Condition
	Sink      *SinkSpec
	ChainHash string
}

// Source is Stream.fromFeed's options.
type Source struct {
	Queues       []string
	Partitions   []string
	From         interface{}
	Since        interface{}
	FollowerURLs []string
	Kinds        []string
	MaxRecords   int
	MaxBytes     int
}

// Step is one step of the plan: stateless operators ("ops"), or a stateful
// step ("window", "pattern", "collect", "table").
type Step struct {
	Kind    string
	Name    string
	Ops     []Op
	Window  *WindowSpec
	Reducer *ReducerSpec
	Pattern *PatternSpec
	Collect *CollectSpec
	Table   *TableSpec
	// Config is the step's config as JS writes it (the window's, the
	// pattern's, ...): the stream's description says it (about.go).
	Config jsv.Ordered
	// ReducerConfig is a window's reduce/aggregate: { kind, ...config }.
	ReducerConfig jsv.Ordered
}

// Op is a stateless operator: map, filter, flatMap, keyBy, or a where
// after a stateful step (evaluated here, on the envelope's message).
type Op struct {
	Kind    string
	Map     func(msg, ctx interface{}) (interface{}, error)
	Filter  func(msg, ctx interface{}) (bool, error)
	FlatMap func(msg, ctx interface{}) ([]interface{}, error)
	KeyBy   func(msg interface{}) (string, error)
	Where   *feed.Condition
}

// TimeSpec is a step's clock options (stepTimeOptions / feedTimeOptions).
type TimeSpec struct {
	// EventTime is eventTime(msg): a time.Time, epoch ms or ISO string (nil: none).
	EventTime    func(msg interface{}) (interface{}, error)
	EventTimeOff bool
	LatenessMs   float64
	OnLate       string
	// IdleMs is idle in milliseconds (nil: none).
	IdleMs *float64
}

// WindowSpec is a window step's shape.
type WindowSpec struct {
	Kind            string // tumbling | sliding | cron | session
	WindowMs        float64
	SlideMs         float64
	SizeMs          float64
	WindowsPerEvent float64
	GapMs           float64
	Every           string
	GraceMs         float64
	Time            TimeSpec
}

// ReducerSpec is the reduce or aggregate after a window.
type ReducerSpec struct {
	Fn func(acc, value interface{}) (interface{}, error)
	// Initial is the fold's start (Undefined: none, as reduce(fn) in JS).
	Initial     interface{}
	IsAggregate bool
}

// PatternSpec is .pattern() or .join() of two sides.
type PatternSpec struct {
	Kind      string // pattern | join
	WithinMs  float64
	Key       func(msg, value interface{}) (interface{}, error)
	Keep      func(msg, value interface{}) (interface{}, error)
	OnTimeout func(msg interface{}, side string) (interface{}, error)
	OnRepeat  string
	Start     func(msg interface{}) (bool, error)
	Then      func(msg interface{}) (bool, error)
	OnMatch   func(start, then interface{}) (interface{}, error)
	Left      func(msg interface{}) (bool, error)
	Right     func(msg interface{}) (bool, error)
	Select    func(left, right interface{}) (interface{}, error)
	Time      TimeSpec
}

// Part is one named part of a join of many sides or of a gather.
type Part struct {
	Name string
	Pred func(msg, value interface{}) (bool, error)
}

// CollectSpec is .join({sides}) or .gather().
type CollectSpec struct {
	Kind       string // join | gather
	WithinMs   float64
	Key        func(msg, value interface{}) (interface{}, error)
	Keep       func(msg, value interface{}) (interface{}, error)
	OnTimeout  func(got, missing interface{}, key string) (interface{}, error)
	OnRepeat   string
	Parts      []Part
	Count      func(msg, value interface{}) (interface{}, error)
	OnComplete func(got interface{}, key string) (interface{}, error)
	Remember   bool
	Time       TimeSpec
}

// TableSpec is .joinTable().
type TableSpec struct {
	Queues []string
	On     func(msg, value interface{}) (interface{}, error)
	Key    func(row interface{}) (interface{}, error)
	Value  func(row interface{}) (interface{}, error)
	// Select gets the row (nil when there is none) and the message.
	Select  func(value, row, msg interface{}) (interface{}, error)
	Missing string
	Initial string
}

// SinkSpec is the end of the chain: .to() or .foreach().
type SinkSpec struct {
	Kind    string // sink | foreach
	Foreach func(value interface{}, ctx interface{}) error
	Queues  []string
	// Route answers a queue name, a Target, a list of those, or nil (nowhere).
	Route func(value, msg interface{}) (interface{}, error)
	// Partition is nil, a fixed partition, or a function of the value.
	Partition     string
	PartitionFn   func(value interface{}) (interface{}, error)
	HasPartition  bool
	PartitionFunc bool
}

// Target is where one record goes: a queue, and its partition ("" = the default rule).
type Target struct {
	Queue     string
	Partition string
}
