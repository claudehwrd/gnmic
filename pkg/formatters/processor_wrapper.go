// © 2022 Nokia.
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package formatters

import (
	"log"
	"runtime"
	"time"

	"github.com/openconfig/gnmic/pkg/api/types"
)

// InstrumentedProcessor wraps an EventProcessor and collects performance metrics
type InstrumentedProcessor struct {
	processor     EventProcessor
	processorName string
	processorType string
	logger        *log.Logger
}

// NewInstrumentedProcessor creates a new instrumented wrapper around a processor
func NewInstrumentedProcessor(processor EventProcessor, name, processorType string, logger *log.Logger) *InstrumentedProcessor {
	return &InstrumentedProcessor{
		processor:     processor,
		processorName: name,
		processorType: processorType,
		logger:        logger,
	}
}

// Init passes through to the wrapped processor
func (ip *InstrumentedProcessor) Init(cfg interface{}, opts ...Option) error {
	return ip.processor.Init(cfg, opts...)
}

// Apply wraps the processor's Apply method with instrumentation
func (ip *InstrumentedProcessor) Apply(evs ...*EventMsg) []*EventMsg {
	// Track input events
	inputCount := len(evs)
	processorEventsInput.WithLabelValues(ip.processorName, ip.processorType).Add(float64(inputCount))

	// Capture memory stats before processing
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Measure duration
	start := time.Now()

	// Call the actual processor (wrapped in recovery to handle panics)
	var result []*EventMsg
	func() {
		defer func() {
			if r := recover(); r != nil {
				processorErrors.WithLabelValues(ip.processorName, ip.processorType).Inc()
				if ip.logger != nil {
					ip.logger.Printf("event processor '%s' (type=%s) panic: %v", ip.processorName, ip.processorType, r)
				}
				// Return input events unchanged on panic
				result = evs
			}
		}()
		result = ip.processor.Apply(evs...)
	}()

	// Record duration
	duration := time.Since(start).Seconds()
	processorDuration.WithLabelValues(ip.processorName, ip.processorType).Observe(duration)

	// Capture memory stats after processing
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate memory allocated during this call
	// TotalAlloc is cumulative, so we calculate the delta
	memAllocated := memAfter.TotalAlloc - memBefore.TotalAlloc
	if memAllocated > 0 {
		processorMemoryAllocated.WithLabelValues(ip.processorName, ip.processorType).Observe(float64(memAllocated))
	}

	// Track output events
	outputCount := len(result)
	processorEventsOutput.WithLabelValues(ip.processorName, ip.processorType).Add(float64(outputCount))

	return result
}

// WithTargets passes through to the wrapped processor
func (ip *InstrumentedProcessor) WithTargets(tcs map[string]*types.TargetConfig) {
	ip.processor.WithTargets(tcs)
}

// WithLogger passes through to the wrapped processor and stores for panic recovery
func (ip *InstrumentedProcessor) WithLogger(l *log.Logger) {
	ip.logger = l
	ip.processor.WithLogger(l)
}

// WithActions passes through to the wrapped processor
func (ip *InstrumentedProcessor) WithActions(act map[string]map[string]interface{}) {
	ip.processor.WithActions(act)
}

// WithProcessors passes through to the wrapped processor
func (ip *InstrumentedProcessor) WithProcessors(procs map[string]map[string]any) {
	ip.processor.WithProcessors(procs)
}
