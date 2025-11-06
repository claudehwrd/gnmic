// © 2022 Nokia.
//
// This code is a Contribution to the gNMIc project ("Work") made under the Google Software Grant and Corporate Contributor License Agreement ("CLA") and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia's intellectual property are granted for any other purpose.
// This code is provided on an "as is" basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package formatters

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Processor performance metrics
var (
	// processorDuration tracks how long each processor takes to process events
	processorDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gnmic",
		Subsystem: "event_processor",
		Name:      "duration_seconds",
		Help:      "Duration of event processor Apply() calls in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 20), // 1µs to ~1s
	}, []string{"processor_name", "processor_type"})

	// processorEventsInput tracks the number of events entering each processor
	processorEventsInput = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gnmic",
		Subsystem: "event_processor",
		Name:      "events_input_total",
		Help:      "Total number of events entering the processor",
	}, []string{"processor_name", "processor_type"})

	// processorEventsOutput tracks the number of events exiting each processor
	processorEventsOutput = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gnmic",
		Subsystem: "event_processor",
		Name:      "events_output_total",
		Help:      "Total number of events output by the processor",
	}, []string{"processor_name", "processor_type"})

	// processorMemoryAllocated tracks memory allocated during processing
	processorMemoryAllocated = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gnmic",
		Subsystem: "event_processor",
		Name:      "memory_allocated_bytes",
		Help:      "Memory allocated during event processor Apply() calls in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 15), // 1KB to ~16MB
	}, []string{"processor_name", "processor_type"})

	// processorErrors tracks errors during processing
	processorErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gnmic",
		Subsystem: "event_processor",
		Name:      "errors_total",
		Help:      "Total number of errors during event processing",
	}, []string{"processor_name", "processor_type"})
)

// RegisterProcessorMetrics registers processor metrics with the provided registry
func RegisterProcessorMetrics(reg *prometheus.Registry) error {
	if err := reg.Register(processorDuration); err != nil {
		return err
	}
	if err := reg.Register(processorEventsInput); err != nil {
		return err
	}
	if err := reg.Register(processorEventsOutput); err != nil {
		return err
	}
	if err := reg.Register(processorMemoryAllocated); err != nil {
		return err
	}
	if err := reg.Register(processorErrors); err != nil {
		return err
	}
	return nil
}
