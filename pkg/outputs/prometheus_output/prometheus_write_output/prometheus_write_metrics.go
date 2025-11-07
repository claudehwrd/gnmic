// © 2023 Nokia.
//
// This code is a Contribution to the gNMIc project (“Work”) made under the Google Software Grant and Corporate Contributor License Agreement (“CLA”) and governed by the Apache License 2.0.
// No other rights or licenses in or to any of Nokia’s intellectual property are granted for any other purpose.
// This code is provided on an “as is” basis without any warranties of any kind.
//
// SPDX-License-Identifier: Apache-2.0

package prometheus_write_output

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "gnmic"
	subsystem = "prometheus_write_output"
)

var registerMetricsOnce sync.Once

var prometheusWriteNumberOfSentMsgs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "number_of_prometheus_write_msgs_sent_success_total",
	Help:      "Number of msgs successfully sent by gnmic prometheus_write output",
}, []string{"name"})

var prometheusWriteNumberOfFailSendMsgs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "number_of_prometheus_write_msgs_sent_fail_total",
	Help:      "Number of failed msgs sent by gnmic prometheus_write output",
}, []string{"name", "reason"})

var prometheusWriteSendDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "msg_send_duration_ns",
	Help:      "gnmic prometheus_write output send duration in ns",
}, []string{"name"})

var prometheusWriteNumberOfSentMetadataMsgs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "number_of_prometheus_write_metadata_msgs_sent_success_total",
	Help:      "Number of metadata msgs successfully sent by gnmic prometheus_write output",
}, []string{"name"})

var prometheusWriteNumberOfFailSendMetadataMsgs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "number_of_prometheus_write_metadata_msgs_sent_fail_total",
	Help:      "Number of failed metadata msgs sent by gnmic prometheus_write output",
}, []string{"name", "reason"})

var prometheusWriteMetadataSendDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "metadata_msg_send_duration_ns",
	Help:      "gnmic prometheus_write output metadata send duration in ns",
}, []string{"name"})

var prometheusWriteNumberOfDroppedMsgs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "number_of_prometheus_write_msgs_dropped_total",
	Help:      "Number of msgs dropped due to buffer full by gnmic prometheus_write output",
}, []string{"name", "reason"})

var prometheusWriteBufferUtilization = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "buffer_utilization",
	Help:      "Current utilization of the time series buffer (0-1)",
}, []string{"name"})

func initMetrics(name string) {
	// data msgs metrics
	prometheusWriteNumberOfSentMsgs.WithLabelValues(name).Add(0)
	prometheusWriteNumberOfFailSendMsgs.WithLabelValues(name, "").Add(0)
	prometheusWriteSendDuration.WithLabelValues(name).Set(0)
	// metadata msgs metrics
	prometheusWriteNumberOfSentMetadataMsgs.WithLabelValues(name).Add(0)
	prometheusWriteNumberOfFailSendMetadataMsgs.WithLabelValues(name, "").Add(0)
	prometheusWriteMetadataSendDuration.WithLabelValues(name).Set(0)
	// buffer metrics
	prometheusWriteNumberOfDroppedMsgs.WithLabelValues(name, "").Add(0)
	prometheusWriteBufferUtilization.WithLabelValues(name).Set(0)
}

func (p *promWriteOutput) registerMetrics() error {
	if p.reg == nil {
		return nil
	}
	var err error
	registerMetricsOnce.Do(func() {
		if err = p.reg.Register(prometheusWriteNumberOfSentMsgs); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteNumberOfFailSendMsgs); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteSendDuration); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteNumberOfSentMetadataMsgs); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteNumberOfFailSendMetadataMsgs); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteMetadataSendDuration); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteNumberOfDroppedMsgs); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
		if err = p.reg.Register(prometheusWriteBufferUtilization); err != nil {
			p.logger.Printf("failed to register metric: %v", err)
			return
		}
	})
	initMetrics(p.cfg.Name)
	return err
}
