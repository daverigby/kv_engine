/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <statistics/prometheus_collector.h>

void PrometheusStatCollector::addStat(const cb::stats::StatDef& spec,
                                      const HistogramData& hist,
                                      const Labels& additionalLabels) {
    prometheus::ClientMetric metric;

    metric.histogram.sample_count = hist.sampleCount;
    metric.histogram.sample_sum = hist.sampleSum;

    uint64_t cumulativeCount = 0;

    for (const auto& bucket : hist.buckets) {
        cumulativeCount += bucket.count;
        auto normalised = spec.unit.toBaseUnit(bucket.upperBound);
        metric.histogram.bucket.push_back(
                {cumulativeCount, gsl::narrow_cast<double>(normalised)});
    }

    addClientMetric(spec,
                    additionalLabels,
                    std::move(metric),
                    prometheus::MetricType::Histogram);
}

void PrometheusStatCollector::addStat(const cb::stats::StatDef& spec,
                                      double v,
                                      const Labels& additionalLabels) {
    prometheus::ClientMetric metric;
    metric.untyped.value = spec.unit.toBaseUnit(v);
    addClientMetric(spec,
                    additionalLabels,
                    std::move(metric),
                    prometheus::MetricType::Untyped);
}

void PrometheusStatCollector::addClientMetric(
        const cb::stats::StatDef& key,
        const Labels& additionalLabels,
        prometheus::ClientMetric metric,
        prometheus::MetricType metricType) {
    auto name = key.metricFamily.empty() ? key.uniqueKey : key.metricFamily;

    auto [itr, inserted] = metricFamilies.try_emplace(
            std::string(name), prometheus::MetricFamily());
    auto& metricFamily = itr->second;
    if (inserted) {
        metricFamily.name = prefix + std::string(name);
        metricFamily.type = metricType;
    }

    metric.label.reserve(key.labels.size() + additionalLabels.size());

    // start with the labels passed down from the collector
    // these are labels common to many stats, and have runtime values like
    // {{"bucket", "someBucketName"},{"scope", "scopeName"}}
    for (const auto& [label, value] : additionalLabels) {
        metric.label.push_back({std::string(label), std::string(value)});
    }

    // Set the labels specific to this stat. These are specified in
    // stats.def.h.
    // These are labels intrinsic to the specific stat - e.g.,
    // "cmd_set" and "cmd_get"
    // could be exposed as
    //  cmd_count{operation="set"}
    //  cmd_count{operation="get"}
    // These labels are not expected to be overriden with different values.
    for (const auto& [label, value] : key.labels) {
        // It is not currently expected that any of these labels will also have
        // a value set in additionalLabels (from the collector or addStat call).
        // If this changes, the stat should _not_ be added here, to avoid
        // duplicate labels being exposed to Prometheus.
        Expects(!additionalLabels.count(label));
        metric.label.push_back({std::string(label), std::string(value)});
    }
    metricFamily.metric.emplace_back(std::move(metric));
}