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

#pragma once

#include "collector.h"
#include "definitions.h"
#include <prometheus/client_metric.h>
#include <prometheus/metric_family.h>

class PrometheusStatCollector : public StatCollector {
public:
    PrometheusStatCollector(std::string prefix = "kv_")
        : prefix(std::move(prefix)) {
    }

    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) override {
        // silently discard text stats (for now). Prometheus can't expose them
    }

    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) override {
        addStat(k, double(v), labels);
    }

    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) override;

    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) override;

    auto& getCollectedStats() {
        return metricFamilies;
    }

protected:
    void addClientMetric(const cb::stats::StatDef& key,
                         const Labels& additionalLabels,
                         prometheus::ClientMetric metric,
                         prometheus::MetricType metricType =
                                 prometheus::MetricType::Untyped);

    const std::string prefix;

    std::unordered_map<std::string, prometheus::MetricFamily> metricFamilies{};
};
