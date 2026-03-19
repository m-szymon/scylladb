// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <map>

#include "expression.hh"

#include "bytes.hh"

namespace cql3 {

class query_options;
class raw_value;

}

namespace cql3::expr {

// Per-element timestamps and TTLs for a collection cell (populated when a WRITETIME(m[key]) or TTL(m[key]) is in the query).
// Keys are the raw serialized map keys as stored in the mutation cells.
struct collection_cell_metadata {
    std::map<bytes, api::timestamp_type> timestamps;  // key bytes -> per-element timestamp
    std::map<bytes, int32_t> ttls;                    // key bytes -> remaining TTL in seconds (-1 if no TTL)
};

// Input data needed to evaluate an expression. Individual members can be
// null if not applicable (e.g. evaluating outside a row context)
struct evaluation_inputs {
    std::span<const bytes> partition_key;
    std::span<const bytes> clustering_key;
    std::span<const managed_bytes_opt> static_and_regular_columns; // indexes match `selection` member
    const cql3::selection::selection* selection = nullptr;
    const query_options* options = nullptr;
    std::span<const api::timestamp_type> static_and_regular_timestamps;  // indexes match `selection` member
    std::span<const int32_t> static_and_regular_ttls;  // indexes match `selection` member
    std::span<const cql3::raw_value> temporaries; // indexes match temporary::index
    std::span<const collection_cell_metadata> collection_element_metadata; // indexes match `selection` member
};

// Takes a prepared expression and calculates its value.
// Evaluates bound values, calls functions and returns just the bytes and type.
cql3::raw_value evaluate(const expression& e, const evaluation_inputs&);

cql3::raw_value evaluate(const expression& e, const query_options&);


}
