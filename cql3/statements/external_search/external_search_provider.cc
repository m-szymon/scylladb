/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>
#include <map>
#include <ranges>

#include "cql3/expr/expr-utils.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/log.hh"

#include <seastar/core/on_internal_error.hh>

#include <algorithm>

namespace cql3::statements {

namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger slogger("external_search");

/// A row's primary key, as a value that can be looked up.
///
/// The serialized compound is canonical for a schema, so two keys are equal exactly when their
/// bytes are - which is what lets an answer be found by key rather than walked to.
using row_key = std::pair<bytes, bytes>;

row_key key_of(const partition_key& partition, const clustering_key_prefix& clustering) {
    return {to_bytes(partition.representation()), to_bytes(clustering.representation())};
}

/// What one search said about one row.
struct answer {
    float score;
    int32_t rank;
};

/// One search's answer, arranged for lookup by key, together with where it is to be delivered.
struct indexed_answer {
    const search_answer_request& request;
    std::map<row_key, answer> by_key;
    size_t document_index = 0;

    std::vector<cql3::raw_value> scores;
    std::vector<cql3::raw_value> ranks;
    std::vector<sstring> documents;
};

/// Where in the values get_non_pk_values() hands back the given column sits.  That vector is aligned
/// with the selection's columns, which is also the order the slice asks the replicas for them in, so
/// the three agree by construction.
size_t column_index_in(const selection::selection& selection, const column_definition& column) {
    const auto& columns = selection.get_columns();
    auto it = std::ranges::find(columns, &column);
    if (it == columns.end()) {
        on_internal_error(slogger, seastar::format("column {} is wanted from every row but was not asked for", column.name_as_text()));
    }
    return std::distance(columns.begin(), it);
}

/// Arranges one search's answer for lookup.  An index can name a key more than once; the first
/// entry wins, since that is the one it ranked highest.
std::map<row_key, answer> index_by_key(const schema& schema, const vector_search::vector_store_client::primary_keys& results) {
    auto by_key = std::map<row_key, answer>{};
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& result = results[i];
        // Vector Store cannot return Inf over its JSON API, and should not return NaN (null in
        // JSON), but if it does then it has nothing to say about the row and this is not an answer.
        if (!std::isfinite(result.similarity)) {
            continue;
        }
        auto clustering = schema.clustering_key_size() > 0 ? result.clustering : clustering_key_prefix::make_empty();
        // The rank is where the entry sits in the answer, counted from 1 - a position in what the
        // index answered, not in the result set, so a key whose row is gone takes its rank with it.
        by_key.emplace(key_of(result.partition.key(), clustering), answer{result.similarity, static_cast<int32_t>(i + 1)});
    }
    return by_key;
}

/// Walks the rows of a base-table read, looking each one up in every search's answer.
///
/// It has to visit exactly the rows result_set_builder::visitor emits, in the same order, since what
/// it produces is handed back out by position.  It does because it is the same walk: the same merged
/// result read with the same slice.  The one thing that does not follow from that is repeated here,
/// that visitor's rule for a partition holding nothing but a static row.
class result_matcher {
    const schema& _schema;
    const selection::selection& _selection;
    std::vector<indexed_answer>& _answers;
    std::vector<bool>& _dropped;

    partition_key _partition_key = partition_key::make_empty();
    clustering_key_prefix _clustering_key = clustering_key_prefix::make_empty();
    uint64_t _row_count = 0;

    void visit(const query::result_row_view& static_row, const query::result_row_view* row) {
        const auto key = key_of(_partition_key, _clustering_key);
        bool looked_up_by_any = false;
        bool found_by_any = false;

        for (auto& answers : _answers) {
            // A search asked only for an excerpt is not looked up: an excerpt is generated from the
            // row rather than found in the answer, so the primary key was never fetched for it, and
            // whether that search also found the row is not something the query asked about.
            const bool looked_up = answers.request.score_slot || answers.request.rank_slot;
            looked_up_by_any |= looked_up;
            auto it = looked_up ? answers.by_key.find(key) : answers.by_key.end();
            found_by_any |= it != answers.by_key.end();

            if (answers.request.score_slot) {
                answers.scores.push_back(it != answers.by_key.end()
                                ? cql3::raw_value::make_value(float_type->decompose(it->second.score))
                                : cql3::raw_value::make_null());
            }
            if (answers.request.rank_slot) {
                answers.ranks.push_back(it != answers.by_key.end()
                                ? cql3::raw_value::make_value(int32_type->decompose(it->second.rank))
                                : cql3::raw_value::make_null());
            }
            if (answers.request.document_column) {
                collect_document(answers, static_row, row);
            }
        }

        // Every index that named this key has since lost the row, or never named it at all: there is
        // nothing to report about it, so it is not part of this answer.  A query that asks no search
        // what it thought of a row drops none of them.
        _dropped.push_back(looked_up_by_any && !found_by_any);
    }

    void collect_document(indexed_answer& answers, const query::result_row_view& static_row, const query::result_row_view* row) {
        const auto& column = *answers.request.document_column;
        // A row whose text is null or absent still needs a document.  The index answers an empty one
        // with no fragment, which is what such a row should report anyway.
        auto value = column.is_clustering_key() ? clustering_key_component(column)
                                                : expr::get_non_pk_values(_selection, static_row, row)[answers.document_index];
        answers.documents.push_back(value ? value_cast<sstring>(column.type->deserialize(managed_bytes_view(*value))) : sstring());
    }

    std::optional<managed_bytes> clustering_key_component(const column_definition& column) const {
        auto components = _clustering_key.explode(_schema);
        if (components.size() <= column.component_index()) {
            return std::nullopt;
        }
        return managed_bytes(components[column.component_index()]);
    }

public:
    result_matcher(const schema& schema, const selection::selection& selection, std::vector<indexed_answer>& answers,
            std::vector<bool>& dropped)
        : _schema(schema)
        , _selection(selection)
        , _answers(answers)
        , _dropped(dropped) {
    }

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _partition_key = key;
        _clustering_key = clustering_key_prefix::make_empty();
        _row_count = row_count;
    }

    void accept_new_partition(uint64_t row_count) {
        _clustering_key = clustering_key_prefix::make_empty();
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        _clustering_key = key;
        accept_new_row(static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        visit(static_row, &row);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0) {
            // The builder emits one row for a partition that holds only a static row, so there is an
            // answer to line up with it too.  It reaches here with the clustering key the builder
            // gives such a row: empty, since no row of this partition has set one.
            visit(static_row, nullptr);
        }
    }
};

} // anonymous namespace

search_answers match_search_results(const query::result& rows, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, std::span<const search_answer_request> requests) {
    auto indexed = std::vector<indexed_answer>{};
    indexed.reserve(requests.size());
    for (const auto& request : requests) {
        indexed.push_back(indexed_answer{
                .request = request,
                .by_key = index_by_key(schema, request.results),
                .document_index = request.document_column ? column_index_in(selection, *request.document_column) : 0,
        });
    }

    auto answers = search_answers{};
    query::result_view::consume(rows, slice, result_matcher(schema, selection, indexed, answers.dropped));

    answers.documents.reserve(indexed.size());
    for (auto& one : indexed) {
        if (one.request.score_slot) {
            answers.slots.emplace_back(*one.request.score_slot, std::move(one.scores));
        }
        if (one.request.rank_slot) {
            answers.slots.emplace_back(*one.request.rank_slot, std::move(one.ranks));
        }
        answers.documents.push_back(std::move(one.documents));
    }
    return answers;
}

external_search_provider::external_search_provider(std::vector<std::pair<size_t, std::vector<cql3::raw_value>>> slots, std::vector<bool> dropped)
    : _slots(std::move(slots))
    , _dropped(std::move(dropped)) {
}

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries) const {
    // Advanced for every row offered, including one that is dropped: the values were computed for
    // the same rows, in the same order, so the position has to move with them.
    const auto row = _next_row++;

    if (row < _dropped.size() && _dropped[row]) {
        return false;
    }

    for (const auto& [slot, values] : _slots) {
        // Nothing clears a slot between rows, so every slot is written for every row - with an
        // explicit null where there is no value, which keeps the row and leaves the value absent.
        temporaries[slot] = row < values.size() ? values[row] : cql3::raw_value::make_null();
    }
    return true;
}

} // namespace cql3::statements
