/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

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

/// What one walk over the rows produces, before it is arranged into search_answers.
struct matched_rows {
    std::vector<cql3::raw_value> scores;
    std::vector<bool> dropped;
    std::vector<sstring> documents;
};

/// Walks the rows of a base-table read, lining an external search's response up with them.
///
/// It has to visit exactly the rows result_set_builder::visitor emits, in the same order, since what
/// it produces is handed back out by position.  It does because it is the same walk: the same merged
/// result read with the same slice.  The one thing that does not follow from that is repeated here,
/// that visitor's rule for a partition holding nothing but a static row.
class result_matcher {
    const schema& _schema;
    const selection::selection& _selection;

    // The relevance, matched by primary key.  The cursor only moves forward: base-table results are
    // merged in the index's primary-key order, so a row can only ever match at or after the current
    // position, and an entry stepped over is a key the index still knows about but that is no longer
    // in the base table.
    const vector_search::vector_store_client::primary_keys& _results;
    const bool _match_scores;
    size_t _next_result = 0;

    // The text a fragment is to be generated from.  The index stores none of it, which is why it has
    // to be read from the rows and sent back.
    const column_definition* _document_column;
    const size_t _document_index;

    matched_rows& _matched;

    partition_key _partition_key = partition_key::make_empty();
    clustering_key_prefix _clustering_key = clustering_key_prefix::make_empty();
    uint64_t _row_count = 0;

    void match_score() {
        while (_next_result < _results.size()) {
            const auto& result = _results[_next_result++];

            if (!result.partition.key().equal(_schema, _partition_key)) {
                continue;
            }
            if (_schema.clustering_key_size() > 0 && !result.clustering.equal(_schema, _clustering_key)) {
                continue;
            }

            // Vector Store cannot return Inf over its JSON API, and should not return NaN (null in
            // JSON), but if it does then the row has no relevance to report and is dropped.
            if (!std::isfinite(result.similarity)) {
                break;
            }

            _matched.scores.push_back(cql3::raw_value::make_value(float_type->decompose(result.similarity)));
            _matched.dropped.push_back(false);
            return;
        }

        _matched.scores.push_back(cql3::raw_value::make_null());
        _matched.dropped.push_back(true);
    }

    void collect_document(const query::result_row_view& static_row, const query::result_row_view* row) {
        // A row whose text is null or absent still needs a document.  The index answers an empty one
        // with no fragment, which is what such a row should report anyway.
        auto value = _document_column->is_clustering_key() ? clustering_key_component()
                                                           : expr::get_non_pk_values(_selection, static_row, row)[_document_index];
        _matched.documents.push_back(value ? value_cast<sstring>(_document_column->type->deserialize(managed_bytes_view(*value))) : sstring());
    }

    std::optional<managed_bytes> clustering_key_component() const {
        auto components = _clustering_key.explode(_schema);
        if (components.size() <= _document_column->component_index()) {
            return std::nullopt;
        }
        return managed_bytes(components[_document_column->component_index()]);
    }

    void visit(const query::result_row_view& static_row, const query::result_row_view* row) {
        if (_match_scores) {
            match_score();
        }
        if (_document_column) {
            collect_document(static_row, row);
        }
    }

public:
    result_matcher(const schema& schema, const selection::selection& selection,
            const vector_search::vector_store_client::primary_keys& results, bool match_scores, const column_definition* document_column,
            matched_rows& matched)
        : _schema(schema)
        , _selection(selection)
        , _results(results)
        , _match_scores(match_scores)
        , _document_column(document_column)
        , _document_index(document_column ? column_index_in(selection, *document_column) : 0)
        , _matched(matched) {
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
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys& results,
        std::optional<size_t> score_slot, const column_definition* document_column) {
    auto matched = matched_rows{};
    query::result_view::consume(rows, slice, result_matcher(schema, selection, results, bool(score_slot), document_column, matched));

    auto answers = search_answers{.dropped = std::move(matched.dropped), .documents = std::move(matched.documents)};
    if (score_slot) {
        answers.slots.emplace_back(*score_slot, std::move(matched.scores));
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
