/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_select_statement.hh"

#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/vector_index.hh"
#include "types/vector.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>
#include <map>
#include <ranges>

namespace cql3::statements {

static logging::logger ess_log("external_search_select");

namespace {

using primary_keys = vector_search::vector_store_client::primary_keys;

bool is_hybrid(const std::vector<search_source>& sources) {
    return sources.size() > 1;
}

bool is_ann_only(const std::vector<search_source>& sources) {
    return !is_hybrid(sources) && sources.front().kind == external_search_kind::ann;
}

/// How a query is named in what it is told it may not do.  The three kinds word this the same way,
/// and two of them shipped saying it, so the wording is theirs.
std::string_view query_kind_name(const std::vector<search_source>& sources) {
    if (is_hybrid(sources)) {
        return "Hybrid search";
    }
    return sources.front().kind == external_search_kind::ann ? "Vector ANN" : "Full-text search";
}

/// The evaluated query value of one search, in the form its index takes.
struct query_value {
    /// The vector an ANN search is near to.
    std::vector<float> vector;
    /// The term a full-text search matches, kept as text because a second request needs it again.
    sstring term;
};

query_value evaluate_query_value(const search_source& source, const query_options& options) {
    auto value = expr::evaluate(source.query_value, options);
    if (value.is_null()) {
        throw exceptions::invalid_request_exception(source.kind == external_search_kind::ann
                        ? seastar::format("Unsupported null value for column {}", source.column->name_as_text())
                        : sstring("Full-text search query term must not be null"));
    }

    // Every call naming this search had to agree on what it asks; where a bind marker stood on
    // either side, that could only be settled now.
    for (const auto& deferred : source.deferred) {
        if (expr::evaluate(deferred.value, options) != value) {
            throw exceptions::invalid_request_exception(deferred.disagreement_message);
        }
    }

    if (source.kind == external_search_kind::bm25) {
        return query_value{.term = value_cast<sstring>(utf8_type->deserialize(std::move(value).to_bytes()))};
    }
    auto components = value_cast<vector_type_impl::native_type>(source.column->type->deserialize(value.to_managed_bytes_view()));
    return query_value{.vector = util::to_vector<float>(components)};
}

/// How many candidates one search of this statement is asked for.
uint64_t candidates_wanted(const search_source& source, const std::vector<search_source>& sources, uint64_t limit) {
    auto wanted = limit;
    if (is_hybrid(sources)) {
        wanted *= external_search_select_statement::hybrid_candidate_factor;
    }
    if (source.kind == external_search_kind::ann) {
        // A quantized index answers approximately, so it is asked for more than is wanted and the
        // recomputed scores decide which of them are kept.
        wanted = static_cast<uint64_t>(
                std::ceil(static_cast<double>(wanted) * secondary_index::vector_index::get_oversampling(source.index.metadata().options())));
    }
    return std::min(wanted, external_search_select_statement::max_query_limit);
}

/// The candidate keys of every search, merged.
///
/// A row is worth reading if any search named it, so this is a union: the searches are walked in
/// turn and each key kept the first time it is seen. The order is what the base-table read follows,
/// and it is no search's ranking - a hybrid query is ranked afterwards, from what the searches said
/// about the rows that came back.
std::vector<vector_search::primary_key> union_of(const schema& schema, std::span<const primary_keys> answers) {
    auto seen = std::set<std::pair<bytes, bytes>>{};
    auto candidates = std::vector<vector_search::primary_key>{};

    for (const auto& answer : answers) {
        for (const auto& key : answer) {
            auto clustering = schema.clustering_key_size() > 0 ? key.clustering : clustering_key_prefix::make_empty();
            if (seen.emplace(to_bytes(key.partition.key().representation()), to_bytes(clustering.representation())).second) {
                candidates.push_back(key);
            }
        }
    }
    return candidates;
}

/// Asks the index to mark the terms of `term` in each of `documents`.  One request for them all,
/// answered positionally, so the fragments come back lined up with the rows they were collected from.
future<vector_search::vector_store_client::highlights> fetch_highlights(vector_search::vector_store_client& client,
        const sstring& keyspace, const sstring& index, const sstring& term, std::vector<sstring> documents, abort_source& as) {

    if (documents.empty()) {
        // Nothing matched, so there is nothing to mark up and no reason to ask.
        co_return vector_search::vector_store_client::highlights{};
    }

    auto fragments = co_await client.highlight(keyspace, index, term, std::move(documents), as);
    if (!fragments.has_value()) {
        // Being unable to ask fails the query.  A null fragment means the index found none, and the
        // two must not arrive as the same answer.
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }
    co_return std::move(fragments.value());
}

/// The fragments as the slot that reports them holds each one: text, or null for a row the index
/// found no fragment in - such a row is kept, with the value left absent.
std::vector<cql3::raw_value> to_values(const vector_search::vector_store_client::highlights& fragments) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(fragments.size());
    for (const auto& fragment : fragments) {
        values.push_back(fragment ? cql3::raw_value::make_value(utf8_type->decompose(*fragment)) : cql3::raw_value::make_null());
    }
    return values;
}

/// Checks the one relation a full-text search takes, and returns the comparison of search terms that
/// only execution can make.
std::optional<expr::expression> validate_bm25_restriction(const expr::binary_operator& binop, const search_source& source) {
    // Whichever of the family the user wrote, preparation rewrote it to the reading a relation
    // compares, so this is the score, and the plan has already checked it names this search.
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    auto [column, where_term] = external_search::extract_call_arguments(fc, "BM25");

    if (binop.op != expr::oper_t::GT) {
        throw exceptions::invalid_request_exception(
                seastar::format("Unsupported \"{}\" relation for BM25 function restriction, only \">\" is supported", binop.op));
    }
    const auto* rhs = expr::as_if<expr::constant>(&binop.rhs);
    if (!rhs || rhs->is_null() || rhs->view().deserialize<float>(*float_type) != 0.0f) {
        throw exceptions::invalid_request_exception("BM25 function comparison value must be the literal 0");
    }

    return external_search::check_query_value(where_term, source.query_value,
            "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
}

/// The relations the searches own, checked by the search that owns each.  What a query may restrict
/// by is a property of the search doing the work, so a query running several of them restricts by
/// nothing: the searches do not filter alike, and agreeing them is not this change's to settle.
void validate_restrictions(std::vector<search_source>& sources, const restrictions::statement_restrictions& restrictions) {
    const auto& scoring = restrictions.get_scoring_function_restrictions();

    if (is_hybrid(sources)) {
        if (!scoring.empty() || !restrictions.partition_key_restrictions_is_empty()
                || !restrictions::is_empty_restriction(restrictions.get_clustering_columns_restrictions())
                || !restrictions::is_empty_restriction(restrictions.get_nonprimary_key_restrictions())) {
            throw exceptions::invalid_request_exception("A query running several searches does not support a WHERE clause");
        }
        return;
    }

    auto& source = sources.front();
    if (source.kind == external_search_kind::ann) {
        // Threshold filtering - WHERE ANN(column, query_vector) > score - is not implemented, so a
        // relation claimed for this query would have nothing to interpret it.
        if (!scoring.empty()) {
            throw exceptions::invalid_request_exception("ANN() is not supported in the WHERE clause");
        }
        return;
    }

    if (scoring.empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
    }
    if (scoring.size() > 1) {
        throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
    }
    if (auto deferred = validate_bm25_restriction(scoring.front(), source)) {
        source.deferred.push_back({std::move(*deferred),
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses"});
    }

    // Anything else in the WHERE clause has nothing to apply it: the BM25 relations are held out of
    // `restrictions`, and a full-text index takes no filter.
    if (!restrictions.partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions.get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions.get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support additional WHERE restrictions");
    }
}

} // anonymous namespace

::shared_ptr<select_statement> external_search_select_statement::prepare(
        data_dictionary::database db, std::vector<search_source> sources, external_statement_args args) {

    if (!args.limit.has_value()) {
        throw exceptions::invalid_request_exception(is_ann_only(sources)
                        ? sstring("Vector ANN queries must have a limit specified")
                        : seastar::format("{} queries require a LIMIT", query_kind_name(sources)));
    }
    if (args.per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries do not support per-partition limits", query_kind_name(sources)));
    }
    // GROUP BY is aggregation too, and a reported score does not make the selection look aggregate.
    if (args.selection->is_aggregate() || !args.group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries cannot be run with aggregation", query_kind_name(sources)));
    }

    validate_restrictions(sources, *args.restrictions);

    // A score or rank slot was allocated, so what a search said about each row is reported, and the
    // provider finds it by looking the row up in that search's answer by primary key.
    if (std::ranges::any_of(sources, [] (const search_source& source) { return source.score_slot || source.rank_slot; })) {
        external_search::fetch_primary_key_columns(*args.selection, *args.schema);
    }
    // An excerpt is generated by the index from text it stores none of, so the column has to be read
    // from every row even when the query does not select it.
    for (const auto& source : sources) {
        if (source.fragment_column) {
            external_search::fetch_column(*args.selection, *source.fragment_column);
        }
    }

    auto prepared_filter = !is_hybrid(sources) && sources.front().kind == external_search_kind::ann
            ? external_search::prepare_filter(*args.restrictions, args.parameters->allow_filtering())
            : external_search::prepared_filter{{}, args.parameters->allow_filtering()};

    return ::make_shared<external_search_select_statement>(std::move(sources), std::move(prepared_filter), std::move(args));
}

external_search_select_statement::external_search_select_statement(std::vector<search_source> sources,
        external_search::prepared_filter prepared_filter, external_statement_args args)
    : external_index_select_statement{args.schema, args.bound_terms, args.parameters, args.selection, args.restrictions,
              args.group_by_cell_indices, args.is_reversed, args.ordering_comparator, args.limit, args.per_partition_limit,
              args.stats, sources.front().index, std::move(args.attrs)}
    , _sources(std::move(sources))
    , _prepared_filter(std::move(prepared_filter)) {
}

std::string_view external_search_select_statement::index_search_type_name() const {
    if (is_hybrid(_sources)) {
        return "Hybrid Search";
    }
    return _sources.front().kind == external_search_kind::ann ? "Vector Search" : "Full-Text Search";
}

future<::shared_ptr<cql_transport::messages::result_message>> external_search_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(is_ann_only(_sources)
                        ? seastar::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}",
                                  max_query_limit, limit)
                        : seastar::format("{} queries require a LIMIT that is not greater than {}. LIMIT was {}",
                                  query_kind_name(_sources), max_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    // Everything a search needs of the query is settled before any of them is asked, so that a
    // rejection is a rejection rather than one request already sent.
    auto query_values = std::vector<query_value>{};
    query_values.reserve(_sources.size());
    for (const auto& source : _sources) {
        query_values.push_back(evaluate_query_value(source, options));
    }

    auto& client = qp.vector_store_client();
    auto filter_json = _prepared_filter.to_json(options);

    // The searches are independent, so they are asked at once: a hybrid query costs the slowest of
    // them rather than their sum.
    auto answers = std::vector<primary_keys>(_sources.size());
    co_await coroutine::parallel_for_each(std::views::iota(size_t(0), _sources.size()), [&] (size_t i) -> future<> {
        const auto& source = _sources[i];
        const auto wanted = candidates_wanted(source, _sources, limit);
        const auto& index_name = source.index.metadata().name();

        if (source.kind == external_search_kind::ann) {
            auto answer = co_await client.ann(_schema->ks_name(), index_name, _schema, query_values[i].vector, wanted, filter_json,
                    aoe.abort_source());
            if (!answer.has_value()) {
                co_await coroutine::return_exception(exceptions::invalid_request_exception(
                        std::visit(vector_search::vector_store_client::ann_error_visitor{}, answer.error())));
            }
            answers[i] = std::move(answer.value());
            co_return;
        }

        auto answer = co_await client.bm25(_schema->ks_name(), index_name, _schema, query_values[i].term, wanted, aoe.abort_source());
        if (!answer.has_value()) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    std::visit(vector_search::vector_store_client::fts_error_visitor{}, answer.error())));
        }
        answers[i] = std::move(answer.value());
    });

    auto candidates = union_of(*_schema, answers);
    if (!needs_post_query_ordering() && candidates.size() > limit) {
        // Nothing will reorder the rows, so the one search's own order is the answer and the rest of
        // what it named is not wanted.  A ranked query keeps them all: which of them survive is
        // decided from what the searches said, once the rows are read.
        candidates.erase(candidates.begin() + limit, candidates.end());
    }

    auto read = co_await query_base_table(qp, state, options, timeout, candidates);

    auto provider = std::unique_ptr<external_search_provider>{};
    if (read.rows && std::ranges::any_of(_sources, [] (const search_source& source) { return source.reports_anything(); })) {
        auto requests = std::vector<search_answer_request>{};
        requests.reserve(_sources.size());
        for (size_t i = 0; i < _sources.size(); ++i) {
            requests.push_back(search_answer_request{
                    .results = answers[i],
                    .score_slot = _sources[i].score_slot,
                    .rank_slot = _sources[i].rank_slot,
                    .document_column = _sources[i].fragment_slot ? _sources[i].fragment_column : nullptr,
            });
        }

        // What a search says about a row can only be lined up with it now that the rows are read,
        // and an excerpt does not exist at all until the index has been sent their text.
        auto matched = match_search_results(*read.rows.value(), read.command->slice, *_schema, *_selection, requests);

        for (size_t i = 0; i < _sources.size(); ++i) {
            if (!_sources[i].fragment_slot) {
                continue;
            }
            auto fragments = co_await fetch_highlights(client, _schema->ks_name(), _sources[i].index.metadata().name(),
                    query_values[i].term, std::move(matched.documents[i]), aoe.abort_source());
            matched.slots.emplace_back(*_sources[i].fragment_slot, to_values(fragments));
        }
        provider = std::make_unique<external_search_provider>(std::move(matched.slots), std::move(matched.dropped));
    }
    co_return co_await emit_result_set(std::move(read), options, provider.get());
}

} // namespace cql3::statements
