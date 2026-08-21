/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "scoring_fcts.hh"
#include "native_scalar_function.hh"
#include "types/tuple.hh"
#include "types/types.hh"
#include "utils/log.hh"
#include <seastar/core/on_internal_error.hh>

namespace cql3 {
namespace functions {

extern logging::logger log;

namespace {

/// A native scalar function whose value comes from an external search system rather than from
/// evaluating its arguments locally.
///
/// Being external implies the two invariants every such function needs, so they are stated here
/// once instead of once per function:
///  - non-pure, so that the expression evaluator does not constant-fold a call whose arguments
///    happen to all be literals before statement preparation gets to claim it;
///  - no evaluation of its own: preparation either lowers the call to a value injected per row or
///    rejects the clause it appears in, so reaching this body is a bug.
class external_scalar_function : public native_scalar_function {
    // The reading a relation on this function compares, null when there is none.  Held rather than
    // computed, because a relation needs it before anything has decided which search this is.
    const function_name* _comparison_reading;

public:
    external_scalar_function(sstring name, data_type return_type, std::vector<data_type> arg_types,
            const function_name* comparison_reading)
        : native_scalar_function(std::move(name), std::move(return_type), std::move(arg_types))
        , _comparison_reading(comparison_reading) {
    }

    const function_name* comparison_reading() const override {
        return _comparison_reading;
    }

    bool is_pure() const override {
        return false;
    }

    bool is_external() const override {
        return true;
    }

    bytes_opt execute(std::span<const bytes_opt>) override {
        on_internal_error(log, format("{}() reached scalar evaluation; prepare-time handling should have prevented this", name()));
    }
};

/// The type each reading of the ANN family answers with.
data_type ann_return_type(const function_name& name) {
    if (name == ANN_SCORE_FUNCTION_NAME) {
        return float_type;
    }
    if (name == ANN_RANK_FUNCTION_NAME) {
        return int32_type;
    }
    return search_hit_type();
}

/// What a relation on an ANN-family call compares.  The score, for the pair and for itself; a rank
/// has no threshold that means anything, so nothing at all.
const function_name* ann_comparison_reading(const function_name& name) {
    return name == ANN_RANK_FUNCTION_NAME ? nullptr : &ANN_SCORE_FUNCTION_NAME;
}

} // anonymous namespace

data_type search_hit_type() {
    // Interned by the type registry, so asking for it per call costs a lookup rather than a type.
    return tuple_type_impl::get_instance({float_type, int32_type});
}

bool is_ann_function_name(const function_name& name) {
    return name == ANN_FUNCTION_NAME || name == ANN_SCORE_FUNCTION_NAME || name == ANN_RANK_FUNCTION_NAME;
}

shared_ptr<function> make_bm25_function() {
    // Full-text search: bm25(column, query) -> (score, rank)
    // Registered with utf8_type args; ascii is implicitly coerced to utf8 by the type system.
    return ::make_shared<external_scalar_function>(BM25_FUNCTION_NAME.name, search_hit_type(),
            std::vector<data_type>{utf8_type, utf8_type}, &BM25_SCORE_FUNCTION_NAME);
}

shared_ptr<function> make_bm25_score_function() {
    return ::make_shared<external_scalar_function>(BM25_SCORE_FUNCTION_NAME.name, float_type,
            std::vector<data_type>{utf8_type, utf8_type}, &BM25_SCORE_FUNCTION_NAME);
}

shared_ptr<function> make_bm25_rank_function() {
    return ::make_shared<external_scalar_function>(BM25_RANK_FUNCTION_NAME.name, int32_type,
            std::vector<data_type>{utf8_type, utf8_type}, nullptr);
}

shared_ptr<function> make_bm25_highlight_function() {
    // Full-text search highlighting: bm25_highlight(column, query) -> text
    //
    // Answers with a fragment of the row's own text, with the terms of the query marked. Only the
    // full-text index can pick the fragment - it needs the corpus statistics and the analyzer -
    // and it may find none, which is why the return type is nullable.
    //
    // A fragment is not a number and no threshold on one means anything, so no relation can be
    // written on it.
    return ::make_shared<external_scalar_function>(BM25_HIGHLIGHT_FUNCTION_NAME.name, utf8_type,
            std::vector<data_type>{utf8_type, utf8_type}, nullptr);
}

/// How much a rank of 1 is worth relative to the ranks behind it.
///
/// Reciprocal-rank fusion scores a row at sum(1 / (k + rank)) over the searches that found it. The
/// constant flattens the curve: without it the top rank would be worth so much more than the second
/// that agreement between searches could never outweigh one search's first place. 60 is the value
/// the original paper settled on and what every implementation uses, so a score computed here is
/// comparable with one computed anywhere else.
constexpr int32_t RRF_K = 60;

shared_ptr<function> make_rrf_function(size_t arity) {
    // rrf(hit, hit, ...) -> float
    //
    // Pure: it is an ordinary computation over what the searches answered, with nothing external
    // about it. Its arguments are what make a call to it worth keeping until execution.
    return make_native_scalar_function<true>(RRF_FUNCTION_NAME.name, float_type,
            std::vector<data_type>(arity, search_hit_type()),
            [] (std::span<const bytes_opt> args) -> bytes_opt {
        float score = 0.0f;
        for (const auto& arg : args) {
            if (!arg) {
                // No answer at all from that search: it did not find this row.
                continue;
            }
            auto hit = value_cast<tuple_type_impl::native_type>(search_hit_type()->deserialize(bytes_view(*arg)));
            // A pair whose rank is absent says the same thing. The score half is not read: ranks
            // are what searches can be compared on, which is the whole point of fusing by them.
            if (hit.size() < 2 || hit[1].is_null()) {
                continue;
            }
            score += 1.0f / static_cast<float>(RRF_K + value_cast<int32_t>(hit[1]));
        }
        // A row no search found scores 0 and sorts last, which is where it belongs.
        return float_type->decompose(score);
    });
}

shared_ptr<function> make_ann_function(const function_name& name, const std::vector<data_type>& arg_types) {
    // Vector search: ann(column, query_vector) -> (score, rank), and its two halves.
    return ::make_shared<external_scalar_function>(name.name, ann_return_type(name), arg_types, ann_comparison_reading(name));
}

} // namespace functions
} // namespace cql3
