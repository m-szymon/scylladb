/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"

namespace cql3 {
namespace functions {

// A search answers with two readings of one row, and each is named on its own as well as
// together: BM25(c, t) is the pair, BM25_SCORE(c, t) and BM25_RANK(c, t) its halves.  All three
// name the same search - the arguments say which - so a query writing several of them still costs
// one request.
static const function_name BM25_FUNCTION_NAME = function_name::native_function("bm25");
static const function_name BM25_SCORE_FUNCTION_NAME = function_name::native_function("bm25_score");
static const function_name BM25_RANK_FUNCTION_NAME = function_name::native_function("bm25_rank");
static const function_name ANN_FUNCTION_NAME = function_name::native_function("ann");
static const function_name ANN_SCORE_FUNCTION_NAME = function_name::native_function("ann_score");
static const function_name ANN_RANK_FUNCTION_NAME = function_name::native_function("ann_rank");

/// What a search says about one row: `(score, rank)`.
///
/// The score is the relevance or similarity the index gave the row.  The rank is the position that
/// score put it at among the rows the search returned, 1 for the best - a reading the score alone
/// does not carry, because scores of different searches are not on one scale while their ranks
/// are.  That is what lets the two be fused, and why a search answers with both rather than with
/// the score a caller would otherwise have to rank for itself.
///
/// Score first: it is the reading a bare comparison means.
data_type search_hit_type();

/// Whether `name` is one of the ANN family, whose argument types are not fixed but inferred from
/// the call site.
bool is_ann_function_name(const function_name& name);

shared_ptr<function> make_bm25_function();
shared_ptr<function> make_bm25_score_function();
shared_ptr<function> make_bm25_rank_function();

/// Creates the ANN-family function `name`. The argument types are not fixed: they are inferred
/// from the call site and must be float vectors of the same dimension.
shared_ptr<function> make_ann_function(const function_name& name, const std::vector<data_type>& arg_types);

} // namespace functions
} // namespace cql3
