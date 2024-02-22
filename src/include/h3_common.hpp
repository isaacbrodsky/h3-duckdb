//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "h3api.h"

namespace duckdb {

void ThrowH3Error(H3Error err);

} // namespace duckdb
