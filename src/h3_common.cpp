#include "h3_common.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void ThrowH3Error(H3Error err) {
  if (err) {
    throw InvalidInputException(StringUtil::Format("H3 error: '%d'", err));
  }
}

} // namespace duckdb
