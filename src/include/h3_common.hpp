//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "h3api.h"

#define DUCKDB_API_NO_DEPRECATED
#include "duckdb_extension.h"

DUCKDB_EXTENSION_EXTERN

#include <string>
#include <stdexcept>

namespace h3duckdb {

std::string ToHexString(H3Index index);

std::string DuckdbToString(duckdb_string_t *str);

template <typename T> inline H3Index IndexFromVector(T *data, idx_t idx) {
  static_assert(std::is_same<T, duckdb_string_t>::value ||
                    std::is_same<T, uint64_t>::value ||
                    std::is_same<T, int64_t>::value,
                "T must be duckdb_string_t, uint64_t, or int64_t");
  constexpr auto IsStringT = std::is_same<T, duckdb_string_t>::value;
  H3Index cell;
  if constexpr (IsStringT) {
    auto str = static_cast<duckdb_string_t *>(&data[idx]);
    auto str2 = DuckdbToString(str);
    H3Error err = stringToH3(str2.c_str(), &cell);
    if (err) {
      cell = 0;
    }
  } else {
    cell = data[idx];
  }
  return cell;
}

template <typename T>
inline void AssignHexString(duckdb_vector &output, T *resultData, idx_t row,
                            H3Index out) {
  static_assert(std::is_same<T, duckdb_string_t>::value ||
                    std::is_same<T, uint64_t>::value ||
                    std::is_same<T, int64_t>::value,
                "T must be duckdb_string_t, uint64_t, or int64_t");
  constexpr auto IsStringT = std::is_same<T, duckdb_string_t>::value;

  if constexpr (IsStringT) {
    auto str = ToHexString(out);
    duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                            str.size());
  } else {
    resultData[row] = out;
  }
}

template <typename T, typename U, typename Operator>
void InspectGenericFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  U *resultData = (U *)duckdb_vector_get_data(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto cell = IndexFromVector(indexVecData, row);

    resultData[row] = Operator::operate(cell);
  }
}

/** Get a generic index -> ResultType function, for all index input types.
 * ResultType should be a simple type like integer or boolean, not string or
 * list.
 */
template <typename ResultType, typename Operator>
duckdb_scalar_function_set GetGenericInspectFunction(const char *name,
                                                     duckdb_type returnTypeId) {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set(name);

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type returnType = duckdb_create_logical_type(returnTypeId);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, InspectGenericFunction<uint64_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, InspectGenericFunction<int64_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function,
        InspectGenericFunction<duckdb_string_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&returnType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

void AppendDouble(std::string &str, double d);

class H3Exception : public std::runtime_error {
public:
  H3Exception(std::string err) : std::runtime_error(err){};
};

} // namespace h3duckdb
