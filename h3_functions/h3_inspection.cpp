#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void GetResolutionFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, int>(
      inputs, result, args.size(),
      [&](H3Index cell) { return getResolution(cell); });
}

static void GetBaseCellNumberFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, int>(
      inputs, result, args.size(),
      [&](H3Index cell) { return getBaseCellNumber(cell); });
}

static void StringToH3Function(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<string_t, uint64_t>(
      inputs, result, args.size(), [&](string_t input) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          ThrowH3Error(err);
        }
        return h;
      });
}

static void H3ToStringFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, string_t>(
      inputs, result, args.size(), [&](uint64_t h3) {
        char buf[17] = {0};
        H3Error err = h3ToString(h3, buf, sizeof(buf));
        if (err) {
          ThrowH3Error(err);
        }
        return string_t(buf);
      });
}

static void IsValidCellFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<string_t, bool>(
      inputs, result, args.size(), [&](string_t input) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          return false;
        }
        return bool(isValidCell(h));
      });
}

static void IsResClassIIIFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, bool>(
      inputs, result, args.size(),
      [&](uint64_t cell) { return bool(isResClassIII(cell)); });
}

static void IsPentagonFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, bool>(
      inputs, result, args.size(),
      [&](uint64_t cell) { return bool(isPentagon(cell)); });
}

CreateScalarFunctionInfo H3Functions::GetGetResolutionFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_resolution", {LogicalType::UBIGINT}, LogicalType::INTEGER,
      GetResolutionFunction, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetGetBaseCellNumberFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_base_cell_number", {LogicalType::UBIGINT}, LogicalType::INTEGER,
      GetBaseCellNumberFunction, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetStringToH3Function() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_string_to_h3", {LogicalType::VARCHAR}, LogicalType::UBIGINT,
      StringToH3Function, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetH3ToStringFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_h3_to_string", {LogicalType::UBIGINT}, LogicalType::VARCHAR,
      H3ToStringFunction, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetIsValidCellFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_is_valid_cell", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
      IsValidCellFunction, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetIsResClassIIIFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_is_res_class_iii", {LogicalType::UBIGINT}, LogicalType::BOOLEAN,
      IsResClassIIIFunction, false, nullptr, nullptr, nullptr));
}

CreateScalarFunctionInfo H3Functions::GetIsPentagonFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_is_pentagon", {LogicalType::UBIGINT}, LogicalType::BOOLEAN,
      IsPentagonFunction, false, nullptr, nullptr, nullptr));
}

} // namespace duckdb
