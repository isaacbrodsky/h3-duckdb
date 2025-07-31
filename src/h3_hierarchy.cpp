#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void CellToParentFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<uint64_t, int, H3Index>(
      inputs, inputs2, result, args.size(),
      [&](uint64_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index parent;
        H3Error err = cellToParent(input, res, &parent);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return parent;
        }
      });
}

static void CellToParentVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, int, string_t>(
      inputs, inputs2, result, args.size(),
      [&](string_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err0 = stringToH3(input.GetString().c_str(), &h);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index parent;
          H3Error err1 = cellToParent(h, res, &parent);
          if (err1) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", parent);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void CellToChildrenFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t parent = args.GetValue(0, i)
                          .DefaultCastAs(LogicalType::UBIGINT)
                          .GetValue<uint64_t>();
    int32_t res = args.GetValue(1, i)
                      .DefaultCastAs(LogicalType::INTEGER)
                      .GetValue<int32_t>();
    int64_t sz;
    H3Error err1 = cellToChildrenSize(parent, res, &sz);
    if (err1) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {

      std::vector<H3Index> out(sz);
      H3Error err2 = cellToChildren(parent, res, out.data());
      if (err2) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        int64_t actual = 0;
        for (auto val : out) {
          if (val != H3_NULL) {
            ListVector::PushBack(result, Value::UBIGINT(val));
            actual++;
          }
        }

        result_data[i].length = actual;
      }
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void CellToChildrenVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  UnifiedVectorFormat vdata;
  args.data[0].ToUnifiedFormat(args.size(), vdata);
  auto ldata = UnifiedVectorFormat::GetData<string_t>(vdata);

  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    string_t parentStr = ldata[i];
    int32_t res = args.GetValue(1, i)
                      .DefaultCastAs(LogicalType::INTEGER)
                      .GetValue<int32_t>();
    H3Index parent;
    H3Error err0 = stringToH3(parentStr.GetString().c_str(), &parent);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t sz;
      H3Error err1 = cellToChildrenSize(parent, res, &sz);
      if (err1) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        std::vector<H3Index> out(sz);
        H3Error err2 = cellToChildren(parent, res, out.data());
        if (err2) {
          result.SetValue(i, Value(LogicalType::SQLNULL));
        } else {
          int64_t actual = 0;
          for (auto val : out) {
            if (val != H3_NULL) {
              auto str = StringUtil::Format("%llx", val);
              string_t strAsStr = string_t(strdup(str.c_str()), str.size());
              ListVector::PushBack(result, strAsStr);
              actual++;
            }
          }

          result_data[i].length = actual;
        }
      }
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void CellToCenterChildVarcharFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, int, string_t>(
      inputs, inputs2, result, args.size(),
      [&](string_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err0 = stringToH3(input.GetString().c_str(), &h);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index parent;
          H3Error err1 = cellToCenterChild(h, res, &parent);
          if (err1) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", parent);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void CellToCenterChildFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<uint64_t, int, H3Index>(
      inputs, inputs2, result, args.size(),
      [&](uint64_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index child;
        H3Error err = cellToCenterChild(input, res, &child);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return child;
        }
      });
}

static void CellToChildPosVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, int, int64_t>(
      inputs, inputs2, result, args.size(),
      [&](string_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err0 = stringToH3(input.GetString().c_str(), &h);
        if (err0) {
          mask.SetInvalid(idx);
          return int64_t(0);
        } else {
          int64_t child;
          H3Error err1 = cellToChildPos(h, res, &child);
          if (err1) {
            mask.SetInvalid(idx);
            return int64_t(0);
          } else {
            return child;
          }
        }
      });
}

static void CellToChildPosFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<H3Index, int, int64_t>(
      inputs, inputs2, result, args.size(),
      [&](H3Index input, int res, ValidityMask &mask, idx_t idx) {
        int64_t child;
        H3Error err = cellToChildPos(input, res, &child);
        if (err) {
          mask.SetInvalid(idx);
          return int64_t(0);
        } else {
          return child;
        }
      });
}

static void ChildPosToCellVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  auto &inputs3 = args.data[2];
  TernaryExecutor::ExecuteWithNulls<int64_t, string_t, int, string_t>(
      inputs, inputs2, inputs3, result, args.size(),
      [&](int64_t pos, string_t input, int res, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err0 = stringToH3(input.GetString().c_str(), &h);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index child;
          H3Error err = childPosToCell(pos, h, res, &child);
          if (err) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", child);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void ChildPosToCellFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  auto &inputs3 = args.data[2];
  TernaryExecutor::ExecuteWithNulls<int64_t, H3Index, int, H3Index>(
      inputs, inputs2, inputs3, result, args.size(),
      [&](int64_t pos, H3Index input, int res, ValidityMask &mask, idx_t idx) {
        H3Index child;
        H3Error err = childPosToCell(pos, input, res, &child);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return child;
        }
      });
}

static void CompactCellsFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  D_ASSERT(args.ColumnCount() == 1);
  auto count = args.size();

  Vector &lists = args.data[0];
  if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(lists);
    return;
  }

  auto lists_size = ListVector::GetListSize(lists);
  auto &child_vector = ListVector::GetEntry(lists);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  lists.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<list_entry_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    result_entries[i].offset = offset;
    result_entries[i].length = 0;
    auto list_index = lists_data.sel->get_index(i);

    if (!lists_data.validity.RowIsValid(list_index)) {
      result_validity.SetInvalid(i);
      continue;
    }

    vector<H3Index> input_set(list_entries[i].length);
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        input_set[j] =
            ((H3Index *)child_data
                 .data)[child_data.sel->get_index(list_entries[i].offset + j)];
      }
    }
    vector<H3Index> compacted(input_set.size());
    H3Error err =
        compactCells(input_set.data(), compacted.data(), input_set.size());

    if (err) {
      result_validity.SetInvalid(i);
    } else {
      int64_t actual = 0;
      for (size_t k = 0; k < input_set.size(); k++) {
        auto child_val = compacted[k];
        if (child_val != H3_NULL) {
          ListVector::PushBack(result, Value::UBIGINT(child_val));
          actual++;
        }
      }

      result_entries[i].length = actual;
      offset += actual;
    }
  }

  if (lists.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void CompactCellsVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  D_ASSERT(args.ColumnCount() == 1);
  auto count = args.size();

  Vector &lists = args.data[0];
  if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(lists);
    return;
  }

  auto lists_size = ListVector::GetListSize(lists);
  auto &child_vector = ListVector::GetEntry(lists);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  lists.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<list_entry_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    result_entries[i].offset = offset;
    result_entries[i].length = 0;
    auto list_index = lists_data.sel->get_index(i);

    if (!lists_data.validity.RowIsValid(list_index)) {
      result_validity.SetInvalid(i);
      continue;
    }

    vector<H3Index> input_set(list_entries[i].length);
    bool hasInvalid = false;
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        H3Index tmp;
        H3Error tmpErr = stringToH3(
            ((string_t *)child_data
                 .data)[child_data.sel->get_index(list_entries[i].offset + j)]
                .GetString()
                .c_str(),
            &tmp);
        if (tmpErr) {
          hasInvalid = true;
          break;
        } else {
          input_set[j] = tmp;
        }
      }
    }
    if (hasInvalid) {
      result_validity.SetInvalid(i);
      continue;
    } else {
      vector<H3Index> compacted(input_set.size());
      H3Error err =
          compactCells(input_set.data(), compacted.data(), input_set.size());

      if (err) {
        result_validity.SetInvalid(i);
      } else {
        int64_t actual = 0;
        for (size_t k = 0; k < input_set.size(); k++) {
          auto child_val = compacted[k];
          if (child_val != H3_NULL) {
            auto str = StringUtil::Format("%llx", child_val);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            ListVector::PushBack(result, strAsStr);
            actual++;
          }
        }

        result_entries[i].length = actual;
        offset += actual;
      }
    }
  }

  if (lists.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void UncompactCellsFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  D_ASSERT(args.ColumnCount() == 2);
  auto count = args.size();

  Vector &lhs = args.data[0];
  if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(lhs);
    return;
  }
  Vector res_vec = args.data[1];
  if (res_vec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(res_vec);
    return;
  }

  auto lists_size = ListVector::GetListSize(lhs);
  auto &child_vector = ListVector::GetEntry(lhs);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  lhs.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  UnifiedVectorFormat res_data;
  res_vec.ToUnifiedFormat(count, res_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<list_entry_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    result_entries[i].offset = offset;
    result_entries[i].length = 0;
    auto list_index = lists_data.sel->get_index(i);

    if (!lists_data.validity.RowIsValid(list_index) ||
        !res_data.validity.RowIsValid(list_index)) {
      result_validity.SetInvalid(i);
      continue;
    }

    const auto res = res_vec.GetValue(i)
                         .DefaultCastAs(LogicalType::INTEGER)
                         .GetValue<int32_t>();

    vector<H3Index> input_set(list_entries[i].length);
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        input_set[j] =
            ((H3Index *)child_data
                 .data)[child_data.sel->get_index(list_entries[i].offset + j)];
      }
    }
    int64_t uncompacted_sz;
    H3Error sz_err = uncompactCellsSize(input_set.data(), input_set.size(), res,
                                        &uncompacted_sz);
    if (sz_err) {
      result_validity.SetInvalid(i);
      continue;
    }
    vector<H3Index> uncompacted(uncompacted_sz);
    H3Error err = uncompactCells(input_set.data(), input_set.size(),
                                 uncompacted.data(), uncompacted_sz, res);

    if (err) {
      result_validity.SetInvalid(i);
    } else {
      int64_t actual = 0;
      for (size_t k = 0; k < uncompacted_sz; k++) {
        auto child_val = uncompacted[k];
        if (child_val != H3_NULL) {
          ListVector::PushBack(result, Value::UBIGINT(child_val));
          actual++;
        }
      }

      result_entries[i].length = actual;
      offset += actual;
    }
  }

  if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void UncompactCellsVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  D_ASSERT(args.ColumnCount() == 2);
  auto count = args.size();

  Vector &lhs = args.data[0];
  if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(lhs);
    return;
  }
  Vector res_vec = args.data[1];
  if (res_vec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(res_vec);
    return;
  }

  auto lists_size = ListVector::GetListSize(lhs);
  auto &child_vector = ListVector::GetEntry(lhs);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  lhs.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  UnifiedVectorFormat res_data;
  res_vec.ToUnifiedFormat(count, res_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<list_entry_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    result_entries[i].offset = offset;
    result_entries[i].length = 0;
    auto list_index = lists_data.sel->get_index(i);

    if (!lists_data.validity.RowIsValid(list_index) ||
        !res_data.validity.RowIsValid(list_index)) {
      result_validity.SetInvalid(i);
      continue;
    }

    const auto res = res_vec.GetValue(i)
                         .DefaultCastAs(LogicalType::INTEGER)
                         .GetValue<int32_t>();

    vector<H3Index> input_set(list_entries[i].length);
    bool hasInvalid = false;
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        H3Index tmp;
        H3Error tmpErr = stringToH3(
            ((string_t *)child_data
                 .data)[child_data.sel->get_index(list_entries[i].offset + j)]
                .GetString()
                .c_str(),
            &tmp);
        if (tmpErr) {
          hasInvalid = true;
          break;
        } else {
          input_set[j] = tmp;
        }
      }
    }
    if (hasInvalid) {
      result_validity.SetInvalid(i);
      continue;
    } else {
      int64_t uncompacted_sz;
      H3Error sz_err = uncompactCellsSize(input_set.data(), input_set.size(),
                                          res, &uncompacted_sz);
      if (sz_err) {
        result_validity.SetInvalid(i);
        continue;
      }
      vector<H3Index> uncompacted(uncompacted_sz);
      H3Error err = uncompactCells(input_set.data(), input_set.size(),
                                   uncompacted.data(), uncompacted_sz, res);

      if (err) {
        result_validity.SetInvalid(i);
      } else {
        int64_t actual = 0;
        for (size_t k = 0; k < uncompacted_sz; k++) {
          auto child_val = uncompacted[k];
          if (child_val != H3_NULL) {
            auto str = StringUtil::Format("%llx", child_val);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            ListVector::PushBack(result, strAsStr);
            actual++;
          }
        }

        result_entries[i].length = actual;
        offset += actual;
      }
    }
  }

  if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

CreateScalarFunctionInfo H3Functions::GetCellToParentFunction() {
  ScalarFunctionSet funcs("h3_cell_to_parent");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::VARCHAR,
                                   CellToParentVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::UBIGINT, CellToParentFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::BIGINT, CellToParentFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToChildrenFunction() {
  ScalarFunctionSet funcs("h3_cell_to_children");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   CellToChildrenVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   CellToChildrenFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::LIST(LogicalType::BIGINT),
                                   CellToChildrenFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToCenterChildFunction() {
  ScalarFunctionSet funcs("h3_cell_to_center_child");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::VARCHAR,
                                   CellToCenterChildVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::UBIGINT,
                                   CellToCenterChildFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::BIGINT,
                                   CellToCenterChildFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToChildPosFunction() {
  ScalarFunctionSet funcs("h3_cell_to_child_pos");
  // Note this does not return an index, rather it returns a position ID
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::BIGINT,
                                   CellToChildPosVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::BIGINT,
                                   CellToChildPosFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::BIGINT,
                                   CellToChildPosFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetChildPosToCellFunction() {
  ScalarFunctionSet funcs("h3_child_pos_to_cell");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::INTEGER},
      LogicalType::VARCHAR, ChildPosToCellVarcharFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::INTEGER},
      LogicalType::UBIGINT, ChildPosToCellFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::INTEGER},
      LogicalType::BIGINT, ChildPosToCellFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCompactCellsFunction() {
  ScalarFunctionSet funcs("h3_compact_cells");
  // TODO: Refactor this to use a templated InputOperator, reference
  // h3_cells_to_multi_polygon_wkt
  funcs.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::VARCHAR)},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   CompactCellsVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::UBIGINT)},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   CompactCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::BIGINT)},
                                   LogicalType::LIST(LogicalType::BIGINT),
                                   CompactCellsFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetUncompactCellsFunction() {
  ScalarFunctionSet funcs("h3_uncompact_cells");
  // TODO: Refactor this to use a templated InputOperator, reference
  // h3_cells_to_multi_polygon_wkt
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::VARCHAR), UncompactCellsVarcharFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::UBIGINT), LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::UBIGINT), UncompactCellsFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::BIGINT), LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::BIGINT), UncompactCellsFunction));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
