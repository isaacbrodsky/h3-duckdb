#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

// TODO: For convenience, 0 is returned instead of throwing. However, this may actually be interpreted by
// cellsToMultiPolygon as the index referring to base cell 0.
struct CellsToMultiPolygonWktInputOperator {
	static H3Index Get(const Value &value) {
		if (value.IsNull()) {
			return 0;
		} else {
			return value.GetValue<uint64_t>();
		}
	}
};

struct CellsToMultiPolygonWktVarcharInputOperator {
	static H3Index Get(const Value &value) {
		string str = value.GetValue<string>();
		H3Index cell;
		H3Error err = stringToH3(str.c_str(), &cell);
		if (err) {
			return 0;
		} else {
			return cell;
		}
	}
};

template <LogicalTypeId InputType, class InputOperator>
static void CellsToMultiPolygonWktFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector &lhs = args.data[0];
	if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(lhs);
		return;
	}

	UnifiedVectorFormat lhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);

	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lhs_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		result_entries[i].offset = offset;
		result_entries[i].length = 0;
		auto list_index = lhs_data.sel->get_index(i);

		if (!lhs_data.validity.RowIsValid(list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];

		const auto lvalue = lhs.GetValue(list_entry.offset).DefaultCastAs(LogicalType::LIST(InputType));

		auto &list_children = ListValue::GetChildren(lvalue);

		size_t ii = 0;
		auto input_set = new H3Index[list_children.size()];
		for (const auto &child : list_children) {
			input_set[ii] = InputOperator::Get(child);
			ii++;
		}
		LinkedGeoPolygon first_lgp;
		H3Error err = cellsToLinkedMultiPolygon(input_set, list_children.size(), &first_lgp);

		if (err) {
			result_validity.SetInvalid(i);
		} else {
			std::string str = "MULTIPOLYGON ";

			if (first_lgp.first) {
				str += "(";
				std::string lgp_sep = "";
				LinkedGeoPolygon *lgp = &first_lgp;
				while (lgp) {
					LinkedGeoLoop *loop = lgp->first;
					std::string loop_sep = "";
					str += lgp_sep + "(";
					while (loop) {
						LinkedLatLng *lat_lng = loop->first;
						std::string lat_lng_sep = "";
						str += loop_sep + "(";
						while (lat_lng) {
							str += StringUtil::Format("%s%f %f", lat_lng_sep, radsToDegs(lat_lng->vertex.lng),
							                          radsToDegs(lat_lng->vertex.lat));

							lat_lng_sep = ", ";
							lat_lng = lat_lng->next;
						}
						str += ")";
						loop_sep = ", ";
						loop = loop->next;
					}
					str += ")";
					lgp_sep = ", ";
					lgp = lgp->next;
				}
				str += ")";
			} else {
				str += "EMPTY";
			}

			string_t strAsStr = string_t(strdup(str.c_str()), str.size());
			result.SetValue(i, StringVector::AddString(result, strAsStr));

			destroyLinkedMultiPolygon(&first_lgp);
		}
	}

	result.Verify(args.size());

	if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

CreateScalarFunctionInfo H3Functions::GetCellsToMultiPolygonWktFunction() {
	ScalarFunctionSet funcs("h3_cells_to_multi_polygon_wkt");
	funcs.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::VARCHAR,
	    CellsToMultiPolygonWktFunction<LogicalType::VARCHAR, CellsToMultiPolygonWktVarcharInputOperator>));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::LIST(LogicalType::UBIGINT)}, LogicalType::VARCHAR,
	                   CellsToMultiPolygonWktFunction<LogicalType::UBIGINT, CellsToMultiPolygonWktInputOperator>));
	return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
