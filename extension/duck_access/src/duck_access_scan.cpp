#include "duck_access_scan.hpp"
#include "duck_access_secret.hpp"
#include "duck_access_manifest.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

void DuckAccessScan::Register(DatabaseInstance &instance) {
	auto &config = DBConfig::GetConfig(instance);
	config.replacement_scans.emplace_back(ReplacementScanFunction, nullptr);
}

unique_ptr<TableRef> DuckAccessScan::ReplacementScanFunction(
	ClientContext &context,
	ReplacementScanInput &input,
	optional_ptr<ReplacementScanData> data
) {
	auto &table_name = input.table_name;
	auto &schema_name = input.schema_name;
	// Step 1: Check if a duck_access secret exists.
	// If not, this table isn't ours — return nullptr to let DuckDB handle it.
	auto secret = DuckAccessSecret::GetSecret(context);
	if (!secret) {
		return nullptr;
	}

	// Step 2: Fetch the manifest from the Go API.
	// The replacement scan is only called for unresolved table names,
	// so we know this table doesn't exist locally.
	string error_msg;
	// Use schema from input; default to "main" if empty
	string effective_schema = schema_name.empty() ? "main" : schema_name;
	auto manifest = ManifestCache::GetOrFetch(
		secret->api_url,
		secret->api_key,
		effective_schema,
		table_name,
		error_msg
	);

	if (!manifest) {
		// The API returned an error. Since the user has a duck_access secret,
		// they intend to use the platform — give a descriptive error.
		throw BinderException("duck_access: failed to resolve table '%s': %s",
		                      table_name, error_msg);
	}

	// Step 3: Build the read_parquet() function call with presigned URLs.
	//
	// We construct:
	//   SELECT <masked_columns>
	//   FROM read_parquet([url1, url2, ...])
	//   WHERE <rls_filter_1> AND <rls_filter_2> ...

	// Build read_parquet function expression
	vector<unique_ptr<ParsedExpression>> func_children;

	// Build the list of presigned URLs as a LIST constant
	vector<Value> url_values;
	for (auto &url : manifest->files) {
		url_values.push_back(Value(url));
	}
	func_children.push_back(
		make_uniq<ConstantExpression>(Value::LIST(LogicalType::VARCHAR, url_values))
	);

	auto function = make_uniq<FunctionExpression>("read_parquet", std::move(func_children));

	// Note: presigned URLs use https:// scheme, not s3://, so httpfs will treat
	// them as plain HTTP requests and will NOT inject S3 auth headers.
	// No secret='NONE' workaround is needed.

	// If no row filters and no column masks, return simple table function ref
	if (manifest->row_filters.empty() && manifest->column_masks.empty()) {
		auto table_function = make_uniq<TableFunctionRef>();
		table_function->function = std::move(function);
		table_function->alias = table_name;
		return std::move(table_function);
	}

	// Step 4: Wrap in a SubqueryRef for RLS + column masking enforcement.
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();

	// FROM clause: read_parquet(...)
	auto from_ref = make_uniq<TableFunctionRef>();
	from_ref->function = std::move(function);
	from_ref->alias = "__duck_access_src";
	select_node->from_table = std::move(from_ref);

	// SELECT list: apply column masks
	if (manifest->column_masks.empty()) {
		// No masks — SELECT *
		select_node->select_list.push_back(make_uniq<StarExpression>());
	} else {
		// Build explicit column list with mask expressions applied
		for (auto &col : manifest->columns) {
			auto mask_it = manifest->column_masks.find(col.name);
			if (mask_it != manifest->column_masks.end()) {
				// Parse the mask expression and alias it to the column name
				// e.g., '***' AS "Name"
				auto mask_sql = mask_it->second + " AS " + KeywordHelper::WriteOptionallyQuoted(col.name);
				auto expressions = Parser::ParseExpressionList(mask_sql);
				if (!expressions.empty()) {
					select_node->select_list.push_back(std::move(expressions[0]));
				} else {
					// Fallback: use the column reference unmasked
					select_node->select_list.push_back(
						make_uniq<ColumnRefExpression>(col.name)
					);
				}
			} else {
				// No mask — regular column reference
				select_node->select_list.push_back(
					make_uniq<ColumnRefExpression>(col.name)
				);
			}
		}
	}

	// WHERE clause: combine RLS filters with AND
	if (!manifest->row_filters.empty()) {
		unique_ptr<ParsedExpression> combined_filter;

		for (auto &filter_sql : manifest->row_filters) {
			auto filter_exprs = Parser::ParseExpressionList(filter_sql);
			if (filter_exprs.empty()) {
				continue;
			}

			if (!combined_filter) {
				combined_filter = std::move(filter_exprs[0]);
			} else {
				combined_filter = make_uniq<ConjunctionExpression>(
					ExpressionType::CONJUNCTION_AND,
					std::move(combined_filter),
					std::move(filter_exprs[0])
				);
			}
		}

		if (combined_filter) {
			select_node->where_clause = std::move(combined_filter);
		}
	}

	select_statement->node = std::move(select_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_statement));
	subquery->alias = table_name;
	return std::move(subquery);
}

} // namespace duckdb
