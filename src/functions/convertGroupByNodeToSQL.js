// src/functions/convertGroupByNodeToSQL.js

import { getEntryValue } from "../common/getEntryValue"; // [cite: uploaded:src/common/getEntryValue.js]
import { findConfigByKey } from "../common/findConfigByKey"; // [cite: uploaded:src/common/findConfigByKey.js]
import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig"; // [cite: uploaded:src/common/getArrayValuesFromConfig.js]

/**
 * Maps KNIME aggregation methods to SQL aggregate functions.
 * Note: Some mappings might be dialect-specific (e.g., list aggregation).
 * @param {string} knimeMethod - The KNIME aggregation method string.
 * @returns {string|null} - The corresponding SQL function name or template, or null if unsupported.
 */
const mapKnimeAggregationToSQL = (knimeMethod) => {
  // Basic Aggregations
  if (knimeMethod === "Sum") return "SUM";
  if (knimeMethod === "Count") return "COUNT"; // Needs column or *
  if (knimeMethod === "Mean" || knimeMethod === "Average") return "AVG";
  if (knimeMethod === "Minimum") return "MIN";
  if (knimeMethod === "Maximum") return "MAX";
  if (knimeMethod === "StandardDeviation") return "STDDEV_SAMP"; // Or STDDEV_POP, STDDEV
  if (knimeMethod === "Variance") return "VAR_SAMP"; // Or VAR_POP, VARIANCE
  if (knimeMethod === "Median") return null; // Median often requires specific window functions or PERCENTILE_CONT/DISC

  // String/List Aggregations (Dialect Specific!)
  if (knimeMethod === "Concatenate" || knimeMethod === "List") {
    // Choose one based on target DB, or provide comment
    // return "GROUP_CONCAT"; // MySQL, SQLite
    // return "STRING_AGG"; // PostgreSQL, SQL Server 2017+
    return "LISTAGG"; // Oracle, DB2, Redshift, Snowflake (often preferred, requires delimiter)
  }

  // Positional Aggregations (Often require window functions or MIN/MAX on ordered data)
  if (knimeMethod === "First") return "MIN"; // Simplification: MIN often gives the 'first' value in sorted group
  if (knimeMethod === "Last") return "MAX"; // Simplification: MAX often gives the 'last' value in sorted group

  // Other types
  // Placeholder $$col$$ will be replaced with the actual quoted column name
  if (knimeMethod === "Unique count") return "COUNT(DISTINCT $$col$$)";
  if (knimeMethod === "Missing value count")
    return "SUM(CASE WHEN $$col$$ IS NULL THEN 1 ELSE 0 END)";

  // Mode might require subquery/window functions

  console.warn(`Unsupported KNIME aggregation method: ${knimeMethod}`);
  return null;
};

/**
 * Converts a KNIME GroupBy node configuration (compact JSON) to an SQL query.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {string} previousNodeName - The name of the table/view representing the input data.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertGroupByNodeToSQL(
  nodeConfig,
  previousNodeName = "input_table"
) {
  // --- 1. Verify Node Type ---
  const factory = getEntryValue(nodeConfig?.entry, "factory"); // [cite: uploaded:src/common/getEntryValue.js]
  const GROUPBY_FACTORY =
    "org.knime.base.node.preproc.groupby.GroupByNodeFactory";
  if (factory !== GROUPBY_FACTORY) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected GroupBy node factory (${GROUPBY_FACTORY}), but got ${factoryInfo}.`;
  }

  // --- 2. Locate Model ---
  const modelNode = findConfigByKey(nodeConfig.config, "model"); // [cite: uploaded:src/common/findConfigByKey.js]
  if (!modelNode || !modelNode.config || !modelNode.entry) {
    return "Error: Model configuration not found or invalid.";
  }

  // --- 3. Extract Grouping Columns ---
  const groupByColumnsNode = findConfigByKey(modelNode.config, "grouByColumns");
  const inclListGroupNode = findConfigByKey(
    groupByColumnsNode?.config,
    "InclList"
  );
  const groupingColumns = getArrayValuesFromConfig(inclListGroupNode); // [cite: uploaded:src/common/getArrayValuesFromConfig.js]

  if (!groupingColumns || groupingColumns.length === 0) {
    console.warn("No grouping columns specified. Aggregating entire table.");
    // Continue, will handle logic based on aggregations present
  }
  const quotedGroupingColumns = groupingColumns.map(
    (col) => `"${col.replace(/"/g, '""')}"`
  );

  // --- 4. Extract Aggregation Columns & Methods ---
  const aggColumnNode = findConfigByKey(modelNode.config, "aggregationColumn");
  const aggColNamesNode = findConfigByKey(aggColumnNode?.config, "columnNames");
  const aggColMethodsNode = findConfigByKey(
    aggColumnNode?.config,
    "aggregationMethod"
  );

  const aggColumnNames = getArrayValuesFromConfig(aggColNamesNode);
  const aggMethods = getArrayValuesFromConfig(aggColMethodsNode);
  const columnNamePolicy =
    getEntryValue(modelNode.entry, "columnNamePolicy") ||
    "Aggregation method (column name)";
  const valueDelimiter =
    getEntryValue(modelNode.entry, "valueDelimiter") || ", ";

  const aggregations = [];
  if (aggColumnNames.length !== aggMethods.length) {
    console.warn(
      "Mismatch between aggregation column names and methods count. Using minimum length."
    );
  }

  const numAggs = Math.min(aggColumnNames.length, aggMethods.length);
  for (let i = 0; i < numAggs; i++) {
    const colName = aggColumnNames[i];
    const knimeMethod = aggMethods[i];
    const sqlFunctionTemplate = mapKnimeAggregationToSQL(knimeMethod);

    if (sqlFunctionTemplate) {
      const quotedColName = `"${colName.replace(/"/g, '""')}"`;
      let sqlFunctionCall;

      // Handle functions needing special syntax
      if (sqlFunctionTemplate.includes("$$col$$")) {
        sqlFunctionCall = sqlFunctionTemplate.replace(
          /\$\$col\$\$/g,
          quotedColName
        ); // Use regex replaceAll
      } else if (knimeMethod === "Count") {
        // KNIME's Count usually counts non-missing values of the column. COUNT(*) counts rows.
        sqlFunctionCall = `COUNT(${quotedColName})`;
      } else if (knimeMethod === "List" || knimeMethod === "Concatenate") {
        // LISTAGG example (adjust function name and syntax for other dialects like STRING_AGG or GROUP_CONCAT)
        const delimiter = valueDelimiter;
        // Basic LISTAGG - Add WITHIN GROUP if ordering is needed based on KNIME config/behavior
        sqlFunctionCall = `${sqlFunctionTemplate}(${quotedColName}, '${delimiter.replace(
          /'/g,
          "''"
        )}')`;
        // Example with ordering (if KNIME implies order or retainOrder=true):
        // sqlFunctionCall = `${sqlFunctionTemplate}(${quotedColName}, '${delimiter.replace(/'/g,"''")}') WITHIN GROUP (ORDER BY ${quotedColName})`;
      } else {
        // Standard function call like SUM(col), AVG(col), MIN(col), MAX(col)
        sqlFunctionCall = `${sqlFunctionTemplate}(${quotedColName})`;
      }

      // Determine alias based on policy
      let alias = "";
      // Basic cleanup for alias generation
      const cleanColName = colName.replace(/[^a-zA-Z0-9_]/g, "_");
      const cleanMethod = knimeMethod.replace(/[^a-zA-Z0-9_]/g, "_");

      if (columnNamePolicy === "Aggregation method (column name)") {
        alias = `${cleanMethod}_${cleanColName}`;
      } else if (columnNamePolicy === "Column name (aggregation method)") {
        alias = `${cleanColName}_${cleanMethod}`;
      } else {
        // Default: Keep original name - risk of clash! Add suffix maybe?
        alias = `${cleanColName}_agg`; // Add suffix to reduce clash risk
        console.warn(
          `Applying default column name policy for ${colName}. Using alias ${alias}. Ensure no clashes.`
        );
      }
      const quotedAlias = `"${alias.replace(/"/g, '""')}"`; // Quote the final alias

      aggregations.push(`${sqlFunctionCall} AS ${quotedAlias}`);
    }
  }

  // --- 5. Construct SQL Query ---
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;

  // SELECT Clause
  const selectParts = [];
  if (quotedGroupingColumns.length > 0) {
    selectParts.push(...quotedGroupingColumns);
  }
  if (aggregations.length > 0) {
    selectParts.push(...aggregations);
  }

  // Handle case where SELECT would be empty (no group cols, no aggs) - should not happen in valid KNIME WF
  if (selectParts.length === 0) {
    return `Error: Cannot generate SELECT clause. No grouping or aggregation columns specified/successful.`;
  }

  const selectClause = `SELECT\n  ${selectParts.join(",\n  ")}`;

  // FROM Clause
  const fromClause = `FROM ${quotedPreviousNodeName}`;

  // GROUP BY Clause
  let groupByClause = "";
  if (quotedGroupingColumns.length > 0) {
    groupByClause = `GROUP BY\n  ${quotedGroupingColumns.join(",\n  ")}`;
  } else if (aggregations.length > 0) {
    // Aggregation without GROUP BY (aggregating all rows) - GROUP BY clause is omitted
    groupByClause = "";
    console.log("Performing full table aggregation (no GROUP BY clause).");
  } else {
    // Neither grouping nor aggregation columns - this state should be prevented by KNIME
    return `Error: GroupBy node has neither grouping columns nor aggregation columns configured.`;
  }

  // Handle the specific case: Grouping cols present, NO aggregations -> Use DISTINCT
  if (groupingColumns.length > 0 && aggregations.length === 0) {
    const distinctSelectClause = `SELECT DISTINCT\n  ${quotedGroupingColumns.join(
      ",\n  "
    )}`;
    return `${distinctSelectClause}\n${fromClause};`;
  } else {
    // Standard GROUP BY query (with or without grouping columns, but must have aggregations)
    const parts = [selectClause, fromClause];
    if (groupByClause) {
      // Only add GROUP BY if it was generated
      parts.push(groupByClause);
    }
    return parts.join("\n") + ";";
  }
}
