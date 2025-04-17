// src/functions/convertSorterNodeToSQL.js

import { getEntryValue } from "../common/getEntryValue"; // [cite: uploaded:src/common/getEntryValue.js]
import { findConfigByKey } from "../common/findConfigByKey"; // [cite: uploaded:src/common/findConfigByKey.js]

/**
 * Converts a KNIME Sorter node configuration (compact JSON) to an SQL query
 * using the ORDER BY clause.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {string} previousNodeName - The name of the table/view representing the input data.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertSorterNodeToSQL(
  nodeConfig,
  previousNodeName = "input_table"
) {
  // --- 1. Verify Node Type ---
  const factory = getEntryValue(nodeConfig?.entry, "factory");
  const SORTER_FACTORY = "org.knime.base.node.preproc.sorter.SorterNodeFactory";
  if (factory !== SORTER_FACTORY) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected Sorter node factory (${SORTER_FACTORY}), but got ${factoryInfo}.`;
  }

  // --- 2. Locate Model ---
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config || !modelNode.entry) {
    return "Error: Model configuration not found or invalid.";
  }

  // --- 3. Extract Sorting Criteria ---
  const sortingCriteriaNode = findConfigByKey(modelNode.config, "sortingCriteria");
  if (!sortingCriteriaNode || !sortingCriteriaNode.config) {
    // It's possible to have a Sorter node configured without criteria (no-op)
    console.warn("Sorter node has no sorting criteria defined. Outputting data as is.");
    return `SELECT * FROM "${previousNodeName.replace(/"/g, '""')}"; -- Warning: No sorting criteria found`;
  }

  const criteriaConfigs = Array.isArray(sortingCriteriaNode.config)
    ? sortingCriteriaNode.config
    : [sortingCriteriaNode.config]; // Ensure it's an array

  const orderByParts = [];
  const missingToEnd = getEntryValue(modelNode.entry, "missingToEnd"); // boolean: true -> NULLS LAST, false -> NULLS FIRST

  for (const criterionConfig of criteriaConfigs) {
    // Skip if it's not a valid config object (might have other entries sometimes)
    if (!criterionConfig || !criterionConfig._attributes || isNaN(parseInt(criterionConfig._attributes.key))) {
        continue;
    }

    const columnNode = findConfigByKey(criterionConfig.config, "column");
    const columnName = getEntryValue(columnNode?.entry, "selected");
    const sortingOrder = getEntryValue(criterionConfig.entry, "sortingOrder"); // ASCENDING or DESCENDING
    const stringComparison = getEntryValue(criterionConfig.entry, "stringComparison"); // NATURAL or ALPHANUMERIC - Often handled by DB

    if (!columnName || !sortingOrder) {
      console.warn(`Skipping incomplete sorting criterion: ${JSON.stringify(criterionConfig)}`);
      continue;
    }

    const direction = sortingOrder === "DESCENDING" ? "DESC" : "ASC";
    const nullsOrder = missingToEnd === true ? "NULLS LAST" : "NULLS FIRST";
    const quotedColumnName = `"${columnName.replace(/"/g, '""')}"`;

    // Note: stringComparison ('NATURAL' vs 'ALPHANUMERIC') is complex to replicate perfectly
    // in standard SQL without database-specific functions (COLLATE) or casting.
    // Standard ORDER BY usually provides behavior close to 'NATURAL' for appropriate types.
    // We'll add a comment but won't add complex collation logic here for general compatibility.
    let comment = "";
    if (stringComparison === 'ALPHANUMERIC') {
        comment = ` -- Note: Alphanumeric string comparison requested for ${columnName}. Standard ORDER BY used; verify behavior with DB.`
    }

    orderByParts.push(`${quotedColumnName} ${direction} ${nullsOrder}${comment}`);
  }

  if (orderByParts.length === 0) {
    console.warn("Sorter node criteria configs found, but none were valid. Outputting data as is.");
    return `SELECT * FROM "${previousNodeName.replace(/"/g, '""')}"; -- Warning: No valid sorting criteria processed`;
  }

  // --- 4. Construct SQL Query ---
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  // Sorter node passes through all columns
  const selectClause = `SELECT *`;
  const fromClause = `FROM ${quotedPreviousNodeName}`;
  const orderByClause = `ORDER BY\n  ${orderByParts.join(",\n  ")}`;

  return `${selectClause}\n${fromClause}\n${orderByClause};`;
}