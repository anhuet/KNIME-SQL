// src/functions/convertStringToNumberNodeToSQL.js

// Import necessary helper functions from the common directory
import { getEntryValue } from "../common/getEntryValue"; // [cite: uploaded:src/common/getEntryValue.js]
import { findConfigByKey } from "../common/findConfigByKey"; // [cite: uploaded:src/common/findConfigByKey.js]
import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig"; // [cite: uploaded:src/common/getArrayValuesFromConfig.js]

/**
 * Maps KNIME cell class strings to corresponding SQL data types.
 * @param {string} knimeCellClass - The cell class string (e.g., "org.knime.core.data.def.DoubleCell").
 * @returns {string} - The corresponding SQL data type (e.g., "DOUBLE PRECISION"). Returns "VARCHAR" as fallback.
 */
const mapKnimeTypeToSQL = (knimeCellClass) => {
  if (!knimeCellClass) return "VARCHAR"; // Default fallback

  if (knimeCellClass.includes("DoubleCell")) {
    return "DOUBLE PRECISION"; // Or NUMERIC, FLOAT depending on dialect/precision needs
  } else if (knimeCellClass.includes("IntCell")) {
    return "INTEGER";
  } else if (knimeCellClass.includes("LongCell")) {
    return "BIGINT";
  }
  // Add mappings for other numeric types if needed (e.g., BigDecimalCell -> NUMERIC(p,s))
  else {
    console.warn(
      `Unsupported KNIME cell class for numeric conversion: ${knimeCellClass}. Defaulting to VARCHAR.`
    );
    return "VARCHAR"; // Fallback if it's not a recognized numeric type
  }
};

/**
 * Finds the direct predecessor node(s) for a given node ID from a list of processed nodes.
 * @param {number} currentNodeId - The ID of the current node.
 * @param {Array<object>} allProcessedNodes - An array of node objects that have already been processed,
 * each expected to have an 'id' and 'nextNodes' property.
 * @returns {Array<object>} - An array of predecessor node objects.
 */
const findPredecessorNodes = (currentNodeId, allProcessedNodes) => {
  if (!Array.isArray(allProcessedNodes)) return [];
  return allProcessedNodes.filter(
    (node) =>
      node &&
      Array.isArray(node.nextNodes) &&
      node.nextNodes.includes(currentNodeId)
  );
};

/**
 * Converts a KNIME String to Number node configuration (compact JSON) to an SQL query.
 * This version attempts to derive input columns from predecessor nodes.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * MUST include an 'id' property representing the node's workflow ID.
 * @param {string} previousNodeName - The *logical* name of the table/view representing the input data
 * (often derived from the predecessor node's name or ID).
 * @param {Array<object>} allProcessedNodes - An array containing the processed data of all nodes
 * executed *before* this one. Each object should include at least 'id', 'nextNodes', and 'nodes' (output columns).
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertStringToNumberNodeToSQL(
  nodeConfig,
  previousNodeName = "input_table",
  allProcessedNodes,
  id // Expects array of processed nodes {id: number, nextNodes: number[], nodes: string[]}
) {
  console.log(nodeConfig, "nodeConfig");
  // Step 1: Verify node type and get current node ID
  const currentNodeId = id; // Assuming nodeConfig has the ID added during processing

  // Step 1.5: Find Predecessor and derive Input Columns
  const predecessors = findPredecessorNodes(currentNodeId, allProcessedNodes);
  let inputColumnNames = null;

  if (predecessors.length === 0) {
    // This might happen for nodes directly after a reader or if context is incomplete
    console.warn(
      `Node ${currentNodeId} (String to Number): No predecessors found in provided context. Cannot determine input columns accurately.`
    );
    // Depending on strictness, could return error or proceed assuming no input columns (unlikely for this node)
    return `Error: Node ${currentNodeId} (String to Number): No predecessors found. Cannot determine input columns.`;
  } else if (predecessors.length > 1) {
    // String to Number typically has only one input port
    console.warn(
      `Node ${currentNodeId} (String to Number): Found multiple predecessors (${predecessors
        .map((p) => p.id)
        .join(", ")}). Using columns from the first one found (${
        predecessors[0].id
      }).`
    );
    // Need a strategy here - maybe merge columns? For now, use the first.
    inputColumnNames = predecessors[0].nodes; // Get output columns from the first predecessor
  } else {
    // Exactly one predecessor found
    inputColumnNames = predecessors[0].nodes; // Get output columns from the predecessor
  }

  if (!Array.isArray(inputColumnNames) || inputColumnNames.length === 0) {
    return `Error: Node ${currentNodeId} (String to Number): Predecessor node ${predecessors[0]?.id} has no output columns defined in the provided context.`;
  }

  // Step 2: Locate the model node
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config || !modelNode.entry) {
    return "Error: Model configuration not found or invalid.";
  }

  // Step 3: Extract parameters
  const failOnError = getEntryValue(modelNode.entry, "fail_on_error"); // boolean

  // Find the column filter config
  let columnFilterConfig = null;
  const modelConfigs = Array.isArray(modelNode.config)
    ? modelNode.config
    : [modelNode.config];
  for (const config of modelConfigs) {
    if (findConfigByKey(config.config, "included_names")) {
      columnFilterConfig = config;
      break;
    }
  }

  if (!columnFilterConfig) {
    return "Error: Could not find the column filter configuration within the model.";
  }

  const includedNamesNode = findConfigByKey(
    columnFilterConfig.config,
    "included_names"
  );
  const columnsToConvert = getArrayValuesFromConfig(includedNamesNode);

  if (!columnsToConvert || columnsToConvert.length === 0) {
    return "Error: No columns specified for conversion in 'included_names'.";
  }

  const parseTypeNode = findConfigByKey(modelNode.config, "parse_type");
  const knimeTargetType = getEntryValue(parseTypeNode?.entry, "cell_class");

  if (!knimeTargetType) {
    return "Error: Could not determine target data type from 'parse_type' configuration.";
  }

  // Step 4: Determine SQL Type and Cast Function
  const sqlTargetType = mapKnimeTypeToSQL(knimeTargetType);
  const castFunction = failOnError ? "CAST" : "TRY_CAST";

  // Step 5: Build the SELECT clause (Now requires derived inputColumnNames)
  const selectParts = [];
  const columnsToConvertSet = new Set(columnsToConvert);
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;

  // Iterate through the derived input columns
  inputColumnNames.forEach((col) => {
    const quotedCol = `"${col.replace(/"/g, '""')}"`;
    if (columnsToConvertSet.has(col)) {
      // This column needs conversion
      selectParts.push(
        `${castFunction}(${quotedCol} AS ${sqlTargetType}) AS ${quotedCol}`
      );
    } else {
      // This column is passed through
      selectParts.push(quotedCol);
    }
  });

  if (selectParts.length === 0) {
    return `Error: Node ${currentNodeId} (String to Number): Failed to generate SELECT clause. Input columns might be empty or invalid.`;
  }

  const selectClause = `SELECT\n  ${selectParts.join(",\n  ")}`;

  // Step 6: Construct the final SQL query
  const sqlQuery = `
${selectClause}
FROM ${quotedPreviousNodeName};
-- Node ID: ${currentNodeId}
-- Converted columns: ${columnsToConvert.join(
    ", "
  )} to ${sqlTargetType} using ${castFunction}
-- Input columns derived from predecessor(s): ${predecessors
    .map((p) => p.id)
    .join(", ")}
`;

  return sqlQuery.trim();
}
