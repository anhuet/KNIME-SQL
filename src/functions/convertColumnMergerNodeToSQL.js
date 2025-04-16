// src/functions/convertColumnMergerNodeToSQL.js

/**
 * Utility function to get a value from an entry array or object (compact format).
 * @param {object|array} entryProp - The entry property which can be an object or array.
 * @param {string} key - The key to search for within the entry attributes.
 * @returns {string|boolean|null} - The value associated with the key, or null/boolean if applicable.
 */
const getEntryValue = (entryProp, key) => {
  if (!entryProp) return null;
  const entries = Array.isArray(entryProp) ? entryProp : [entryProp];
  const entry = entries.find((e) => e._attributes && e._attributes.key === key);
  if (!entry || !entry._attributes) return null;
  if (entry._attributes.isnull === "true") return null;
  if (entry._attributes.type === "xboolean")
    return entry._attributes.value === "true";
  return entry._attributes.value || null;
};

/**
 * Utility function to find a configuration node by its _attributes.key (compact format).
 * @param {object|array} config - The config node or array of nodes.
 * @param {string} key - The key to search for.
 * @returns {object|null} - The found node, or null if not found.
 */
const findConfigByKey = (config, key) => {
  if (!config) return null;
  const nodes = Array.isArray(config) ? config : [config];
  return (
    nodes.find((node) => node._attributes && node._attributes.key === key) ||
    null
  );
};

/**
 * Converts a KNIME Column Merger node configuration (compact JSON) to an SQL query.
 * Uses COALESCE to simulate the merging logic. Requires the full list of input columns.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {string} previousNodeName - The name of the table/view representing the input data.
 * @param {string[]} [inputColumnNames=[]] - Array of all column names from the input node. Crucial for handling 'Replace' options.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertColumnMergerNodeToSQL(
  nodeConfig,
  previousNodeName = "input_table",
  inputColumnNames = []
) {
  // Step 1: Verify node type
  const factory = getEntryValue(nodeConfig?.entry, "factory");
  const MERGER_FACTORY =
    "org.knime.base.node.preproc.columnmerge.ColumnMergerNodeFactory";
  if (factory !== MERGER_FACTORY) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected Column Merger node factory (${MERGER_FACTORY}), but got ${factoryInfo}.`;
  }

  // Step 2: Locate the model node
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.entry) {
    return "Error: Model configuration not found or invalid.";
  }

  // Step 3: Extract parameters
  const primaryCol = getEntryValue(modelNode.entry, "primaryColumn");
  const secondaryCol = getEntryValue(modelNode.entry, "secondaryColumn");
  const outputPlacement = getEntryValue(modelNode.entry, "outputPlacement"); // e.g., "ReplaceBoth", "ReplacePrimary", "NewColumn"
  const outputName = getEntryValue(modelNode.entry, "outputName"); // e.g., "NewColumn"

  if (!primaryCol || !secondaryCol || !outputPlacement || !outputName) {
    return "Error: Missing required parameters (primaryColumn, secondaryColumn, outputPlacement, outputName) in model configuration.";
  }

  // Step 4: Validate input columns list (needed for Replace options)
  if (
    inputColumnNames.length === 0 &&
    (outputPlacement === "ReplaceBoth" || outputPlacement === "ReplacePrimary")
  ) {
    return `Error: Cannot perform '${outputPlacement}' without the full list of input column names provided to the conversion function.`;
  }
  if (inputColumnNames.length === 0 && outputPlacement === "NewColumn") {
    console.warn(
      "Input column list is empty for Column Merger with outputPlacement='NewColumn'. Output will only contain the merged column."
    );
    // Allow proceeding, but the SELECT will be limited.
  }

  // Step 5: Construct the COALESCE expression (quoting identifiers)
  const quotedPrimary = `"${primaryCol.replace(/"/g, '""')}"`;
  const quotedSecondary = `"${secondaryCol.replace(/"/g, '""')}"`;
  const quotedOutput = `"${outputName.replace(/"/g, '""')}"`;
  const coalesceExpr = `COALESCE(${quotedPrimary}, ${quotedSecondary}) AS ${quotedOutput}`;

  // Step 6: Determine the final SELECT list based on outputPlacement
  let selectColumns = [];

  switch (outputPlacement) {
    case "ReplaceBoth":
      // Select all input columns EXCEPT primary and secondary, then add the COALESCE result
      selectColumns = inputColumnNames
        .filter((col) => col !== primaryCol && col !== secondaryCol)
        .map((col) => `"${col.replace(/"/g, '""')}"`); // Quote remaining columns
      selectColumns.push(coalesceExpr);
      break;
    case "ReplacePrimary":
      // Select all input columns EXCEPT primary, then add the COALESCE result
      selectColumns = inputColumnNames
        .filter((col) => col !== primaryCol)
        .map((col) => `"${col.replace(/"/g, '""')}"`); // Quote remaining columns
      selectColumns.push(coalesceExpr);
      break;
    case "NewColumn":
    default: // Treat unknown as NewColumn
      // Select all original input columns, plus the COALESCE result
      selectColumns = inputColumnNames.map(
        (col) => `"${col.replace(/"/g, '""')}"`
      ); // Quote all original columns
      selectColumns.push(coalesceExpr);
      // Ensure the new column name doesn't clash with existing ones (though KNIME usually prevents this)
      if (inputColumnNames.includes(outputName)) {
        console.warn(
          `Warning: Output column name '${outputName}' might clash with an existing input column.`
        );
      }
      break;
  }

  if (selectColumns.length === 0) {
    // This might happen if inputColumnNames was empty and output wasn't NewColumn
    return `Error: Could not determine columns for SELECT statement. Input columns: ${inputColumnNames.join(
      ", "
    )}, Placement: ${outputPlacement}`;
  }

  // Step 7: Build the final SQL query
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  const selectClause = selectColumns.join(",\n  ");

  const sqlQuery = `
SELECT
  ${selectClause}
FROM ${quotedPreviousNodeName};
`;

  return sqlQuery.trim();
}
