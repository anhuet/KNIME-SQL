import { getEntryValue } from "../common/getEntryValue"; // [cite: uploaded:src/common/getEntryValue.js]
import { findConfigByKey } from "../common/findConfigByKey"; // [cite: uploaded:src/common/findConfigByKey.js]

/**
 * Converts a KNIME Concatenate node configuration (compact JSON) to an SQL UNION ALL query.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {Array<object>} predecessorNodeContext - An array of context objects for each direct predecessor node.
 * Each object must contain:
 * - `nodeName`: The name to use for the input table/view in the FROM clause.
 * - `nodes`: An array of output column names (strings) for that predecessor.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertConcatenateNodeToSQL(
  nodeConfig,
  predecessorNodeContext
) {
  // --- 1. Verify Node Type ---
  const factory = getEntryValue(nodeConfig?.entry, "factory");
  const CONCATENATE_FACTORY =
    "org.knime.base.node.preproc.append.row.AppendedRowsNodeFactory";
  if (factory !== CONCATENATE_FACTORY) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected Concatenate node factory (${CONCATENATE_FACTORY}), but got ${factoryInfo}.`;
  }

  // --- 2. Check Predecessors ---
  if (
    !Array.isArray(predecessorNodeContext) ||
    predecessorNodeContext.length < 2
  ) {
    return `Error: Concatenate node requires at least two predecessors, but found ${
      predecessorNodeContext?.length || 0
    }. Context: ${JSON.stringify(predecessorNodeContext)}`;
  }

  // Validate predecessor context structure
  for (let i = 0; i < predecessorNodeContext.length; i++) {
    const pred = predecessorNodeContext[i];
    if (!pred || !pred.nodeName || !Array.isArray(pred.nodes)) {
      return `Error: Invalid predecessor context at index ${i}. Expected { nodeName: string, nodes: string[] }, got: ${JSON.stringify(
        pred
      )}`;
    }
  }

  // --- 3. Extract Model Settings ---
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode) {
    // Concatenate node might have settings directly in entry for simpler cases
    // Let's try reading directly from nodeConfig.entry as a fallback
    console.warn(
      "Model node not found directly, attempting to read settings from nodeConfig.entry."
    );
  }

  // Use modelNode?.entry if modelNode exists, otherwise fallback to nodeConfig.entry
  const configSource = modelNode?.entry || nodeConfig.entry;

  const intersectionOfColumns =
    getEntryValue(configSource, "intersection_of_columns") === true; // Default false if null/missing
  // Note: fail_on_duplicates and append_suffix relate to column name handling *before* concatenation in KNIME,
  // which SQL UNION ALL doesn't handle automatically. We assume columns are compatible.
  const failOnDuplicates = getEntryValue(configSource, "fail_on_duplicates");
  const appendSuffix = getEntryValue(configSource, "append_suffix");

  let sqlComment = `-- Concatenate Node Conversion (using UNION ALL)\n`;
  if (intersectionOfColumns) {
    sqlComment += `-- Mode: Intersection of columns\n`;
  } else {
    sqlComment += `-- Mode: Union of columns (default)\n`;
  }
  if (failOnDuplicates !== null || appendSuffix !== null) {
    sqlComment += `-- Note: KNIME options 'fail_on_duplicates' and 'append_suffix' for column names are not directly translated. SQL UNION ALL requires columns in SELECT lists to align.\n`;
  }

  // --- 4. Determine Final Output Columns ---
  let finalOutputColumns = [];
  const allInputColumnSets = predecessorNodeContext.map(
    (pred) => new Set(pred.nodes)
  );

  if (intersectionOfColumns) {
    // Start with columns from the first predecessor
    if (allInputColumnSets.length > 0) {
      let intersection = new Set(allInputColumnSets[0]);
      // Iterate through the rest, keeping only common columns
      for (let i = 1; i < allInputColumnSets.length; i++) {
        intersection = new Set(
          [...intersection].filter((col) => allInputColumnSets[i].has(col))
        );
      }
      finalOutputColumns = [...intersection];
    }
    if (finalOutputColumns.length === 0) {
      return "Error: Intersection of columns resulted in an empty column set.";
    }
  } else {
    // Union of columns
    const unionSet = new Set();
    allInputColumnSets.forEach((colSet) => {
      colSet.forEach((col) => unionSet.add(col));
    });
    finalOutputColumns = [...unionSet];
  }

  // Sort columns alphabetically for consistent order in SQL
  finalOutputColumns.sort();

  if (finalOutputColumns.length === 0) {
    return "Error: No columns determined for the output.";
  }

  // --- 5. Construct UNION ALL Parts ---
  const unionParts = [];
  for (let i = 0; i < predecessorNodeContext.length; i++) {
    const pred = predecessorNodeContext[i];
    const predColumns = allInputColumnSets[i]; // Use the Set for faster lookup
    const quotedPredName = `"${pred.nodeName.replace(/"/g, '""')}"`;

    const selectList = finalOutputColumns
      .map((col) => {
        const quotedCol = `"${col.replace(/"/g, '""')}"`;
        if (predColumns.has(col)) {
          // Column exists in this predecessor
          return `${quotedPredName}.${quotedCol}`;
        } else {
          // Column does not exist, select NULL
          // Note: Assumes compatible data types for UNION ALL.
          // We don't know the type here, so just use NULL.
          return `NULL AS ${quotedCol}`;
        }
      })
      .join(",\n    ");

    const selectStatement = `SELECT\n    ${selectList}\n  FROM ${quotedPredName}`;
    unionParts.push(selectStatement);
  }

  // --- 6. Combine and Return SQL ---
  const finalSql = sqlComment + unionParts.join("\nUNION ALL\n");

  return finalSql.trim() + ";";
}
