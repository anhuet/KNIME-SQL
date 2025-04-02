/**
 * Converts a KNIME Column Filter node configuration to an SQL query.
 *
 * The configuration is expected to have the following properties:
 * - included_names: an array of column names to include in the output.
 * - excluded_names: an array of column names to exclude from the output.
 *
 * Example SQL output:
 * SELECT country, date, amount FROM table_name;
 *
 * @param {object} nodeConfig - The full node configuration object.
 * @param {string} previousNodeName - The name of the previous node (for table name).
 * @returns {string} - The generated SQL query or an error message if something is missing.
 */
export function convertColumnFilterNodeToSQL(nodeConfig, previousNodeName) {
  // Step 1: Ensure the node is a Column Filter by checking the "factory" entry.
  const factory = getEntryValue(nodeConfig, "factory");
  const COLUMN_FILTER_FACTORY =
    "org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory";
  if (factory !== COLUMN_FILTER_FACTORY) {
    return "This function only converts Column Filter nodes.";
  }

  // Step 2: Locate the "model" node from the top-level config.
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config) {
    return "Model node not found in the configuration.";
  }

  // Step 3: Locate the "column-filter" node within the model.
  const columnFilterNode = findConfigByKey(modelNode.config, "column-filter");
  if (!columnFilterNode || !columnFilterNode.config) {
    return "Column filter configuration not found.";
  }

  // Step 4: Extract included and excluded column names.
  const includedNamesNode = findConfigByKey(
    columnFilterNode.config,
    "included_names"
  );
  const excludedNamesNode = findConfigByKey(
    columnFilterNode.config,
    "excluded_names"
  );

  const includedColumns = [];
  const excludedColumns = [];

  console.log(includedNamesNode);
  // Extract included column names
  if (includedNamesNode && includedNamesNode.entry) {
    const includedEntries = Array.isArray(includedNamesNode.entry)
      ? includedNamesNode.entry
      : [includedNamesNode.entry];
    console.log(includedEntries, "includedEntries");
    includedEntries.forEach((entry) => {
      if (entry._attributes.key !== "array-size") {
        includedColumns.push(entry._attributes.value);
      }
    });
  }

  // Extract excluded column names
  if (excludedNamesNode && excludedNamesNode.entry) {
    const excludedEntries = Array.isArray(excludedNamesNode.entry)
      ? excludedNamesNode.entry
      : [excludedNamesNode.entry];
    excludedEntries.forEach((entry) => {
      if (entry._attributes.key !== "array-size") {
        excludedColumns.push(entry._attributes.value);
      }
    });
  }

  console.log(includedColumns, excludedColumns);
  // Step 5: Build the SQL query
  // Filter out excluded columns from included columns
  const finalColumns = includedColumns.filter(
    (col) => !excludedColumns.includes(col)
  );

  // Create the SELECT clause
  const selectClause = finalColumns.length > 0 ? finalColumns.join(", ") : "*"; // Fallback to * if no columns are included
  const fromClause = previousNodeName || "input_table"; // Use previous node name or default

  return `SELECT ${selectClause} FROM ${fromClause};`;
}

// Utility function to get a value from an entry array.
const getEntryValue = (data, key) => {
  if (!data?.entry) return "";
  const entries = Array.isArray(data.entry) ? data.entry : [data.entry];
  const entry = entries.find((e) => e._attributes.key === key);
  return entry?._attributes?.value || "";
};

// Utility function to find a configuration node by its _attributes.key.
const findConfigByKey = (config, key) => {
  if (!config) return null;
  if (Array.isArray(config)) {
    return config.find(
      (node) => node._attributes && node._attributes.key === key
    );
  } else if (config._attributes && config._attributes.key === key) {
    return config;
  }
  return null;
};
