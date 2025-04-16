/**
 * Utility function to get a value from an array of entry objects
 * based on the provided JSON structure.
 * @param {Array<object>} entryArray - The array of entry objects (e.g., [{_key: 'k', _value: 'v', ...}]).
 * @param {string} key - The _key to search for.
 * @returns {string|boolean|null} - The value associated with the key, or null if not found. Handles boolean/null.
 */
const getEntryValueFromJSON = (entryArray, key) => {
  if (!Array.isArray(entryArray)) return null;
  const entry = entryArray.find((e) => e._key === key);
  if (!entry) return null;

  if (entry._isnull === "true") {
    return null;
  }
  if (entry._type === "xboolean") {
    return entry._value === "true";
  }
  return entry._value;
};

/**
 * Utility function to find a config object within an array of config objects
 * based on the provided JSON structure.
 * @param {Array<object>} configArray - The array of config objects (e.g., [{_key: 'k', config: [...], entry: [...]}]).
 * @param {string} key - The _key to search for.
 * @returns {object|null} - The found config object, or null if not found.
 */
const findConfigByKeyFromJSON = (configArray, key) => {
  if (!Array.isArray(configArray)) return null;
  return configArray.find((c) => c._key === key) || null;
};

/**
 * Extracts the list of column names from the 'group_cols' config object
 * based on the provided JSON structure.
 * @param {object} groupColsConfig - The config object with _key 'group_cols'.
 * @returns {string[]} - An array of column names.
 */
const getColumnsFromGroupColsJSON = (groupColsConfig) => {
  if (!groupColsConfig || !Array.isArray(groupColsConfig.config)) {
    console.warn("'group_cols' config node not found or invalid.");
    return [];
  }

  const filterType = getEntryValueFromJSON(
    groupColsConfig.entry,
    "filter-type"
  );
  const includedNamesConfig = findConfigByKeyFromJSON(
    groupColsConfig.config,
    "included_names"
  );

  if (
    filterType === "STANDARD" &&
    includedNamesConfig &&
    Array.isArray(includedNamesConfig.entry)
  ) {
    // Filter out the 'array-size' entry and return the values of the rest
    return includedNamesConfig.entry
      .filter((e) => e._key !== "array-size")
      .map((e) => e._value)
      .filter((name) => name); // Ensure names are not null/empty
  } else {
    console.warn(
      `Filter type '${filterType}' or complex config requires specific handling.`
    );
    return []; // Fallback for non-standard or unhandled cases
  }
};

/**
 * Converts a KNIME Duplicate Row Filter node configuration (as specific JSON) to an SQL query.
 * Handles duplicates based on SELECTED columns as per the configuration.
 *
 * @param {object} nodeJsonConfig - The full node configuration object in the provided JSON format.
 * @param {string} previousNodeName - The name of the table/view representing the input data.
 * @param {string[]} [inputColumns=[]] - Optional: Array of all column names from the input. Needed if no columns are explicitly included.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertDuplicateRowFilterJSONToSQL(
  nodeJsonConfig,
  previousNodeName,
  inputColumns = []
) {
  // Expecting nodeJsonConfig to be the outer { config: { entry: [...], config: [...] } } object

  if (
    !nodeJsonConfig ||
    !nodeJsonConfig.config ||
    !Array.isArray(nodeJsonConfig.config.entry) ||
    !Array.isArray(nodeJsonConfig.config.config)
  ) {
    return "Error: Invalid JSON structure passed. Expected top-level 'config' object with 'entry' and 'config' arrays.";
  }

  // Step 1: Verify node type using the adapted helper
  const factory = getEntryValueFromJSON(nodeJsonConfig.config.entry, "factory");
  const DUP_FILTER_FACTORY =
    "org.knime.base.node.preproc.duplicates.DuplicateRowFilterNodeFactory";

  if (factory !== DUP_FILTER_FACTORY) {
    const factoryInfo =
      factory === null
        ? "null"
        : factory === undefined
        ? "undefined"
        : `"${factory}"`;
    return `Error: Expected Duplicate Row Filter node factory (${DUP_FILTER_FACTORY}), but got ${factoryInfo}.`;
  }

  // Step 2: Find the model configuration object using the adapted helper
  const modelConfig = findConfigByKeyFromJSON(
    nodeJsonConfig.config.config,
    "model"
  );
  if (
    !modelConfig ||
    !Array.isArray(modelConfig.config) ||
    !Array.isArray(modelConfig.entry)
  ) {
    return "Error: Model configuration ('config' with _key='model') not found or invalid in JSON.";
  }

  // Step 3: Extract filtering parameters from the modelConfig
  const removeDuplicates = getEntryValueFromJSON(
    modelConfig.entry,
    "remove_duplicates"
  );
  const rowSelection = getEntryValueFromJSON(
    modelConfig.entry,
    "row_selection"
  );
  const groupColsConfig = findConfigByKeyFromJSON(
    modelConfig.config,
    "group_cols"
  );

  if (removeDuplicates === null || rowSelection === null || !groupColsConfig) {
    return "Error: Could not find 'remove_duplicates', 'row_selection', or 'group_cols' settings within the model config.";
  }

  if (!removeDuplicates) {
    return `-- SQL Conversion Note: Duplicate Row Filter node is not configured to remove duplicates.\nSELECT * FROM ${previousNodeName};`;
  }

  // Step 4: Determine columns to partition by using the adapted helper
  let partitionColumns = getColumnsFromGroupColsJSON(groupColsConfig);

  // THIS IS THE CHECK FOR "ALL COLUMNS" SCENARIO
  if (partitionColumns.length === 0) {
    // If no columns are explicitly included, check if inputColumns are provided (for 'all columns' case)
    if (inputColumns && inputColumns.length > 0) {
      console.warn(
        "No specific columns selected for duplicate check; using all provided input columns."
      );
      partitionColumns = inputColumns; // Use all available columns
    } else {
      // Cannot determine partition columns if none selected and input list not provided
      return `Error: Could not determine columns for duplicate check. Either explicitly select columns in KNIME or provide the full input column list to the conversion function for 'all columns' mode.`;
    }
  }

  const partitionByClause = partitionColumns
    .map((col) => `"${col}"`)
    .join(", ");
  if (!partitionByClause) {
    return `Error: Failed to construct PARTITION BY clause. No valid columns found.`;
  }

  // Step 5: Determine ORDER BY for ROW_NUMBER()
  const orderByDirection = rowSelection === "LAST" ? "DESC" : "ASC";
  const orderByClause = partitionColumns
    .map((col) => `"${col}" ${orderByDirection}`)
    .join(", ");

  // Step 6: Construct the SQL query
  const sqlQuery = `
-- Note: Keeps the '${
    rowSelection || "FIRST"
  }' row based on ordering by partition columns (${orderByDirection}).
-- Duplicates are checked based on columns: ${partitionColumns.join(", ")}
WITH RankedRows AS (
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY ${partitionByClause} ORDER BY ${orderByClause}) as knime_row_number
FROM ${previousNodeName}
)
SELECT
* -- Selects all original columns + knime_row_number.
FROM RankedRows
WHERE knime_row_number = 1;
`;

  return sqlQuery.trim();
}
