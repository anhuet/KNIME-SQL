/**
 * Utility function to get a value from an entry array or object.
 * Needed for parsing KNIME node configurations.
 * @param {object|array} entryProp - The entry property which can be an object or array.
 * @param {string} key - The key to search for within the entry attributes.
 * @returns {string|null} - The value associated with the key, or null if not found.
 */
const getEntryValue = (entryProp, key) => {
  if (!entryProp) return null;
  const entries = Array.isArray(entryProp) ? entryProp : [entryProp];
  const entry = entries.find((e) => e._attributes && e._attributes.key === key);
  // Handle boolean values specifically
  if (entry?._attributes?.type === "xboolean") {
    return entry._attributes.value === "true";
  }
  return entry?._attributes?.value || null;
};

/**
 * Utility function to find a configuration node by its _attributes.key.
 * The "config" parameter can be an array or a single node.
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
 * Maps KNIME comparison operators found in XML to SQL operators/functions.
 * @param {string} knimeOperator - The operator string from KNIME XML (e.g., "EQ", "NEQ", "LIKE", "REGEX").
 * @returns {string} - The corresponding SQL operator or function keyword (e.g., "=", "!=", "LIKE", "REGEXP").
 */
const mapKnimeOperatorToSQL = (knimeOperator) => {
  switch (knimeOperator) {
    case "EQ":
      return "=";
    case "NEQ":
      return "!="; // Or '<>'
    case "LT":
      return "<";
    case "LE":
      return "<=";
    case "GT":
      return ">";
    case "GE":
      return ">=";
    case "LIKE":
      return "LIKE";
    case "REGEX":
      return "REGEXP"; // Adjust if needed for specific DB
    case "IS_MISSING":
      return "IS NULL";
    case "IS_NOT_MISSING":
      return "IS NOT NULL";
    default:
      console.warn(
        `Unsupported KNIME operator: ${knimeOperator}. Defaulting to '='.`
      );
      return "=";
  }
};

/**
 * Translates KNIME wildcard patterns (*, ?) to SQL LIKE patterns (%, _).
 * Also escapes existing SQL wildcards in the value itself.
 * @param {string} knimePattern - The pattern string from KNIME using * and ?.
 * @returns {string} - The SQL LIKE pattern string.
 */
const translateKnimeWildcardToSQL = (knimePattern) => {
  if (typeof knimePattern !== "string") return ""; // Handle non-string input
  let sqlPattern = knimePattern.replace(/%/g, "\\%").replace(/_/g, "\\_");
  sqlPattern = sqlPattern.replace(/\*/g, "%").replace(/\?/g, "_");
  return sqlPattern;
};

/**
 * Converts a KNIME Row Filter node configuration (as JSON) to an SQL query.
 *
 * @param {object} nodeConfig - The full node configuration object (converted from settings.xml).
 * @param {string} previousNodeName - The name of the table/view representing the input data for this node.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertRowFilterNodeToSQL(nodeConfig, previousNodeName) {
  // Step 1: Verify node type
  const factory = getEntryValue(nodeConfig.entry, "factory");
  const ROW_FILTER_FACTORY =
    "org.knime.base.node.preproc.filter.row3.RowFilterNodeFactory";
  if (factory !== ROW_FILTER_FACTORY) {
    return `Error: Expected Row Filter node factory (${ROW_FILTER_FACTORY}), but got ${
      factory || "N/A"
    }.`;
  }

  // Step 2: Find the model configuration
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config) {
    return "Error: Model configuration not found.";
  }

  // Step 3: Extract filtering parameters
  const outputMode = getEntryValue(modelNode.entry, "outputMode");
  const matchCriteria = getEntryValue(modelNode.entry, "matchCriteria");
  const predicatesNode = findConfigByKey(modelNode.config, "predicates");

  if (
    !outputMode ||
    !matchCriteria ||
    !predicatesNode ||
    !predicatesNode.config
  ) {
    return "Error: Essential filtering parameters (outputMode, matchCriteria, predicates) not found.";
  }

  const predicateConfigs = Array.isArray(predicatesNode.config)
    ? predicatesNode.config
    : [predicatesNode.config];

  // Step 4: Build the WHERE clause conditions from predicates
  const conditions = predicateConfigs
    .map((predConfig) => {
      if (!predConfig || !predConfig.config) return null;

      const columnNode = findConfigByKey(predConfig.config, "column");
      const operatorEntry = findConfigByKey(predConfig.entry, "operator");
      const predicateValuesNode = findConfigByKey(
        predConfig.config,
        "predicateValues"
      );

      if (
        !columnNode ||
        !operatorEntry ||
        !predicateValuesNode ||
        !predicateValuesNode.config
      ) {
        console.warn(
          "Skipping predicate due to missing column, operator, or predicateValues configuration."
        );
        return null;
      }

      const columnName = getEntryValue(columnNode.entry, "selected");
      const knimeOperator = operatorEntry._attributes.value;
      const valuesNode = findConfigByKey(predicateValuesNode.config, "values");

      if (!columnName || !knimeOperator || !valuesNode || !valuesNode.config) {
        console.warn(
          "Skipping predicate due to missing column name, operator value, or values configuration."
        );
        return null;
      }

      // Handle operators that don't need a value first
      const sqlOperator = mapKnimeOperatorToSQL(knimeOperator);
      if (sqlOperator === "IS NULL" || sqlOperator === "IS NOT NULL") {
        const quotedColumnName = `"${columnName.replace(/"/g, '""')}"`;
        return `${quotedColumnName} ${sqlOperator}`;
      }

      // --- START: FIX for valueConfig structure ---
      // Now handle operators that require a value
      // valuesNode.config might be an object { "_attributes": { "key": "0" }, config: ..., entry: ... }
      // or an array of such objects if multiple values are possible (e.g., for IN operator)
      // We'll assume the common case of a single value config under key "0" based on your example.
      const singleValueConfig = findConfigByKey(valuesNode.config, "0"); // Find the config keyed "0"

      if (
        !singleValueConfig ||
        !singleValueConfig.entry ||
        !singleValueConfig.config
      ) {
        console.warn(
          `Skipping predicate for column "${columnName}" due to missing or invalid value configuration under key '0'.`
        );
        return null;
      }

      // Extract value from the 'entry' property of singleValueConfig
      const value = getEntryValue(singleValueConfig.entry, "value");

      // Extract type info from the 'config' property (typeIdentifier) of singleValueConfig
      const typeIdentifierConfig = findConfigByKey(
        singleValueConfig.config,
        "typeIdentifier"
      );
      const isNull = getEntryValue(typeIdentifierConfig?.entry, "is_null"); // is_null is boolean
      const cellClass = getEntryValue(
        typeIdentifierConfig?.entry,
        "cell_class"
      );

      // --- END: FIX for valueConfig structure ---

      // Check if the value itself represents NULL (though IS_MISSING should handle this)
      if (isNull === true) {
        // Explicit boolean check
        const quotedColumnName = `"${columnName.replace(/"/g, '""')}"`;
        return `${quotedColumnName} IS NULL`;
      }

      // Value extraction should handle different types (string, int, etc.)
      if (value === null || value === undefined) {
        console.warn(
          `Skipping predicate for column "${columnName}" because extracted value is null or undefined.`
        );
        return null; // Cannot compare against null like this, IS NULL/IS NOT NULL should be used.
      }

      let sqlValue = "";
      let condition = "";
      const quotedColumnName = `"${columnName.replace(/"/g, '""')}"`;

      // Determine if the value needs quotes based on cellClass or failing Number conversion
      const isStringType = cellClass?.includes("StringCell");
      const isNumeric = !isNaN(Number(value)); // Check if value *can* be treated as a number
      const needsQuotes = isStringType || !isNumeric; // Quote if KNIME says string OR if it's not a number

      // Prepare the value based on operator type and quoting needs
      if (sqlOperator === "LIKE") {
        sqlValue = `'${translateKnimeWildcardToSQL(value).replace(
          /'/g,
          "''"
        )}'`;
      } else if (sqlOperator === "REGEXP") {
        sqlValue = `'${value.replace(/'/g, "''")}'`;
      } else if (needsQuotes) {
        sqlValue = `'${value.replace(/'/g, "''")}'`;
      } else {
        sqlValue = value; // Numeric value, no quotes
      }

      // Handle case sensitivity (relevant for string types)
      const caseSensitiveConfig = findConfigByKey(
        singleValueConfig.config,
        "stringCaseMatching"
      );
      const caseSensitive =
        getEntryValue(caseSensitiveConfig?.entry, "caseMatching") ===
        "CASESENSITIVE";

      if (
        needsQuotes &&
        !caseSensitive &&
        (sqlOperator === "=" ||
          sqlOperator === "!=" ||
          sqlOperator === "LIKE" ||
          sqlOperator === "REGEXP")
      ) {
        // Apply LOWER for case-insensitive comparison on strings
        condition = `LOWER(${quotedColumnName}) ${sqlOperator} LOWER(${sqlValue})`;
        if (sqlOperator === "REGEXP") {
          console.warn(
            `Case-insensitive REGEXP for "${columnName}" using LOWER(). Verify compatibility/syntax with your SQL dialect.`
          );
        }
      } else {
        // Case-sensitive string comparison or numeric comparison
        condition = `${quotedColumnName} ${sqlOperator} ${sqlValue}`;
      }

      // Add ESCAPE clause for LIKE if necessary
      if (
        sqlOperator === "LIKE" &&
        translateKnimeWildcardToSQL(value) !==
          value.replace(/\*/g, "%").replace(/\?/g, "_")
      ) {
        condition += " ESCAPE '\\'";
      }

      return condition;
    })
    .filter((condition) => condition !== null);

  if (conditions.length === 0) {
    console.warn("No valid filter conditions generated from predicates.");
    const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
    return `SELECT * FROM ${quotedPreviousNodeName}; -- Warning: No valid filter conditions generated or applied`;
  }

  // Step 5: Combine conditions
  const combinedConditions = conditions.join(` ${matchCriteria} `);

  // Step 6: Apply outputMode
  const whereClause =
    outputMode === "NON_MATCHING"
      ? `NOT (${combinedConditions})`
      : combinedConditions;

  // Step 7: Construct final SQL query
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  const sqlQuery = `SELECT * FROM ${quotedPreviousNodeName} WHERE ${whereClause};`;

  return sqlQuery;
}
