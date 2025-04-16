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
    case "LIKE": // Handles "Matches wildcard"
      return "LIKE";
    case "REGEX": // Handles "Matches regex" - Note: SQL function varies by dialect (REGEXP, RLIKE, REGEXP_LIKE)
      return "REGEXP"; // Using REGEXP as a common default, adjust if needed for specific DB
    case "IS_MISSING":
      return "IS NULL";
    case "IS_NOT_MISSING":
      return "IS NOT NULL";
    // Add other operators if encountered (e.g., STARTS_WITH, ENDS_WITH -> could use LIKE)
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
  // 1. Escape existing SQL wildcards (_, %) in the original string
  let sqlPattern = knimePattern.replace(/%/g, "\\%").replace(/_/g, "\\_");
  // 2. Translate KNIME wildcards (*, ?) to SQL wildcards (%, _)
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
  const outputMode = getEntryValue(modelNode.entry, "outputMode"); // "MATCHING" or "NON_MATCHING"
  const matchCriteria = getEntryValue(modelNode.entry, "matchCriteria"); // "AND" or "OR"
  const predicatesNode = findConfigByKey(modelNode.config, "predicates");

  if (
    !outputMode ||
    !matchCriteria ||
    !predicatesNode ||
    !predicatesNode.config
  ) {
    return "Error: Essential filtering parameters (outputMode, matchCriteria, predicates) not found in the model.";
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
        return `"${columnName}" ${sqlOperator}`;
      }

      // Now handle operators that require a value
      const valueConfig = findConfigByKey(valuesNode.config, "0"); // Assuming index '0' for single value
      if (!valueConfig || !valueConfig.entry) {
        console.warn(
          `Skipping predicate for column "${columnName}" due to missing value configuration.`
        );
        return null;
      }

      const value = getEntryValue(valueConfig.entry, "value");
      const isNull =
        getEntryValue(
          valueConfig.config?.find(
            (c) => c._attributes.key === "typeIdentifier"
          )?.entry,
          "is_null"
        ) === "true";

      // Check if the value itself represents NULL (though IS_MISSING should handle this)
      if (isNull) {
        return `"${columnName}" IS NULL`; // Or potentially IS NOT NULL depending on operator? Seems unlikely.
      }

      let sqlValue = "";
      let condition = "";

      // Determine if the value is inherently string-like from KNIME's perspective
      const isStringType = valueConfig.config
        ?.find((c) => c._attributes.key === "typeIdentifier")
        ?.entry?.find((e) => e._attributes.key === "cell_class")
        ?._attributes?.value?.includes("StringCell");
      const needsQuotes = isStringType || isNaN(Number(value)); // Quote if KNIME says string OR if it's not a valid number

      // Prepare the value based on operator type
      if (sqlOperator === "LIKE") {
        // Translate KNIME wildcards and escape for SQL LIKE
        sqlValue = `'${translateKnimeWildcardToSQL(value).replace(
          /'/g,
          "''"
        )}'`;
      } else if (sqlOperator === "REGEXP") {
        // Just quote the regex string, escaping internal single quotes
        sqlValue = `'${value.replace(/'/g, "''")}'`;
      } else if (needsQuotes) {
        // Standard string quoting
        sqlValue = `'${value.replace(/'/g, "''")}'`;
      } else {
        // Numeric value
        sqlValue = value;
      }

      // Handle case sensitivity (only relevant for string comparisons like =, !=, LIKE, REGEXP)
      const caseSensitiveConfig = valueConfig.config?.find(
        (c) => c._attributes.key === "stringCaseMatching"
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
        // Apply LOWER to both column and value for case-insensitive comparison
        // Note: LOWER might impact index usage on the column.
        condition = `LOWER("${columnName}") ${sqlOperator} LOWER(${sqlValue})`;
        // For REGEXP, case-insensitivity might be handled by flags/functions depending on SQL dialect,
        // LOWER() might not always work as expected with REGEXP. Check dialect specifics.
        if (sqlOperator === "REGEXP") {
          console.warn(
            `Case-insensitive REGEXP for "${columnName}" using LOWER(). Verify compatibility with your SQL dialect.`
          );
        }
      } else {
        // Case-sensitive comparison or numeric comparison
        condition = `"${columnName}" ${sqlOperator} ${sqlValue}`;
      }

      // Add ESCAPE clause for LIKE if we translated wildcards that needed escaping
      if (
        sqlOperator === "LIKE" &&
        translateKnimeWildcardToSQL(value) !==
          value.replace(/\*/g, "%").replace(/\?/g, "_")
      ) {
        condition += " ESCAPE '\\'"; // Standard SQL escape character
      }

      return condition;
    })
    .filter((condition) => condition !== null);

  if (conditions.length === 0) {
    console.warn("No valid filter conditions generated from predicates.");
    // If NON_MATCHING and no conditions, it means NOT (FALSE) -> TRUE -> Select All
    // If MATCHING and no conditions, it means TRUE -> Select All
    // However, this usually indicates an issue, so maybe returning an error/warning comment is better.
    return `SELECT * FROM ${previousNodeName}; -- Warning: No valid filter conditions generated or applied`;
  }

  // Step 5: Combine conditions with AND/OR
  const combinedConditions = conditions.join(` ${matchCriteria} `);

  // Step 6: Apply outputMode
  const whereClause =
    outputMode === "NON_MATCHING"
      ? `NOT (${combinedConditions})`
      : combinedConditions;

  // Step 7: Construct final SQL query
  const sqlQuery = `SELECT * FROM ${previousNodeName} WHERE ${whereClause};`;

  return sqlQuery;
}

// Example Usage:
// const nodeConfigJson = { ... }; // Your parsed JSON for the Row Filter node
// const previousTableName = "previous_step_results";
// const sql = convertRowFilterNodeToSQL(nodeConfigJson, previousTableName);
// console.log(sql);
