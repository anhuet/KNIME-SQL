import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig";
const getEntryValue = (entryProp, key) => {
  // ... (implementation remains the same) ...
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
  // ... (implementation remains the same) ...
  if (!config) return null;
  const nodes = Array.isArray(config) ? config : [config];
  return (
    nodes.find((node) => node._attributes && node._attributes.key === key) ||
    null
  );
};

/**
 * Extracts an array of string values from a KNIME config entry array (compact format),
 * typically used for lists like 'included_names'.
 * @param {object} configNode - The config node containing the entry array (e.g., 'included_names').
 * @returns {string[]} - Array of string values.
 */

/**
 * Determines the SQL JOIN type based on the include flags from Joiner3/modern Joiner config.
 * @param {boolean} includeMatches - Value of 'includeMatchesInOutput'.
 * @param {boolean} includeLeftUnmatched - Value of 'includeLeftUnmatchedInOutput'.
 * @param {boolean} includeRightUnmatched - Value of 'includeRightUnmatchedInOutput'.
 * @returns {string} - SQL JOIN keyword (e.g., "INNER JOIN", "LEFT OUTER JOIN").
 */
const getSQLJoinTypeFromFlags = (
  includeMatches,
  includeLeftUnmatched,
  includeRightUnmatched
) => {
  // ... (implementation remains the same) ...
  if (includeMatches && includeLeftUnmatched && includeRightUnmatched)
    return "FULL OUTER JOIN";
  if (includeMatches && includeLeftUnmatched && !includeRightUnmatched)
    return "LEFT OUTER JOIN";
  if (includeMatches && !includeLeftUnmatched && includeRightUnmatched)
    return "RIGHT OUTER JOIN";
  if (includeMatches && !includeLeftUnmatched && !includeRightUnmatched)
    return "INNER JOIN";
  console.warn(
    `Unsupported join combination: matches=${includeMatches}, left=${includeLeftUnmatched}, right=${includeRightUnmatched}. Defaulting to INNER JOIN.`
  );
  return "INNER JOIN";
};

/**
 * Converts a KNIME Joiner node configuration (JoinerNodeFactory or Joiner3NodeFactory
 * using the modern internal structure) from compact JSON to an SQL query.
 * Handles KNIME's "<row-keys>" by assuming a standard "RowID" column in SQL.
 * Relies solely on column lists defined within the configuration.
 * Ensures table names/aliases passed in are quoted in the final SQL.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {string} [leftInputName="left_input"] - The name/alias for the left input table/subquery.
 * @param {string} [rightInputName="right_input"] - The name/alias for the right input table/subquery.
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertJoinerNodeToSQL(
  nodeConfig,
  leftInputName = "left_input",
  rightInputName = "right_input"
) {
  // Step 1: Verify node type
  // ... (implementation remains the same) ...
  const factory = getEntryValue(nodeConfig.entry, "factory");
  const JOINER_FACTORY_MODERN =
    "org.knime.base.node.preproc.joiner3.Joiner3NodeFactory";
  const JOINER_FACTORY_LEGACY_WITH_MODERN_CONFIG =
    "org.knime.base.node.preproc.joiner.JoinerNodeFactory";
  if (
    factory !== JOINER_FACTORY_MODERN &&
    factory !== JOINER_FACTORY_LEGACY_WITH_MODERN_CONFIG
  ) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected Joiner factory (${JOINER_FACTORY_MODERN} or ${JOINER_FACTORY_LEGACY_WITH_MODERN_CONFIG}), but got ${factoryInfo}. This function expects the modern internal config structure.`;
  }

  // Step 2: Find the model configuration
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config || !modelNode.entry) {
    return "Error: Model configuration not found or invalid in nodeConfig.";
  }

  // Step 3: Extract join parameters
  // ... (extraction logic remains the same) ...
  const duplicateHandling = getEntryValue(modelNode.entry, "duplicateHandling");
  const suffix = getEntryValue(modelNode.entry, "suffix") || "_dup";
  const mergeJoinColumns = getEntryValue(modelNode.entry, "mergeJoinColumns");
  const includeMatches = getEntryValue(
    modelNode.entry,
    "includeMatchesInOutput"
  );
  const includeLeftUnmatched = getEntryValue(
    modelNode.entry,
    "includeLeftUnmatchedInOutput"
  );
  const includeRightUnmatched = getEntryValue(
    modelNode.entry,
    "includeRightUnmatchedInOutput"
  );
  const matchingCriteriaNode = findConfigByKey(
    modelNode.config,
    "matchingCriteria"
  );
  const joinConditions = []; // Store conditions for the ON clause

  if (matchingCriteriaNode && matchingCriteriaNode.config) {
    const criteriaConfigs = Array.isArray(matchingCriteriaNode.config)
      ? matchingCriteriaNode.config
      : [matchingCriteriaNode.config];
    criteriaConfigs.forEach((critConfig) => {
      let leftKey = getEntryValue(critConfig.entry, "leftTableColumn");
      let rightKey = getEntryValue(critConfig.entry, "rightTableColumn");

      // *** Handle <row-keys> translation ***
      // Assumes the upstream tables have a column named "RowID"
      const rowIdColName = '"RowID"'; // Use a standard quoted name
      if (leftKey === "<row-keys>") {
        leftKey = rowIdColName;
        console.log(
          "Detected join on left <row-keys>, translating to",
          rowIdColName
        );
      } else {
        // Quote regular column names
        leftKey = `"${leftKey.replace(/"/g, '""')}"`;
      }
      if (rightKey === "<row-keys>") {
        rightKey = rowIdColName;
        console.log(
          "Detected join on right <row-keys>, translating to",
          rowIdColName
        );
      } else {
        // Quote regular column names
        rightKey = `"${rightKey.replace(/"/g, '""')}"`;
      }
      // *** End <row-keys> handling ***

      if (leftKey && rightKey) {
        // Store the keys for the condition (leftKey/rightKey are now quoted or "RowID")
        joinConditions.push({ leftKey, rightKey });
      }
    });
  }

  if (joinConditions.length === 0) {
    // ... (error handling remains the same) ...
    const compositionMode = getEntryValue(modelNode.entry, "compositionMode");
    if (compositionMode === "UNION" || compositionMode === "INTERSECTION") {
      return `Error: Joiner mode '${compositionMode}' without explicit matching criteria is not directly convertible to a standard SQL JOIN.`;
    }
    return "Error: No valid join keys found in matchingCriteria.";
  }

  // Step 4: Determine columns to include based *only* on the config
  // ... (logic remains the same) ...
  const leftSelectionNode = findConfigByKey(
    modelNode.config,
    "leftColumnSelectionConfig"
  );
  const rightSelectionNode = findConfigByKey(
    modelNode.config,
    "rightColumnSelectionConfig"
  );
  const leftIncludedNamesNode = leftSelectionNode
    ? findConfigByKey(leftSelectionNode.config, "included_names")
    : null;
  const rightIncludedNamesNode = rightSelectionNode
    ? findConfigByKey(rightSelectionNode.config, "included_names")
    : null;
  let finalLeftCols = getArrayValuesFromConfig(leftIncludedNamesNode);
  let finalRightCols = getArrayValuesFromConfig(rightIncludedNamesNode);

  // Step 4.5: Handle column removal based on mergeJoinColumns
  // Need the *original* right join key names before they were potentially replaced by "RowID"
  const originalRightJoinKeys = [];
  if (matchingCriteriaNode && matchingCriteriaNode.config) {
    const criteriaConfigs = Array.isArray(matchingCriteriaNode.config)
      ? matchingCriteriaNode.config
      : [matchingCriteriaNode.config];
    criteriaConfigs.forEach((critConfig) => {
      const rightKey = getEntryValue(critConfig.entry, "rightTableColumn");
      if (rightKey && rightKey !== "<row-keys>") {
        // Exclude row key placeholder
        originalRightJoinKeys.push(rightKey);
      }
      // If joining on RowID and merging, we might want to keep only one RowID column implicitly?
      // Current logic only removes explicitly named columns from the right side.
    });
  }
  const originalRightJoinKeysSet = new Set(originalRightJoinKeys);

  if (mergeJoinColumns === true) {
    // If merging, remove the *original* join keys from the right side column list
    finalRightCols = finalRightCols.filter(
      (col) => !originalRightJoinKeysSet.has(col)
    );
    // If join was on RowID and merge=true, should we also remove "RowID" if it exists in finalRightCols?
    // This depends on whether "RowID" was explicitly selected via included_names. Let's assume not for now.
  }

  // Prepare Quoted Input Names
  const quotedLeftInputName = `"${leftInputName.replace(/"/g, '""')}"`;
  const quotedRightInputName = `"${rightInputName.replace(/"/g, '""')}"`;

  // Step 5: Build SELECT clause with duplicate handling
  // ... (logic remains the same, uses finalLeftCols/finalRightCols) ...
  const selectParts = [];
  const finalLeftColNames = new Set(finalLeftCols);
  const finalRightColNames = new Set(finalRightCols); // Use potentially modified list
  const selectedColumnAliases = new Set();

  for (const col of finalLeftCols) {
    let alias = col;
    if (finalRightColNames.has(col)) {
      if (duplicateHandling === "FAIL")
        return `Error: Duplicate column name "${col}" found...`;
      if (duplicateHandling === "KEEP_RIGHT") continue;
      if (duplicateHandling === "APPEND_SUFFIX") alias = `${col}${suffix}`;
      else if (duplicateHandling !== "KEEP_LEFT") alias = `${col}_L`;
    }
    let finalAlias = alias;
    let suffixCounter = 1;
    while (selectedColumnAliases.has(finalAlias))
      finalAlias = `${alias}_${suffixCounter++}`;
    selectedColumnAliases.add(finalAlias);
    // Quote original column name, use potentially modified alias (also quoted)
    selectParts.push(
      `${quotedLeftInputName}."${col.replace(
        /"/g,
        '""'
      )}" AS "${finalAlias.replace(/"/g, '""')}"`
    );
  }

  for (const col of finalRightCols) {
    // Iterate over potentially modified list
    let alias = col;
    if (finalLeftColNames.has(col)) {
      if (duplicateHandling === "FAIL")
        return `Error: Duplicate column name "${col}" found...`;
      if (duplicateHandling === "KEEP_LEFT") continue;
      if (duplicateHandling === "APPEND_SUFFIX") alias = `${col}${suffix}`;
      else if (duplicateHandling !== "KEEP_RIGHT") alias = `${col}_R`;
    }
    let finalAlias = alias;
    let suffixCounter = 1;
    while (selectedColumnAliases.has(finalAlias))
      finalAlias = `${alias}_${suffixCounter++}`;
    selectedColumnAliases.add(finalAlias);
    // Quote original column name, use potentially modified alias (also quoted)
    selectParts.push(
      `${quotedRightInputName}."${col.replace(
        /"/g,
        '""'
      )}" AS "${finalAlias.replace(/"/g, '""')}"`
    );
  }

  if (selectParts.length === 0) {
    return "Warning: No columns selected for output based on included_names lists in the config (after potential merge).";
  }
  const selectClause = `SELECT\n  ${selectParts.join(",\n  ")}`;

  // Step 6: Build JOIN type based on flags
  // ... (logic remains the same) ...
  const joinType = getSQLJoinTypeFromFlags(
    includeMatches,
    includeLeftUnmatched,
    includeRightUnmatched
  );

  // Step 7: Build ON clause using processed joinConditions
  const onConditions = joinConditions.map((condition) => {
    // leftKey and rightKey are already quoted column names or the quoted "RowID"
    return `${quotedLeftInputName}.${condition.leftKey} = ${quotedRightInputName}.${condition.rightKey}`;
  });
  const onClause = `ON ${onConditions.join(" AND ")}`;

  // Step 8: Construct final SQL query
  // ... (logic remains the same) ...
  const sqlQuery = `
${selectClause}
FROM ${quotedLeftInputName}
${joinType} ${quotedRightInputName}
${onClause};
`;

  return sqlQuery.trim();
}
