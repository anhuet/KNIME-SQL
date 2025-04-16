// src/functions/convertExcelReaderNodeToSQL.js

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
 * Converts a KNIME Excel Reader node configuration (compact JSON) to an SQL query.
 * Extracts the file name, sheet name, and column names.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertExcelReaderNodeToSQL(nodeConfig) {
  // Step 1: Verify node type
  const factory = getEntryValue(nodeConfig?.entry, "factory");
  const EXCEL_READER_FACTORY =
    "org.knime.ext.poi3.node.io.filehandling.excel.reader.ExcelTableReaderNodeFactory";
  if (factory !== EXCEL_READER_FACTORY) {
    const factoryInfo = factory ? `"${factory}"` : "N/A";
    return `Error: Expected Excel Reader node factory (${EXCEL_READER_FACTORY}), but got ${factoryInfo}.`;
  }

  // Step 2: Locate the model -> settings node
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config) {
    return "Error: Model configuration not found.";
  }
  const settingsNode = findConfigByKey(modelNode.config, "settings");
  if (!settingsNode || !settingsNode.config || !settingsNode.entry) {
    return "Error: Settings configuration not found within model.";
  }

  // Step 3: Extract the Excel file name
  // Path: model -> settings -> file_selection -> path -> entry[@key="path"]
  let fileName = "";
  const fileSelectionNode = findConfigByKey(
    settingsNode.config,
    "file_selection"
  );
  if (fileSelectionNode && fileSelectionNode.config) {
    const pathNode = findConfigByKey(fileSelectionNode.config, "path");
    if (pathNode) {
      // Use getEntryValue on the pathNode's entry property
      fileName = getEntryValue(pathNode.entry, "path");
    }
  }
  if (!fileName) {
    // Fallback: Check older structure if needed (though unlikely based on provided XML)
    // Check model -> settings -> entry[@key="path"] as a potential fallback?
    fileName = getEntryValue(settingsNode.entry, "path"); // Attempt fallback
    if (!fileName) {
      return "Error: Excel file name/path not found in the configuration.";
    }
    console.warn(
      "Used fallback path for Excel file name. Check configuration structure."
    );
  }

  // Step 3.5: Extract the Sheet Name
  // Path: model -> settings -> entry[@key="sheet_name"]
  const sheetName = getEntryValue(settingsNode.entry, "sheet_name");

  // Step 4: Extract column names
  // Path: model -> table_spec_config_Internals -> individual_specs -> config[@key=fileName] -> config (numbered) -> entry[@key="name"]
  let columns = [];
  const tableSpecNode = findConfigByKey(
    modelNode.config,
    "table_spec_config_Internals"
  );
  if (tableSpecNode && tableSpecNode.config) {
    const individualSpecsNode = findConfigByKey(
      tableSpecNode.config,
      "individual_specs"
    );
    if (individualSpecsNode && individualSpecsNode.config) {
      // Find the config node whose key matches the file name
      const fileSpecNode = findConfigByKey(
        individualSpecsNode.config,
        fileName
      );

      if (fileSpecNode && fileSpecNode.config) {
        // The column configs are within fileSpecNode.config
        const columnNodes = Array.isArray(fileSpecNode.config)
          ? fileSpecNode.config
          : [fileSpecNode.config];

        columnNodes.forEach((colNode) => {
          // Check if it's actually a column config (might have other entries)
          if (
            colNode._attributes &&
            !isNaN(parseInt(colNode._attributes.key))
          ) {
            const colName = getEntryValue(colNode.entry, "name");
            if (colName) {
              columns.push(colName);
            }
          }
        });
      }
    }
  }

  if (columns.length === 0) {
    // Attempt fallback: Sometimes spec might be directly under table_spec_config_Internals? (Less common)
    // This part needs verification based on more examples if the primary path fails.
    console.warn(
      "Could not find columns in individual_specs. Check table_spec_config_Internals structure."
    );
    // For now, return error if primary path yields no columns.
    return "Error: No columns found in the table specification within the configuration.";
  }

  // Step 5: Build and return the SQL query. Quote identifiers.
  const quotedColumns = columns
    .map((col) => `"${col.replace(/"/g, '""')}"`)
    .join(",\n  ");
  // Quote filename used as table name, escape internal quotes if any
  const quotedFileName = `"${fileName.replace(/"/g, '""')}"`;
  const sheetComment = sheetName
    ? `-- Reading data from sheet: ${sheetName}\n`
    : "";

  return `${sheetComment}SELECT\n  ${quotedColumns}\nFROM ${quotedFileName};`;
}
