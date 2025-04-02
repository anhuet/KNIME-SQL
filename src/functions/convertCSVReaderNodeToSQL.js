/**
 * Utility function to get a value from a node's "entry".
 * The node.entry can be an object or an array.
 *
 * @param {object} node - The node that has an "entry" property.
 * @param {string} key - The key to search for.
 * @returns {string|null} - The value if found; otherwise, null.
 */
function getEntryValue(node, key) {
  if (!node || !node.entry) return null;
  const entries = Array.isArray(node.entry) ? node.entry : [node.entry];
  const found = entries.find(
    (entry) => entry._attributes && entry._attributes.key === key
  );
  return found ? found._attributes.value : null;
}

/**
 * Utility function to find a configuration node by its _attributes.key.
 * The "config" parameter can be an array or a single node.
 *
 * @param {object|array} config - The config node or array of nodes.
 * @param {string} key - The key to search for.
 * @returns {object|null} - The found node, or null if not found.
 */
function findConfigByKey(config, key) {
  if (!config) return null;
  if (Array.isArray(config)) {
    return config.find(
      (node) => node._attributes && node._attributes.key === key
    );
  } else if (config._attributes && config._attributes.key === key) {
    return config;
  }
  return null;
}

/**
 * Converts a KNIME CSV Reader node configuration to an SQL query.
 *
 * The configuration is expected to have two main properties:
 *   - entry: an array of key/value pairs (including "factory", etc.)
 *   - config: an array of child nodes.
 *
 * The CSV file name is extracted from:
 *   model → settings → file_selection → path → (entry with key "path")
 *
 * The column names are extracted from:
 *   model → table_spec_config_Internals → individual_specs → (node with key equal to the CSV file name)
 *     → each column node's entry with key "name"
 *
 * Example SQL output:
 *   SELECT
 *     product,
 *     country,
 *     date,
 *     quantity,
 *     amount,
 *     card,
 *     Cust_ID
 *   FROM sales_2008-2011.csv;
 *
 * @param {object} nodeConfig - The full node configuration object.
 * @returns {string} - The generated SQL query or an error message if something is missing.
 */
export function convertCSVReaderNodeToSQL(nodeConfig) {
  // Step 1: Ensure the node is a CSV Reader by checking the "factory" entry.
  const factory = getEntryValue({ entry: nodeConfig.entry }, "factory");
  const CSV_FACTORY =
    "org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory";
  if (factory !== CSV_FACTORY) {
    return "This function only converts CSV Reader nodes.";
  }

  // Step 2: Locate the "model" node from the top-level config.
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config) {
    return "Model node not found in the configuration.";
  }

  // Step 3: Extract the CSV file name.
  // Path: model → settings → file_selection → path → entry (key "path")
  let fileName = "";
  const settingsNode = findConfigByKey(modelNode.config, "settings");
  if (settingsNode && settingsNode.config) {
    const fileSelectionNode = findConfigByKey(
      settingsNode.config,
      "file_selection"
    );
    if (fileSelectionNode && fileSelectionNode.config) {
      const pathNode = findConfigByKey(fileSelectionNode.config, "path");
      if (pathNode) {
        fileName = getEntryValue(pathNode, "path");
      }
    }
  }
  if (!fileName) {
    return "CSV file name not found in the configuration.";
  }

  // Step 4: Extract the column names.
  // Path: model → table_spec_config_Internals → individual_specs → (node keyed by fileName) → config (array of column nodes)
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
      let fileSpecNode = null;
      // individualSpecsNode.config can be an array or an object.
      if (Array.isArray(individualSpecsNode.config)) {
        fileSpecNode = individualSpecsNode.config.find(
          (node) => node._attributes && node._attributes.key === fileName
        );
      } else if (
        individualSpecsNode.config._attributes &&
        individualSpecsNode.config._attributes.key === fileName
      ) {
        fileSpecNode = individualSpecsNode.config;
      }
      if (fileSpecNode && fileSpecNode.config) {
        const columnNodes = Array.isArray(fileSpecNode.config)
          ? fileSpecNode.config
          : [fileSpecNode.config];
        columnNodes.forEach((colNode) => {
          const colName = getEntryValue(colNode, "name");
          if (colName) {
            columns.push(colName);
          }
        });
      }
    }
  }
  if (columns.length === 0) {
    return "No columns found in the configuration.";
  }

  // Step 5: Build and return the SQL query.
  return `SELECT\n  ${columns.join(",\n  ")}\nFROM ${fileName};`;
}
