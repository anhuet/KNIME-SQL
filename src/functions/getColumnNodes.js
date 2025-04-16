// src/functions/getColumnNodes.js

// Import necessary helpers
import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";
import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig";

/**
 * Analyzes a KNIME node's configuration to determine the final output columns,
 * columns added by the node, and columns removed by the node relative to its input.
 *
 * @param {object} nodeConfig - The full node configuration object (compact format).
 * @param {string[]} [inputColumnNames=null] - Array of column names from the primary input node.
 * Crucial for nodes that modify input columns.
 * @returns {{finalColumns: string[], addedColumns: string[], removedColumns: string[]}|null}
 * - An object detailing column changes, or null if inputs are invalid/insufficient.
 * - finalColumns: List of columns in the node's output.
 * - addedColumns: List of columns newly created by this node.
 * - removedColumns: List of columns from the input that were removed by this node.
 */
export function getColumnNodes(nodeConfig, inputColumnNames = null) {
  // --- Initial checks ---
  if (!nodeConfig || !nodeConfig.entry) {
    console.error("Invalid nodeConfig passed to getColumnNodes");
    return null;
  }
  const factory = getEntryValue(nodeConfig.entry, "factory");
  if (!factory) {
    console.error("Could not determine node factory from config.");
    return null;
  }
  const modelNode = findConfigByKey(nodeConfig.config, "model");

  // --- Initialize result structure ---
  let finalColumns = [];
  let addedColumns = [];
  let removedColumns = [];
  const inputCols = inputColumnNames ? [...inputColumnNames] : []; // Safe copy or empty array

  switch (factory) {
    // --- Reader Nodes ---
    case "org.knime.ext.poi3.node.io.filehandling.excel.reader.ExcelTableReaderNodeFactory":
    case "org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory": {
      if (!modelNode) return null;
      // Logic to extract columns from table_spec_config_Internals
      let extractedColumns = [];
      // --- Start of Extraction Logic (Adapt from your original function) ---
      let fileName = "";
      const settingsNode = findConfigByKey(modelNode.config, "settings");
      if (settingsNode && settingsNode.config) {
        const fileSelectionNode = findConfigByKey(
          settingsNode.config,
          "file_selection"
        );
        if (fileSelectionNode && fileSelectionNode.config) {
          const pathNode = findConfigByKey(fileSelectionNode.config, "path");
          if (pathNode) fileName = getEntryValue(pathNode.entry, "path");
        }
      }
      if (!fileName && settingsNode)
        fileName = getEntryValue(settingsNode.entry, "path");

      if (fileName) {
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
            const fileSpecNode = findConfigByKey(
              individualSpecsNode.config,
              fileName
            );
            if (fileSpecNode && fileSpecNode.config) {
              const columnNodes = Array.isArray(fileSpecNode.config)
                ? fileSpecNode.config
                : [fileSpecNode.config];
              columnNodes.forEach((colNode) => {
                if (
                  colNode._attributes &&
                  !isNaN(parseInt(colNode._attributes.key))
                ) {
                  // Check if it's likely a column config
                  const colName = getEntryValue(colNode.entry, "name");
                  if (colName) extractedColumns.push(colName);
                }
              });
            }
          }
        }
      } else {
        console.error(`Could not extract filename for ${factory}`);
        return null;
      }
      // --- End of Extraction Logic ---

      finalColumns = [...extractedColumns]; // Output is the set of columns read
      addedColumns = []; // Reader doesn't "add" columns relative to KNIME workflow input
      removedColumns = []; // Reader doesn't "remove" columns relative to KNIME workflow input
      break;
    }

    // --- Column Filter ---
    // src/functions/getColumnNodes.js (Inside the switch statement)

    // --- Column Filter ---
    case "org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory": {
      // NOTE: This logic operates WITHOUT inputColumnNames, based only on node config.
      // It primarily reflects 'EnforceInclusion' behavior.
      // Results for 'EnforceExclusion' mode might not be fully accurate without input context.

      if (!modelNode) {
        console.error("Column Filter model configuration not found.");
        // Return empty structure as we cannot read the config
        return { finalColumns: [], addedColumns: [], removedColumns: [] };
      }

      const columnFilterNode = findConfigByKey(
        modelNode.config,
        "column-filter"
      );
      if (!columnFilterNode || !columnFilterNode.config) {
        console.error(
          "Column Filter specific configuration ('column-filter') not found."
        );
        // Return empty structure
        return { finalColumns: [], addedColumns: [], removedColumns: [] };
      }

      const includedNamesNode = findConfigByKey(
        columnFilterNode.config,
        "included_names"
      );
      const excludedNamesNode = findConfigByKey(
        columnFilterNode.config,
        "excluded_names"
      );

      const includedConfig = getArrayValuesFromConfig(includedNamesNode);
      const excludedConfig = getArrayValuesFromConfig(excludedNamesNode);
      const excludedSetConfig = new Set(excludedConfig); // For efficient lookup

      // Calculate finalColumns based on included list minus excluded list
      finalColumns = includedConfig.filter(
        (col) => !excludedSetConfig.has(col)
      );

      // Assume removedColumns are those listed in the exclude config
      removedColumns = [...excludedConfig];

      // Added columns are always empty for a filter node
      addedColumns = [];

      break;
    }

    // --- Row Filter / Duplicate Row Filter (Assume they don't change columns) ---
    case "org.knime.base.node.preproc.filter.row3.RowFilterNodeFactory":

    // --- Column Merger ---
    case "org.knime.base.node.preproc.columnmerge.ColumnMergerNodeFactory": {
      // NOTE: This logic operates WITHOUT inputColumnNames, based only on node config.
      // Determination of finalColumns is not possible without input context.

      if (!modelNode || !modelNode.entry) {
        // Merger config is directly in model's entry
        console.error(
          "Column Merger model configuration or entries not found."
        );
        return { finalColumns: [], addedColumns: [], removedColumns: [] };
      }

      const primaryCol = getEntryValue(modelNode.entry, "primaryColumn");
      const secondaryCol = getEntryValue(modelNode.entry, "secondaryColumn");
      const outputPlacement = getEntryValue(modelNode.entry, "outputPlacement");
      // outputName is needed for the 'AppendAsNewColumn' case
      const outputName = getEntryValue(modelNode.entry, "outputName");

      if (!primaryCol || !secondaryCol || !outputPlacement) {
        console.error(
          "Column Merger config missing required parameters (primary, secondary, placement)."
        );
        return { finalColumns: [], addedColumns: [], removedColumns: [] };
      }

      // --- Determine added columns based *only* on placement config ---
      if (outputPlacement === "AppendAsNewColumn") {
        // Only add if outputName is specified
        if (outputName) {
          addedColumns = [outputName];
        } else {
          addedColumns = [];
          console.warn(
            "Column Merger: AppendAsNewColumn selected but outputName is missing in config."
          );
        }
      } else {
        // For all "Replace..." modes, no columns are considered "added"
        addedColumns = [];
      }

      // --- Determine removed columns based *only* on placement config ---
      const tempRemoved = new Set();
      // Check specific KNIME values for replacement options
      if (
        outputPlacement === "ReplacePrimary" ||
        outputPlacement === "ReplaceBoth"
      ) {
        if (primaryCol) tempRemoved.add(primaryCol);
      }
      if (
        outputPlacement === "ReplaceSecondary" ||
        outputPlacement === "ReplaceBoth"
      ) {
        if (secondaryCol) tempRemoved.add(secondaryCol);
      }
      // For "AppendAsNewColumn", no columns are removed
      removedColumns = Array.from(tempRemoved);

      // --- Determine final columns - Cannot be done accurately without input columns. ---
      finalColumns = []; // Indicate indeterminable
      // We could potentially add outputName here if AppendAsNewColumn, but the full list is unknown.
      // Example: if (outputPlacement === "AppendAsNewColumn" && outputName) finalColumns = [outputName]; // But this is incomplete.
      console.warn(
        "Cannot determine finalColumns for Column Merger without inputColumnNames."
      );

      break;
    }

    // --- Joiner ---
    // TODO: Implement detailed analysis for Joiner based on convertJoinerJSONToSQL logic

    case "org.knime.base.node.preproc.colconvert.stringtonumber2.StringToNumber2NodeFactory": {
      //
      finalColumns = [...inputCols]; // Column names remain the same
      addedColumns = []; // No columns added
      removedColumns = []; // No columns removed
      break;
    }
    case "org.knime.base.node.preproc.joiner.JoinerNodeFactory":
    case "org.knime.base.node.preproc.joiner3.Joiner3NodeFactory": {
      console.warn(
        `Detailed column change analysis for Joiner node (${factory}) is not fully implemented in getColumnNodes.`
      );
      // Placeholder: Assume pass-through or calculate final columns only
      finalColumns = [...inputCols]; // Needs proper calculation based on joiner logic
      addedColumns = []; // Usually no truly new columns, just aliased/merged
      removedColumns = []; // Needs calculation based on merge/duplicate settings
      // You would need to adapt logic from convertJoinerJSONToSQL here
      // to determine which original right keys are removed (if merge=true)
      // and which columns are dropped due to duplicate handling.
      break;
    }

    // --- Default for other nodes (assume pass-through) ---
    default:
      finalColumns = [...inputCols];
      addedColumns = [];
      removedColumns = [];
      break;
  }

  // --- Return the structured result ---
  return { finalColumns, addedColumns, removedColumns };
}
