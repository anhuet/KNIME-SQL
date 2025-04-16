import { InboxOutlined } from "@ant-design/icons";
import {
  Button,
  Card,
  message,
  Table,
  Typography,
  Upload,
  Modal,
  Tag,
} from "antd";
import JSZip from "jszip";
import React, { useEffect, useState } from "react";
import * as xmlJs from "xml-js";
// Removed lodash import as it wasn't used directly in the provided snippet
// import _ from "lodash";

// Import all necessary conversion functions
import { convertConcatenateNodeToSQL } from "./functions/convertConcatenateNodeToSQL"; // Import the new function
import { parseWorkflowKnime } from "./functions/parseWorkflowKnime"; // [cite: uploaded:src/functions/parseWorkflowKnime.js]
import { convertCSVReaderNodeToSQL } from "./functions/convertCSVReaderNodeToSQL"; // [cite: uploaded:src/functions/convertCSVReaderNodeToSQL.js]
import { convertColumnFilterNodeToSQL } from "./functions/convertColumnFilterNodeToSQL"; // [cite: uploaded:src/functions/convertColumnFilterNodeToSQL.js]
import { convertRowFilterNodeToSQL } from "./functions/convertRowFilterNodeToSQL"; // [cite: uploaded:src/functions/convertRowFilterNodeToSQL.js]
import { convertDuplicateRowFilterJSONToSQL } from "./functions/convertDuplicateRowFilterNodeToSQL"; // [cite: uploaded:src/functions/convertDuplicateRowFilterNodeToSQL.js]
import { convertJoinerNodeToSQL } from "./functions/convertJoinerJSONToSQL"; // [cite: uploaded:src/functions/convertJoinerJSONToSQL.js]
import { convertExcelReaderNodeToSQL } from "./functions/convertExcelReaderNodeToSQL"; // [cite: uploaded:src/functions/convertExcelReaderNodeToSQL.js]
import { convertColumnMergerNodeToSQL } from "./functions/convertColumnMergerNodeToSQL"; // [cite: uploaded:src/functions/convertColumnMergerNodeToSQL.js]
import { getColumnNodes } from "./functions/getColumnNodes"; // [cite: uploaded:src/functions/getColumnNodes.js]
// Import the updated String to Number converter
import { convertStringToNumberNodeToSQL } from "./functions/convertStringToNumberNodeToSQL"; // [cite: knime_string_to_number_sql]

const { Dragger } = Upload;
const { Title } = Typography;

// Utility function to get a value from an entry array or object (compact format).
// Used for parsing node details from settings.xml JSON.
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

// Helper function to find *all* direct predecessors of a given node ID
// NOTE: This finds predecessors based on the *complete* node list,
// which is suitable for display but might differ from the context needed for SQL generation.
const findAllPreviousNodes = (currentNodeId, allNodes) => {
  // Find nodes where currentNodeId is in their nextNodes array
  // Ensure node.nextNodes exists and is an array before checking includes()
  return allNodes.filter(
    (node) =>
      node && // Ensure node exists
      node.id !== currentNodeId && // Exclude self
      Array.isArray(node.nextNodes) &&
      node.nextNodes.includes(currentNodeId)
  );
};

// Central function to call the appropriate SQL conversion logic based on node type
// Accepts the node's config, predecessor names, and the context of *all* processed nodes.
// *** UPDATED SIGNATURE AND LOGIC ***
export function convertSelectedNodeToSQL(
  nodeConfig, // Should include 'id' property
  predecessorNames = [], // Still useful for deriving previousNodeName easily
  allProcessedNodes = [],
  id // Context of nodes processed *before* the current one
) {
  const predecessorsWithContext = findAllPreviousNodes(
    // Use your helper
    id,
    allProcessedNodes
  ).map((p) => ({
    nodeName: p.nodeName || `node_${p.id}_output`,
    nodes: p.nodes || [],
  })); // Extract name and columns
  // Determine the primary input table name (often the first predecessor)
  const singlePreviousName =
    predecessorNames.length > 0 ? predecessorNames[0] : "input_table";
  console.log(nodeConfig, "nodeConfig");
  const factory = getEntryValue(nodeConfig?.entry, "factory");
  if (!factory) {
    return "Invalid node configuration: missing factory value.";
  }

  switch (factory) {
    case "org.knime.base.node.preproc.append.row.AppendedRowsNodeFactory":
      // Pass the node's config and the derived predecessor context
      return convertConcatenateNodeToSQL(nodeConfig, predecessorsWithContext);
    case "org.knime.base.node.preproc.colconvert.stringtonumber2.StringToNumber2NodeFactory":
      // *** UPDATED CALL ***
      // Pass nodeConfig (with id), the derived previous name, and the processed nodes context
      return convertStringToNumberNodeToSQL(
        nodeConfig, // Ensure this object has the 'id' property added during processing
        singlePreviousName,
        allProcessedNodes,
        selectedNode.id
        // Pass the context
      );

    // --- Calls to other functions remain unchanged for now ---
    // --- Consider updating them later to use allProcessedNodes if needed ---
    case "org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory":
      return convertCSVReaderNodeToSQL(nodeConfig);

    case "org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory":
      return convertColumnFilterNodeToSQL(nodeConfig, singlePreviousName);

    case "org.knime.base.node.preproc.filter.row3.RowFilterNodeFactory":
      return convertRowFilterNodeToSQL(nodeConfig, singlePreviousName);

    case "org.knime.base.node.preproc.duplicates.DuplicateRowFilterNodeFactory":
      // Placeholder for input columns - this function might also need updating
      // to use allProcessedNodes if it needs to derive input columns.
      const inputColumnsDupFilter = []; // This function's signature needs review
      return convertDuplicateRowFilterJSONToSQL(
        nodeConfig,
        singlePreviousName,
        inputColumnsDupFilter // Passing empty array, needs update if derivation is required
      );

    case "org.knime.base.node.preproc.joiner.JoinerNodeFactory":
    case "org.knime.base.node.preproc.joiner3.Joiner3NodeFactory":
      const leftInputName =
        predecessorNames.length > 0 ? predecessorNames[0] : "left_input";
      const rightInputName =
        predecessorNames.length > 1 ? predecessorNames[1] : "right_input";
      // This function derives columns internally from config, doesn't need allProcessedNodes yet.
      return convertJoinerNodeToSQL(nodeConfig, leftInputName, rightInputName);

    case "org.knime.ext.poi3.node.io.filehandling.excel.reader.ExcelTableReaderNodeFactory":
      return convertExcelReaderNodeToSQL(nodeConfig);

    case "org.knime.base.node.preproc.columnmerge.ColumnMergerNodeFactory":
      // This function currently expects explicit input columns.
      // It would need updating similar to StringToNumber if derivation is required.
      const inputColsForMerger = []; // Passing empty array, needs update if derivation is required
      return convertColumnMergerNodeToSQL(
        nodeConfig,
        singlePreviousName,
        inputColsForMerger // Pass the actual input column list here
      );

    default:
      console.warn(`Unsupported node factory for SQL conversion: ${factory}`);
      const nodeTypeName = factory.split(".").pop() || "Unknown Type";
      return `Conversion for node type "${nodeTypeName}" is not supported.`;
  }
}

function KnimeViewer() {
  const [selectedNode, setSelectedNode] = useState(null);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [rawNodeData, setRawNodeData] = useState([]); // Holds data read directly from files
  const [processedNodes, setProcessedNodes] = useState([]); // Holds fully processed data including columns, order, etc.

  const handleUpload = async (file) => {
    const zip = new JSZip();
    setRawNodeData([]); // Clear previous raw data
    setProcessedNodes([]); // Clear previous processed data
    try {
      const zipContent = await zip.loadAsync(file);
      const allFiles = Object.keys(zipContent.files);

      const knimeFile = allFiles.find((filePath) => {
        const parts = filePath.split("/");
        return parts[parts.length - 1] === "workflow.knime";
      });

      if (!knimeFile) {
        message.error("workflow.knime not found in the .knwf file");
        return false; // Indicate failure
      }
      const workflowXmlText = await zipContent.files[knimeFile].async("text");
      const workflowJson = JSON.parse(
        xmlJs.xml2json(workflowXmlText, { compact: true, spaces: 4 })
      );

      // Get node structure, order, and connections from workflow.knime
      const { nodes: parsedNodes } = parseWorkflowKnime(workflowJson); // [cite: uploaded:src/functions/parseWorkflowKnime.js]

      // Create a map for quick lookup of order and connections by ID
      const nodeInfoMap = {};
      parsedNodes.forEach((node) => {
        nodeInfoMap[node.id] = {
          order: node.order,
          nextNodes: node.nextNodes || [],
        };
      });

      // Find all settings.xml paths
      const xmlPaths = allFiles.filter((filePath) => {
        const parts = filePath.split("/");
        return parts.length > 1 && parts[parts.length - 1] === "settings.xml";
      });

      // Process settings.xml files and merge details
      const tempRawNodes = [];
      for (const item of xmlPaths) {
        const fileObj = zipContent.files[item];
        const fileText = await fileObj.async("text");
        const jsonObj = JSON.parse(
          xmlJs.xml2json(fileText, { compact: true, spaces: 4 })
        );

        const pathParts = item.split("/");
        const nodeFolder = pathParts[pathParts.length - 2];
        const match = nodeFolder.match(/ \(#(\d+)\)$/);
        const nodeId = match ? parseInt(match[1], 10) : null;

        const parsedNodeInfo = nodeId !== null ? nodeInfoMap[nodeId] : null;

        if (nodeId !== null && parsedNodeInfo !== null) {
          // Ensure node ID and workflow info exist
          const nodeName =
            getEntryValue(jsonObj.config?.entry, "node-name") || nodeFolder;
          const nodeType = getEntryValue(jsonObj.config?.entry, "factory");
          const nodeStatus = getEntryValue(jsonObj.config?.entry, "state");
          const customDesc = getEntryValue(
            jsonObj.config?.entry,
            "customDescription"
          );

          // Get initial column estimates (added/removed based *only* on this node's config)
          // Note: Final 'nodes' (output columns) will be calculated later
          const { finalColumns, addedColumns, removedColumns } = getColumnNodes(
            jsonObj.config // Pass only the config for initial analysis
            // Don't pass input columns here yet
          );

          tempRawNodes.push({
            id: nodeId, // Crucial: Ensure ID is stored
            nodeName: nodeName,
            nodeType: nodeType,
            nodeStatus: nodeStatus,
            description: customDesc,
            config: jsonObj.config, // Store the full config JSON
            order: parsedNodeInfo.order,
            nextNodes: parsedNodeInfo.nextNodes,
            // Store initial column analysis results
            initialOutputColumns: finalColumns, // Columns defined by this node (e.g., reader)
            addedColumns: addedColumns, // Columns explicitly added
            removedColumns: removedColumns, // Columns explicitly removed
            nodes: [], // Placeholder for final calculated output columns
            previousNodes: [], // Placeholder for predecessor IDs
          });
        } else {
          console.warn(
            `Could not extract valid node ID or workflow info for folder: ${nodeFolder}`
          );
        }
      }
      setRawNodeData(tempRawNodes); // Update state with raw merged data
      message.success(
        `${file.name} processed. Calculating workflow details...`
      );
    } catch (error) {
      console.error("Error processing .knwf file:", error);
      message.error(
        `Failed to process .knwf file: ${error.message || "Unknown error"}`
      );
    }
    // Don't return false here, customRequest handles completion
  };

  // Function to process raw data and calculate final columns, predecessors etc.
  // Moved outside useEffect for clarity, called when rawNodeData changes.
  const processWorkflowData = (rawData) => {
    if (!Array.isArray(rawData) || rawData.length === 0) {
      return [];
    }
    console.log("Processing raw node data...");

    // 1. Create a map for efficient lookup by ID from the raw data
    const originalNodeMap = rawData.reduce((map, node) => {
      if (node.id !== null && node.id !== undefined) {
        map[node.id] = node;
      }
      return map;
    }, {});

    // 2. Sort nodes by execution order
    const sortedNodes = [...rawData].sort((a, b) => {
      if (a.order === undefined || a.order === null) return 1; // Nodes without order go last
      if (b.order === undefined || b.order === null) return -1;
      return a.order - b.order;
    });

    // 3. Process nodes in order, storing results in a map
    const processedNodeMap = {};

    for (const currentNode of sortedNodes) {
      if (currentNode.id === null || currentNode.id === undefined) continue;

      // --- Find Predecessor IDs ---
      const predecessorIds = [];
      // Iterate over the *original* map keys (all node IDs)
      for (const potentialPredecessorIdStr in originalNodeMap) {
        const potentialPredecessorId = parseInt(potentialPredecessorIdStr, 10);
        const potentialPredecessor = originalNodeMap[potentialPredecessorId];
        if (
          potentialPredecessor &&
          potentialPredecessor.id !== currentNode.id &&
          Array.isArray(potentialPredecessor.nextNodes) &&
          potentialPredecessor.nextNodes.includes(currentNode.id)
        ) {
          predecessorIds.push(potentialPredecessor.id);
        }
      }
      // --- End Find Predecessor IDs ---

      // --- Calculate Final Output Columns ('nodes') ---
      let calculatedInputColumns = [];
      if (predecessorIds.length > 0) {
        // Combine columns from all predecessors (using Set to handle duplicates)
        const combinedPredecessorColumns = new Set();
        predecessorIds.forEach((predId) => {
          const predecessorNode = processedNodeMap[predId]; // IMPORTANT: Lookup *processed* node
          if (predecessorNode?.nodes && Array.isArray(predecessorNode.nodes)) {
            predecessorNode.nodes.forEach((col) =>
              combinedPredecessorColumns.add(col)
            );
          } else {
            console.warn(
              `Predecessor ${predId} for node ${currentNode.id} not found in processed map or has no columns.`
            );
            // Optionally, could look up in originalNodeMap as a fallback, but might be inaccurate
          }
        });
        calculatedInputColumns = Array.from(combinedPredecessorColumns);
      } else {
        // Node has no predecessors (e.g., reader node)
        calculatedInputColumns = [];
      }

      // Now, determine the *output* columns of the *current* node
      let finalOutputColumns;
      // Use getColumnNodes again, but this time provide the calculated input columns
      // Need to ensure getColumnNodes can handle this scenario correctly.
      // Let's simulate its logic here for clarity:
      const initialOutput = Array.isArray(currentNode.initialOutputColumns)
        ? currentNode.initialOutputColumns
        : [];
      const columnsToAdd = Array.isArray(currentNode.addedColumns)
        ? currentNode.addedColumns
        : [];
      const columnsToRemove = new Set(
        Array.isArray(currentNode.removedColumns)
          ? currentNode.removedColumns
          : []
      );

      if (initialOutput.length > 0) {
        // If the node defines its own output (like a reader), use that.
        finalOutputColumns = [...initialOutput];
      } else {
        // Otherwise, start with input columns, remove specified, add specified.
        let currentColumns = calculatedInputColumns.filter(
          (col) => !columnsToRemove.has(col)
        );
        const currentColumnSet = new Set(currentColumns);
        columnsToAdd.forEach((col) => currentColumnSet.add(col));
        finalOutputColumns = Array.from(currentColumnSet);
      }
      // --- End Calculate Final Output Columns ---

      // Store the fully processed node
      processedNodeMap[currentNode.id] = {
        ...currentNode,
        previousNodes: predecessorIds, // Store the calculated previous node IDs
        nodes: finalOutputColumns, // Store the calculated final output columns
      };
    } // End loop through sorted nodes

    // 4. Convert map back to array, maintaining the calculated sort order
    const finalProcessedData = sortedNodes
      .map((node) => processedNodeMap[node.id]) // Get processed data in sorted order
      .filter(Boolean); // Filter out any nodes that might have failed processing

    console.log("Processing complete.");
    return finalProcessedData;
  };

  // Effect to run processing when rawNodeData changes
  useEffect(() => {
    if (rawNodeData && rawNodeData.length > 0) {
      const finalNodes = processWorkflowData(rawNodeData);
      setProcessedNodes(finalNodes); // Update the final state
    } else {
      setProcessedNodes([]); // Clear if raw data is cleared
    }
  }, [rawNodeData]); // Dependency array ensures this runs when rawNodeData updates

  const formatNodeType = (fullType) => {
    if (!fullType) return "Unknown Type";
    return fullType.split(".").pop(); // Get last part of factory string
  };

  // Columns for the main table display
  const columns = [
    {
      title: "Step",
      dataIndex: "order",
      key: "order",
      width: 60,
      sorter: (a, b) => (a.order ?? Infinity) - (b.order ?? Infinity),
      defaultSortOrder: "ascend",
      render: (order) => (
        <span>{order !== undefined && order !== null ? order + 1 : "N/A"}</span>
      ),
    },
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      width: 60,
      render: (id) => <span>{id !== null ? id : "N/A"}</span>,
    },
    {
      title: "Node Name",
      dataIndex: "nodeName",
      key: "nodeName",
      ellipsis: true,
    },
    {
      title: "Node Type",
      dataIndex: "nodeType",
      key: "nodeType",
      render: (text) => <Tag>{formatNodeType(text)}</Tag>,
      ellipsis: true,
    },
    {
      title: "Status",
      dataIndex: "nodeStatus",
      key: "nodeStatus",
      width: 100,
      render: (status) => (
        <Tag color={status === "EXECUTED" ? "green" : "orange"}>
          {status || "Unknown"}
        </Tag>
      ),
    },
    {
      title: "Prev IDs",
      dataIndex: "previousNodes",
      key: "previousNodes",
      width: 80,
      render: (ids) => (
        <span>
          {Array.isArray(ids) && ids.length > 0 ? ids.join(", ") : "None"}
        </span>
      ),
    },
    {
      title: "Next IDs",
      dataIndex: "nextNodes",
      key: "nextNodes",
      width: 80,
      render: (ids) => (
        <span>
          {Array.isArray(ids) && ids.length > 0 ? ids.join(", ") : "None"}
        </span>
      ),
    },
    {
      title: "Output Columns",
      dataIndex: "nodes",
      key: "nodes",
      ellipsis: true,
      render: (cols) => (
        <div
          style={{ maxHeight: "60px", overflowY: "auto", whiteSpace: "normal" }}
        >
          {Array.isArray(cols) && cols.length > 0 ? (
            cols.map((item, idx) => (
              <Tag key={idx} style={{ marginBottom: "2px" }}>
                {item}
              </Tag>
            ))
          ) : (
            <Tag>None</Tag>
          )}
        </div>
      ),
    },
    {
      title: "Action",
      key: "action",
      width: 100,
      fixed: "right",
      render: (_, record) => (
        <Button
          onClick={() => {
            setSelectedNode(record); // Set the full processed node record
            setIsModalVisible(true);
          }}
          size="small"
        >
          View SQL
        </Button>
      ),
    },
  ];

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: 24 }}>
      <Title level={2}>KNIME Workflow to SQL Viewer</Title>
      {!processedNodes.length && ( // Show dragger only if no processed nodes
        <Card style={{ marginBottom: 24 }}>
          <Dragger
            name="file" // Important for AntD upload state tracking
            accept=".knwf"
            customRequest={({ file, onSuccess, onError }) => {
              // Wrap handleUpload in promise to handle async errors for AntD
              message.loading({
                content: `Processing ${file.name}...`,
                key: "upload",
              });
              Promise.resolve(handleUpload(file))
                .then(() => {
                  // onSuccess should be called by handleUpload indirectly setting state
                  // message.success({ content: `${file.name} uploaded successfully.`, key: 'upload' });
                  onSuccess("ok"); // Notify AntD upload is done
                })
                .catch((err) => {
                  message.error({
                    content: `Error processing ${file.name}.`,
                    key: "upload",
                  });
                  onError(err);
                });
            }}
            showUploadList={false}
            maxCount={1} // Allow only one file
            onChange={(info) => {
              // Handle internal state changes if needed
              if (info.file.status === "done") {
                // Already handled via message in customRequest's promise resolution
              } else if (info.file.status === "error") {
                // Already handled via message in customRequest's promise rejection
              }
            }}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">
              Click or drag a .knwf file to this area
            </p>
            <p className="ant-upload-hint">
              View workflow nodes and generated SQL for supported types.
            </p>
          </Dragger>
        </Card>
      )}
      {!!processedNodes.length && ( // Show table only if nodes are processed
        <Table
          dataSource={processedNodes}
          columns={columns}
          rowKey={(record) => record.id ?? record.nodeName + Math.random()}
          bordered
          size="small"
          scroll={{ x: 1000 }} // Adjust scroll width as needed
          pagination={{ pageSize: 15, size: "small" }} // Add pagination
        />
      )}
      <Modal
        open={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        footer={null}
        title={
          selectedNode
            ? `Generated SQL for "${selectedNode.nodeName}" (Node ${
                selectedNode.id
              }, Step ${
                selectedNode.order !== undefined
                  ? selectedNode.order + 1
                  : "N/A"
              })`
            : "Generated SQL"
        }
        width={800}
        destroyOnClose // Reset state when modal is closed
      >
        {selectedNode ? (
          <pre
            style={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
              backgroundColor: "#f5f5f5",
              padding: "15px",
              borderRadius: "4px",
              maxHeight: "60vh",
              overflowY: "auto",
              fontFamily: "monospace", // Use monospace font for SQL
            }}
          >
            {(() => {
              // *** UPDATED LOGIC TO GET CONTEXT ***
              // Find direct predecessors using the *processedNodes* state
              const predecessors = findAllPreviousNodes(
                selectedNode.id,
                processedNodes
              );
              const predecessorNames = predecessors.map(
                (n) => n.nodeName || `node_${n.id}_output`
              );

              // Filter processedNodes to get the context *before* the selected node
              // Nodes with order < selectedNode.order are considered processed before it.
              const contextForSQL = processedNodes.filter(
                (n) =>
                  n.order !== undefined &&
                  selectedNode.order !== undefined &&
                  n.order < selectedNode.order
              );

              // Call the central conversion function with the appropriate context
              try {
                // Pass the selected node's config (which includes its 'id'),
                // the derived predecessor names, and the filtered context.
                return convertSelectedNodeToSQL(
                  selectedNode.config, // Pass the whole selectedNode object (contains config, id, etc.)
                  predecessorNames,
                  contextForSQL,
                  selectedNode.id // Pass the filtered list of nodes processed *before* this one
                );
              } catch (error) {
                console.error("Error during SQL conversion:", error);
                return `Error generating SQL: ${
                  error.message || "Unknown error"
                }`;
              }
            })()}
          </pre>
        ) : (
          <p>No node selected or configuration available.</p>
        )}
      </Modal>
    </div>
  );
}

export default KnimeViewer;
