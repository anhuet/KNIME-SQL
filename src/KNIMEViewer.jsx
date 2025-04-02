import { InboxOutlined } from "@ant-design/icons";
import {
  Button,
  Card,
  message,
  Table,
  Tag,
  Typography,
  Upload,
  Modal,
} from "antd";
import JSZip from "jszip";
import React, { useEffect, useState } from "react";
import * as xmlJs from "xml-js";
import { convertCSVReaderNodeToSQL } from "./functions/convertCSVReaderNodeToSQL";
import { parseWorkflowKnime } from "./functions/parseWorkflowKnime";
import { convertColumnFilterNodeToSQL } from "./functions/convertColumnFilterNodeToSQL";

const { Dragger } = Upload;
const { Title } = Typography;

// Function to find the previous node
const findPreviousNode = (currentNodeId, nextNodeMap, nodes) => {
  for (const [sourceId, destIds] of Object.entries(nextNodeMap)) {
    if (destIds.includes(currentNodeId)) {
      return nodes.find((node) => String(node.id) === sourceId) || null;
    }
  }
  return null; // No previous node found
};

// Function to convert selected node to SQL
export function convertSelectedNodeToSQL(nodeConfig, previousNodeName) {
  const factory = getEntryValue(nodeConfig, "factory");
  if (!factory) {
    return "Invalid node configuration: missing factory value.";
  }

  switch (factory) {
    case "org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory":
      return convertCSVReaderNodeToSQL(nodeConfig);
    case "org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory":
      return convertColumnFilterNodeToSQL(
        nodeConfig,
        previousNodeName || "input_table"
      );
    default:
      return "Conversion for this node type is not supported.";
  }
}

// Utility function to get a value from an entry array.
const getEntryValue = (data, key) => {
  if (!data?.entry) return "";
  const entries = Array.isArray(data.entry) ? data.entry : [data.entry];
  const entry = entries.find((e) => e._attributes.key === key);
  return entry?._attributes?.value || "";
};

function KNIMEViewer() {
  const [selectedNode, setSelectedNode] = useState(null);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [nodeData, setNodeData] = useState([]);
  const [nextNodeMap, setNextNodeMap] = useState({}); // State to hold nextNodeMap

  const handleUpload = async (file) => {
    const zip = new JSZip();
    try {
      const zipContent = await zip.loadAsync(file);
      const allFiles = Object.keys(zipContent.files);

      const knimeFile = allFiles.find((filePath) => {
        const parts = filePath.split("/");
        return parts[parts.length - 1] === "workflow.knime";
      });
      if (!knimeFile) {
        message.error("workflow.knime not found in the .knwf file");
        return false;
      }
      const workflowXmlText = await zipContent.files[knimeFile].async("text");
      const workflowJson = JSON.parse(
        xmlJs.xml2json(workflowXmlText, { compact: true, spaces: 4 })
      );
      const { nodes, connections } = parseWorkflowKnime(workflowJson);

      const nodeOrderMap = {};
      nodes.forEach((node, index) => {
        nodeOrderMap[node.id] = index;
      });
      const nextNodeMap = {};
      connections.forEach((conn) => {
        if (!nextNodeMap[conn.sourceID]) {
          nextNodeMap[conn.sourceID] = [];
        }
        nextNodeMap[conn.sourceID].push(conn.destID);
      });
      setNextNodeMap(nextNodeMap); // Set the nextNodeMap state

      const xmlPaths = allFiles.filter((filePath) => {
        const parts = filePath.split("/");
        return parts.length > 1 && parts[parts.length - 1] === "settings.xml";
      });

      xmlPaths.forEach(async (item) => {
        const fileObj = zipContent.files[item];
        const fileText = await fileObj.async("text");
        const jsonObj = JSON.parse(
          xmlJs.xml2json(fileText, { compact: true, spaces: 4 })
        );
        const pathParts = item.split("/");
        const nodeFolder = pathParts[pathParts.length - 2];
        const match = nodeFolder.match(/#(\d+)\)?$/);
        const nodeId = match ? parseInt(match[1], 10) : null;

        const order =
          nodeId !== null && nodeOrderMap[nodeId] !== undefined
            ? nodeOrderMap[nodeId]
            : "N/A";
        const nextNodes =
          nodeId !== null && nextNodeMap[nodeId] !== undefined
            ? nextNodeMap[nodeId]
            : [];

        const node = {
          id: nodeId,
          nodeName: getEntryValue(jsonObj.config, "name"),
          nodeType: getEntryValue(jsonObj.config, "factory"),
          nodeStatus: getEntryValue(jsonObj.config, "state"),
          description: getEntryValue(jsonObj.config, "customDescription"),
          config: jsonObj.config,
          order,
          nextNodes,
        };
        setNodeData((prev) => [...prev, node]);
      });
    } catch (error) {
      message.error("Failed to process .knwf file");
    }
    return false; // Prevent default upload behavior
  };

  const formatNodeType = (fullType) => fullType.split(".").pop() || fullType;

  const sortedNodeData = [...nodeData].sort((a, b) => {
    if (a.order === "N/A") return 1;
    if (b.order === "N/A") return -1;
    return a.order - b.order;
  });

  const columns = [
    {
      title: "Step",
      dataIndex: "order",
      key: "order",
      width: 80,
      render: (order) => <span>{order + 1}</span>,
    },
    {
      title: "Node Id",
      dataIndex: "id",
      key: "id",
      width: 80,
      render: (id) => <span>{id}</span>,
    },
    {
      title: "Node Name",
      dataIndex: "nodeName",
      key: "nodeName",
    },
    {
      title: "Node Type",
      dataIndex: "nodeType",
      key: "nodeType",
      render: (text) => <code>{formatNodeType(text)}</code>,
    },
    {
      title: "Status",
      dataIndex: "nodeStatus",
      key: "nodeStatus",
      render: (status) => (
        <Tag color={status === "EXECUTED" ? "green" : "red"}>{status}</Tag>
      ),
    },
    {
      title: "Next Nodes",
      dataIndex: "nextNodes",
      key: "nextNodes",
      render: (nodes) => (
        <span>{Array.isArray(nodes) ? nodes.join(", ") : nodes}</span>
      ),
    },
    {
      title: "",
      dataIndex: "action",
      key: "action",
      render: (_, record) => (
        <Button
          onClick={() => {
            setSelectedNode(record);
            setIsModalVisible(true);
          }}
        >
          SQL
        </Button>
      ),
    },
  ];

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: 24 }}>
      <Title level={2}>KNIME Workflow Viewer</Title>
      {!nodeData.length && (
        <Card style={{ marginBottom: 24 }}>
          <Dragger
            accept=".knwf"
            customRequest={({ file, onSuccess }) => {
              handleUpload(file);
              setTimeout(() => onSuccess("ok"), 0);
            }}
            showUploadList={false}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">
              Click or drag .knwf file to this area
            </p>
            <p className="ant-upload-hint">
              We'll parse and show workflow JSON along with node order, node id
              and next nodes.
            </p>
          </Dragger>
        </Card>
      )}

      {!!nodeData.length && (
        <Table
          dataSource={sortedNodeData}
          columns={columns}
          rowKey="nodeName"
        />
      )}

      <Modal
        open={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        footer={null}
        title={`SQL for "${selectedNode?.nodeName}"`}
        width={800}
      >
        <pre style={{ whiteSpace: "pre-wrap" }}>
          {selectedNode?.config
            ? convertSelectedNodeToSQL(
                selectedNode.config,
                findPreviousNode(selectedNode.id, nextNodeMap, nodeData)
                  ?.nodeName || "input_table"
              )
            : "No config found."}
        </pre>
      </Modal>
    </div>
  );
}

export default KNIMEViewer;
