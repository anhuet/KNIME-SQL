/**
 * Utility function to get a value from a node's "entry" property.
 * The node.entry can be an array or a single object.
 *
 * @param {object} node - The node that contains an "entry" property.
 * @param {string} key - The key to search for.
 * @returns {string|null} - The found value or null if not found.
 */
function getEntryValue(node, key) {
  if (!node || !node.entry) return null;
  const entries = Array.isArray(node.entry) ? node.entry : [node.entry];
  const found = entries.find((e) => e._attributes && e._attributes.key === key);
  return found ? found._attributes.value : null;
}

/**
 * Utility function to find a config node by its _attributes.key.
 * The "config" parameter can be either an array or a single node.
 *
 * @param {object|array} config - A config node or an array of nodes.
 * @param {string} key - The key to search for.
 * @returns {object|null} - The matching node or null if not found.
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
 * Parses a KNIME workflow JSON (converted from workflow.knime XML) and builds a graph
 * structure of nodes and connections. Each node is augmented with:
 *  - order: a topologically sorted order (starting at 0 for nodes with no incoming connections)
 *  - nextNodes: an array of node IDs for downstream nodes.
 *
 * Expected structure (compact xml-js):
 * {
 *   "config": {
 *     "_attributes": { "key": "workflow.knime", ... },
 *     "config": [
 *       { "_attributes": { "key": "nodes" }, "config": [ ... node configs ... ] },
 *       { "_attributes": { "key": "connections" }, "config": [ ... connection configs ... ] },
 *       ...
 *     ]
 *   }
 * }
 *
 * @param {object} workflowJson - The workflow JSON.
 * @returns {object} - An object with:
 *    - nodes: an array of node objects (each with id, settingsFile, nodeType, nextNodes, order)
 *    - connections: an array of connection objects { sourceID, destID }
 */
export function parseWorkflowKnime(workflowJson) {
  // The root node is in workflowJson.config.
  const root = workflowJson.config;
  if (!root) {
    throw new Error("Invalid workflow JSON: Missing root config.");
  }

  // Get the nodes block.
  const nodesBlock = findConfigByKey(root.config, "nodes");
  if (!nodesBlock || !nodesBlock.config) {
    throw new Error("Nodes block not found in workflow JSON.");
  }
  const nodeConfigs = Array.isArray(nodesBlock.config)
    ? nodesBlock.config
    : [nodesBlock.config];

  // Process each node and build a node map keyed by node id.
  const nodes = [];
  const nodeMap = {};
  nodeConfigs.forEach((nc) => {
    const id = parseInt(getEntryValue(nc, "id"), 10);
    const settingsFile = getEntryValue(nc, "node_settings_file");
    const nodeType = getEntryValue(nc, "node_type");
    // Initialize node with empty nextNodes array.
    const node = { id, settingsFile, nodeType, nextNodes: [] };
    nodes.push(node);
    nodeMap[id] = node;
  });

  // Get the connections block.
  const connectionsBlock = findConfigByKey(root.config, "connections");
  if (!connectionsBlock || !connectionsBlock.config) {
    throw new Error("Connections block not found in workflow JSON.");
  }
  const connectionConfigs = Array.isArray(connectionsBlock.config)
    ? connectionsBlock.config
    : [connectionsBlock.config];
  const connections = [];
  connectionConfigs.forEach((cc) => {
    const sourceID = parseInt(getEntryValue(cc, "sourceID"), 10);
    const destID = parseInt(getEntryValue(cc, "destID"), 10);
    connections.push({ sourceID, destID });
  });

  // Build the graph: for each connection, add the destID to the source node's nextNodes.
  // Also build an inDegree mapping for topological sort.
  const inDegree = {};
  nodes.forEach((node) => {
    inDegree[node.id] = 0;
  });
  connections.forEach((conn) => {
    if (nodeMap[conn.sourceID]) {
      nodeMap[conn.sourceID].nextNodes.push(conn.destID);
    }
    inDegree[conn.destID] = (inDegree[conn.destID] || 0) + 1;
  });

  // Compute topological order using Kahn's algorithm.
  const queue = [];
  nodes.forEach((node) => {
    if (inDegree[node.id] === 0) {
      queue.push(node);
    }
  });
  let order = 0;
  const sortedNodes = [];
  while (queue.length > 0) {
    const current = queue.shift();
    current.order = order;
    order++;
    sortedNodes.push(current);
    current.nextNodes.forEach((nid) => {
      inDegree[nid]--;
      if (inDegree[nid] === 0) {
        queue.push(nodeMap[nid]);
      }
    });
  }

  if (sortedNodes.length !== nodes.length) {
    console.warn(
      "Warning: The workflow may contain cycles or disconnected parts."
    );
  }

  return { nodes: sortedNodes, connections };
}
