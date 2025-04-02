import React, { useState } from 'react';
import { Tabs, Table, Tag, Input, Button, Tooltip, Typography, Card } from 'antd';
import { EyeOutlined } from '@ant-design/icons';

const { TextArea } = Input;
const { Paragraph } = Typography;

const typeColorMap = {
  LoadCsv: 'magenta',
  LoadExcel: 'orange',
  Container: 'green',
};

const WorkflowTabs = ({ nodes }) => {
  const [nodeList, setNodeList] = useState(nodes || []);

  const handleDescriptionChange = (index, value) => {
    const updated = [...nodeList];
    updated[index].description = value;
    setNodeList(updated);
  };

  const workflowColumns = [
    {
      title: 'Name',
      dataIndex: 'name',
      render: (text) => <strong>{text}</strong>,
    },
    {
      title: 'Type',
      dataIndex: 'type',
      render: (type) => (
        <Tag color={typeColorMap[type] || 'default'}>{type}</Tag>
      ),
    },
    {
      title: 'Description',
      dataIndex: 'description',
      render: (text, record, index) => (
        <TextArea
          rows={1}
          maxLength={200}
          showCount
          placeholder="Add a Description"
          value={text}
          onChange={(e) =>
            handleDescriptionChange(index, e.target.value)
          }
        />
      ),
    },
    {
      title: 'Show Config',
      render: (_, record) => (
        <Tooltip title="View settings.xml content">
          <Button
            icon={<EyeOutlined />}
            onClick={() =>
              alert(JSON.stringify(record.config, null, 2))
            }
          />
        </Tooltip>
      ),
    },
  ];

  const nodeConfigColumns = [
    {
      title: 'Node File',
      dataIndex: 'xmlPath',
      render: (text) => <strong>{text}</strong>,
    },
    {
      title: '',
      dataIndex: 'config',
      render: (config) => (
        <pre style={{ whiteSpace: 'pre-wrap', maxHeight: 300, overflowY: 'auto' }}>
          {JSON.stringify(config, null, 2)}
        </pre>
      ),
    },
  ];

  return (
    <Card style={{ margin: 24 }}>
      <Tabs defaultActiveKey="1" type="card">
        <Tabs.TabPane tab="Workflow Summary" key="1">
          <Table
            dataSource={nodeList}
            columns={workflowColumns}
            pagination={false}
            rowKey="key"
            bordered
          />
        </Tabs.TabPane>

        <Tabs.TabPane tab="Node Configs (XML)" key="2">
          <Table
            dataSource={nodeList.map((n) => ({
              xmlPath: n.key,
              config: n.config,
            }))}
            columns={nodeConfigColumns}
            pagination={false}
            rowKey="xmlPath"
            bordered
          />
        </Tabs.TabPane>
      </Tabs>
    </Card>
  );
};

export default WorkflowTabs;
