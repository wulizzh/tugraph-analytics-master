import React from "react";
import { GraphDefinitionConfigPanel } from "./graphDefinitionConfigPanel";
import { Form } from "antd";
import { isEmpty } from "lodash";

interface ClusterConfigProps {
  record: any;
  stageType: string;
  syncConfig: (params: any) => void;
  form: any;
}

const ClusterConfig: React.FC<ClusterConfigProps> = ({
  record,
  stageType,
  form,
}) => {
  let defaultFormValues = {};
  // 根据 currentItem 来判断是新增还是修改
  if (!isEmpty(record?.release?.clusterConfig)) {
    const configArr = [];
    for (const key in record?.release?.clusterConfig) {
      const current = record?.release?.clusterConfig[key];
      configArr.push({
        key,
        value: current,
      });
    }

    defaultFormValues = {
      clusterConfig: {
        type: record?.type,
        config: configArr,
      },
    };
  } else {
    defaultFormValues = {
      clusterConfig: {
        config: [],
      },
    };
  }

  return (
    <div>
      <Form initialValues={defaultFormValues} form={form}>
        <GraphDefinitionConfigPanel
          prefixName="clusterConfig"
          form={form}
          readonly={stageType !== "CREATED"}
        />
      </Form>
    </div>
  );
};

export default ClusterConfig;
