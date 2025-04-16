export const findConfigByKey = (config, key) => {
  if (!config) return null;
  const nodes = Array.isArray(config) ? config : [config];
  return (
    nodes.find((node) => node._attributes && node._attributes.key === key) ||
    null
  );
};
