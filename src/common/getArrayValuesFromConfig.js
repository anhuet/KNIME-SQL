export const getArrayValuesFromConfig = (configNode) => {
  // ... (implementation remains the same) ...
  if (!configNode || !configNode.entry) {
    return [];
  }
  const entries = Array.isArray(configNode.entry)
    ? configNode.entry
    : [configNode.entry];
  return entries
    .filter(
      (e) =>
        e._attributes &&
        e._attributes.key !== "array-size" &&
        e._attributes.value
    )
    .map((e) => e._attributes.value);
};
