export type JsonObj = Record<string, unknown>;

export function parseJsonFromMixedOutput(output: string): JsonObj | null;
