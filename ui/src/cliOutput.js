export function parseJsonFromMixedOutput(output) {
  const trimmed = output.trim();
  if (!trimmed) return null;
  try {
    const parsed = JSON.parse(trimmed);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    // Keep going and try to recover the trailing JSON object.
  }

  for (let i = trimmed.length - 1; i >= 0; i -= 1) {
    if (trimmed[i] !== '{') continue;
    const candidate = trimmed.slice(i);
    try {
      const parsed = JSON.parse(candidate);
      if (parsed && typeof parsed === 'object') {
        return parsed;
      }
    } catch {
      // Try an earlier opening brace.
    }
  }
  return null;
}
