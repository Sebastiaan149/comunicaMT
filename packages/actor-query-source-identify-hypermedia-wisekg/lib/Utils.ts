import type { IActionContext } from '@comunica/types';

export const KEY_CONTEXT_WISEKG_FALLBACK = 'wisekgFallback';
export const KEY_CONTEXT_WISEKG_SOURCE = 'wisekgSource';
export const KEY_CONTEXT_WISEKG_SPEED_MBPS = 'wisekgSpeedMbps';
export const KEY_CONTEXT_WISEKG_LATENCY_MS = 'wisekgLatencyMs';
export const KEY_CONTEXT_WISEKG_BGP_HANDLED = 'wisekgBgpHandled';

// Normalize dataset URLs so cache keys and comparisons stay stable.
export function normalizeUrl(url: string): string {
  return url.replace(/\/$/u, '');
}

// Read raw context values from Comunica ActionContext or plain objects.
export function getContextRaw<T = unknown>(context: IActionContext | undefined, key: string): T | undefined {
  if (!context) {
    return undefined;
  }
  if (typeof (<any> context).getRaw === 'function') {
    const value = (<any> context).getRaw(key);
    if (value !== undefined) {
      return <T> value;
    }
  }
  return (<Record<string, T | undefined>> <unknown> context)[key];
}

// Set a boolean flag when the context supports raw values.
export function setContextFlag(context: IActionContext, key: string, value: boolean): IActionContext {
  if (typeof (<any> context).setRaw === 'function') {
    return (<any> context).setRaw(key, value);
  }
  return context;
}

// Read boolean flags from Comunica ActionContext or plain objects.
export function isContextFlagSet(context: unknown, key: string): boolean {
  if (!context) {
    return false;
  }

  const rawValue = typeof (<any> context).getRaw === 'function' ?
      (<any> context).getRaw(key) :
    undefined;
  if (rawValue !== undefined) {
    return Boolean(rawValue);
  }

  return Boolean((<Record<string, unknown>> context)[key]);
}
