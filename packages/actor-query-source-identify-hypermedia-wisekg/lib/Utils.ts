import type { IActionContext } from '@comunica/types';

export const KEY_CONTEXT_WISEKG_FALLBACK = 'wisekgFallback';
export const KEY_CONTEXT_WISEKG_SOURCE = 'wisekgSource';
export const KEY_CONTEXT_WISEKG_SPEED_MBPS = 'wisekgSpeedMbps';
export const KEY_CONTEXT_WISEKG_LATENCY_MS = 'wisekgLatencyMs';
export const KEY_CONTEXT_WISEKG_BGP_HANDLED = 'wisekgBgpHandled';

export function normalizeUrl(url: string): string {
  return url.replace(/\/$/u, '');
}

export function getContextRaw<T = unknown>(context: IActionContext | undefined, key: string): T | undefined {
  if (!context) {
    return undefined;
  }
  if (typeof (context as any).getRaw === 'function') {
    const value = (context as any).getRaw(key);
    if (value !== undefined) {
      return value as T;
    }
  }
  return (context as unknown as Record<string, T | undefined>)[key];
}

export function setContextFlag(context: IActionContext, key: string, value: boolean): IActionContext {
  if (typeof (context as any).setRaw === 'function') {
    return (context as any).setRaw(key, value);
  }
  return context;
}

export function isContextFlagSet(context: unknown, key: string): boolean {
  if (!context) {
    return false;
  }

  const rawValue = typeof (context as any).getRaw === 'function' ?
    (context as any).getRaw(key) :
    undefined;
  if (rawValue !== undefined) {
    return Boolean(rawValue);
  }

  return Boolean((context as Record<string, unknown>)[key]);
}
