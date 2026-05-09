const BASE = process.env.NEXT_PUBLIC_BASE_PATH || ''

export function apiUrl(path: string): string {
  return `${BASE}${path}`
}
