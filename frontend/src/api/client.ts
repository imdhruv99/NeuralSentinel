const BASE = import.meta.env.VITE_API_BASE_URL as string;
const KEY = import.meta.env.VITE_API_KEY as string;

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${BASE}${path}`, {
        ...init,
        headers: {
            'X-API-Key': KEY,
            ...(init?.headers || {}),
        },
    });
    if (!res.ok) {
        throw new Error(`API request failed with status ${res.status}: ${path}`);
    }
    return res.json() as Promise<T>;
}


export const API_BASE = BASE;
export const API_KEY = KEY;
