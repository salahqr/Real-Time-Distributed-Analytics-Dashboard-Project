import { api, tokenStore } from './api';

export interface MeResponse {
  id: string;
  name: string;
  email: string;
  company_name?: string | null;
}

export interface LoginResponse {
  access_token: string;
  token_type?: string;
  expires_in?: number;
}

export interface RegisterResponse {
  message: string;
}

const meStoreKey = 'me';

export const meStore = {
  get(): MeResponse | null {
    const raw = localStorage.getItem(meStoreKey);
    if (!raw) return null;
    try { return JSON.parse(raw); } catch { return null; }
  },
  set(me: MeResponse) { localStorage.setItem(meStoreKey, JSON.stringify(me)); },
  clear() { localStorage.removeItem(meStoreKey); }
};

export async function login(payload: { email: string; password: string }): Promise<LoginResponse> {
  const { data } = await api.post<LoginResponse>('/login', payload);
  if (data?.access_token) tokenStore.set(data.access_token);
  return data;
}

/**
 * ملاحظة مهمة: في الباك-إند الحالي، /api/register لا يعيد access_token.
 * لذلك التسجيل هنا لا يسجّل دخول تلقائيًا، والـ UI/Context قد يقوم بعمل login بعده.
 */
export async function register(payload: { name: string; company_name?: string | null; email: string; password: string; password_confirmation?: string }): Promise<RegisterResponse> {
  const { data } = await api.post<RegisterResponse>('/register', payload);
  return data;
}

export async function getMe(): Promise<MeResponse> {
  const { data } = await api.get<MeResponse>('/me');
  meStore.set(data);
  return data;
}

export async function forgotPassword(payload: { email: string }): Promise<any> {
  const { data } = await api.post('/password/forgot', payload);
  return data;
}

export async function resetPassword(payload: { token: string; email?: string; password: string; password_confirmation?: string }): Promise<any> {
  const { data } = await api.post('/password/reset', payload);
  return data;
}

export async function logout(): Promise<void> {
  try {
    await api.post('/logout', {});
  } catch {
    // ignore
  }
  tokenStore.clear();
  meStore.clear();
}

export function isAuthenticated(): boolean {
  return !!tokenStore.get();
}
