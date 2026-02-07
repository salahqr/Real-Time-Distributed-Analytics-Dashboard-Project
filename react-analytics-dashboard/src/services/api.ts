import axios, { AxiosError, AxiosInstance, InternalAxiosRequestConfig } from 'axios';

// NOTE: اضبط VITE_API_URL في ملف .env
// مثال محلي للـ Laravel: http://127.0.0.1:8000/api
const API_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000/api';

export const tokenStore = {
  get: () => localStorage.getItem('access_token'),
  set: (t: string) => localStorage.setItem('access_token', t),
  clear: () => localStorage.removeItem('access_token'),
};

let isRefreshing = false;
let refreshQueue: Array<(token: string | null) => void> = [];

function subscribeTokenRefresh(cb: (token: string | null) => void) {
  refreshQueue.push(cb);
}

function onRefreshed(token: string | null) {
  refreshQueue.forEach((cb) => cb(token));
  refreshQueue = [];
}

export function createApiClient(): AxiosInstance {
  const client = axios.create({
    baseURL: API_URL,
    headers: {
      'Content-Type': 'application/json',
    },
  });

  client.interceptors.request.use((config: InternalAxiosRequestConfig) => {
    const token = tokenStore.get();
    if (token) {
      config.headers = config.headers ?? {};
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  });

  client.interceptors.response.use(
    (res) => res,
    async (error: AxiosError) => {
      const status = error.response?.status;
      const original = error.config as any;

      if (status === 401 && !original?._retry) {
        // try refresh once
        original._retry = true;

        if (isRefreshing) {
          return new Promise((resolve, reject) => {
            subscribeTokenRefresh((token) => {
              if (!token) return reject(error);
              original.headers.Authorization = `Bearer ${token}`;
              resolve(client.request(original));
            });
          });
        }

        isRefreshing = true;
        try {
          const resp = await axios.post(`${API_URL}/refresh`, {}, {
            headers: tokenStore.get() ? { Authorization: `Bearer ${tokenStore.get()}` } : {},
          });
          const newToken = (resp.data as any)?.access_token || (resp.data as any)?.token;
          if (newToken) {
            tokenStore.set(newToken);
            onRefreshed(newToken);
            original.headers.Authorization = `Bearer ${newToken}`;
            return client.request(original);
          }
          tokenStore.clear();
          onRefreshed(null);
          // لو فشل التحديث، ارجع المستخدم لصفحة التسجيل/الدخول
          if (typeof window !== 'undefined') window.location.href = '/login';
          return Promise.reject(error);
        } catch (e) {
          tokenStore.clear();
          onRefreshed(null);
          if (typeof window !== 'undefined') window.location.href = '/login';
          return Promise.reject(e);
        } finally {
          isRefreshing = false;
        }
      }

      return Promise.reject(error);
    }
  );

  return client;
}

export const api = createApiClient();
