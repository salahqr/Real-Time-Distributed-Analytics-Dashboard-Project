import { api } from './api';

export interface ApiUser {
  id: string;
  name?: string;
  email?: string;
  company_name?: string | null;
  created_at?: string;
}

export async function listUsers(): Promise<ApiUser[]> {
  const { data } = await api.get<ApiUser[]>('/users');
  return data;
}
