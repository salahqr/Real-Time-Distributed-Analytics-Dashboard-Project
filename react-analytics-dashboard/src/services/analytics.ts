import { api } from './api';
import { AnalyticsResponse } from '../models/analytics.models';

export async function getUserAnalytics(userId: string): Promise<AnalyticsResponse> {
  const { data } = await api.get<AnalyticsResponse>(`/user/${userId}/analytics`);
  return data;
}
