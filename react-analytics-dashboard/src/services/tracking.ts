import { api } from './api';

export type TrackPayload = {
  tracking_id: string;
  page_url: string;
  event_type: string;
  session_id?: string;
};

export async function trackEvent(payload: TrackPayload) {
  const { data } = await api.post('/track', payload);
  return data;
}
