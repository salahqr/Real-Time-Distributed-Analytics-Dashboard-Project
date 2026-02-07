import { useEffect, useState } from "react";
import echo from "../echo";

export default function LiveDashboard() {
  const [events, setEvents] = useState([]);

  useEffect(() => {
    console.log("Listening to analytics-channel...");

    echo.channel("analytics-channel").listen(".analytics-event", (data) => {
      console.log("New Event Received:", data);

      setEvents((prev) => [data.eventData, ...prev]);
    });

    return () => {
      echo.leaveChannel("analytics-channel");
    };
  }, []);

  return (
    <div style={{ padding: "20px" }}>
      <h2>📡 Real-Time Analytics Dashboard</h2>

      {events.length === 0 && <p>No events yet...</p>}

      {events.map((event, index) => (
        <div
          key={index}
          style={{
            marginTop: "10px",
            padding: "10px",
            border: "1px solid gray",
          }}
        >
          <p><b>Type:</b> {event.event_type}</p>
          <p><b>Tracking ID:</b> {event.tracking_id}</p>
          <p><b>Page:</b> {event.page_url}</p>
          <p><b>User:</b> {event.user_id}</p>
          <p><b>Time:</b> {event.timestamp}</p>
        </div>
      ))}
    </div>
  );
}