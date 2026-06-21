import { useEffect, useState } from "react";
import axios from "axios";

const API_BASE = "http://localhost:8000";

export default function Stats({ refreshKey }) {
  const [status, setStatus] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    axios
      .get(`${API_BASE}/servers/status`)
      .then((res) => {
        setStatus(res.data);
        setError(null);
      })
      .catch(() => setError("Stats unavailable - is the backend running?"));
  }, [refreshKey]);

  if (error) {
    return (
      <footer className="stats">
        <div className="error-message">{error}</div>
      </footer>
    );
  }

  if (!status) return null;

  const maxCount = Math.max(1, ...Object.values(status.distribution));

  return (
    <footer className="stats">
      <div className="stats-row">
        <div className="stats-block">
          <h3>Cache Servers</h3>
          <div className="bars">
            {Object.entries(status.distribution).map(([server, count]) => (
              <div key={server} className="bar-row">
                <span className="bar-label">{server}</span>
                <div className="bar-track">
                  <div
                    className="bar-fill"
                    style={{ width: `${(count / maxCount) * 100}%` }}
                  />
                </div>
                <span className="bar-value">{count.toLocaleString()}</span>
              </div>
            ))}
          </div>
          <p className="stats-footnote">
            {status.total_prefixes.toLocaleString()} prefixes ·{" "}
            {status.virtual_nodes_per_server} virtual nodes/server
          </p>
        </div>

        <div className="stats-block">
          <h3>Cache Hit Rate</h3>
          <div className="big-number">{status.cache_hit_rate_percent}%</div>
          <p className="stats-footnote">
            {status.cache_hits.toLocaleString()} hits / {status.cache_misses.toLocaleString()} misses
          </p>
        </div>

        <div className="stats-block">
          <h3>Write Buffer</h3>
          <div className="big-number">{status.buffer_pending_queries}</div>
          <p className="stats-footnote">
            distinct queries pending ({status.buffer_pending_total} searches) before next flush
          </p>
        </div>
      </div>
    </footer>
  );
}
