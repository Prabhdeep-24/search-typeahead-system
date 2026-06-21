import { useEffect, useState } from "react";
import axios from "axios";

const API_BASE = "http://localhost:8000";
const REFRESH_MS = 30000;

export default function Trending({ onTermClick }) {
  const [trending, setTrending] = useState([]);
  const [error, setError] = useState(null);

  const fetchTrending = async () => {
    try {
      const res = await axios.get(`${API_BASE}/trending`);
      setTrending(res.data.trending);
      setError(null);
    } catch (err) {
      setError("Could not load trending searches.");
    }
  };

  useEffect(() => {
    fetchTrending();
    const interval = setInterval(fetchTrending, REFRESH_MS);
    return () => clearInterval(interval);
  }, []);

  return (
    <section className="trending">
      <h2>Trending Searches</h2>
      {error && <div className="error-message">{error}</div>}
      <ol className="trending-list">
        {trending.map((term, i) => (
          <li key={term} className="trending-item" onClick={() => onTermClick(term)}>
            <span className="trending-rank">{i + 1}</span>
            <span className="trending-text">{term}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
