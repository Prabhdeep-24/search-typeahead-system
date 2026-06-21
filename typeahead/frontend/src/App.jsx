import { useEffect, useState } from "react";
import SearchBar from "./SearchBar";
import Trending from "./Trending";
import Stats from "./Stats";
import "./App.css";

export default function App() {
  const [refreshKey, setRefreshKey] = useState(0);
  const [presetTerm, setPresetTerm] = useState(null);

  useEffect(() => {
    const interval = setInterval(() => setRefreshKey((k) => k + 1), 5000);
    return () => clearInterval(interval);
  }, []);

  // Wrap in {term, ts} so clicking the same trending term twice in a row
  // still re-triggers the effect in SearchBar (ts always changes).
  const handleTrendingClick = (term) => setPresetTerm({ term, ts: Date.now() });

  return (
    <div className="app">
      <header className="app-header">
        <h1>Search Typeahead</h1>
        <p className="subtitle">Distributed cache · consistent hashing · batch writes</p>
      </header>

      <main className="app-main">
        <SearchBar presetTerm={presetTerm} />
        <Trending onTermClick={handleTrendingClick} />
      </main>

      <Stats refreshKey={refreshKey} />
    </div>
  );
}
