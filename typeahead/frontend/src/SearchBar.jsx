import { useEffect, useRef, useState } from "react";
import axios from "axios";
import Suggestions from "./Suggestions";

const API_BASE = "http://localhost:8000";
const DEBOUNCE_MS = 150;
const MIN_PREFIX_LENGTH = 3;

export default function SearchBar({ presetTerm }) {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState([]);
  const [serverInfo, setServerInfo] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const [searchResult, setSearchResult] = useState(null);
  const debounceTimer = useRef(null);

  const fetchSuggestions = async (value) => {
    try {
      setLoading(true);
      setError(null);
      const res = await axios.get(`${API_BASE}/suggest`, { params: { q: value } });
      setSuggestions(res.data.suggestions);
      setServerInfo(res.data.server || "");
    } catch (err) {
      setError("Something went wrong. Try again.");
      setSuggestions([]);
    } finally {
      setLoading(false);
    }
  };

  const handleInputChange = (e) => {
    const value = e.target.value;
    setQuery(value);
    setSelectedIndex(-1);
    setSearchResult(null);

    if (debounceTimer.current) clearTimeout(debounceTimer.current);

    if (value.trim().length >= MIN_PREFIX_LENGTH) {
      debounceTimer.current = setTimeout(() => fetchSuggestions(value), DEBOUNCE_MS);
    } else {
      setSuggestions([]);
      setServerInfo("");
      setError(null);
    }
  };

  // Clicking a trending term fills the search bar and fetches suggestions,
  // same as if the user had typed it.
  useEffect(() => {
    if (!presetTerm) return;
    setQuery(presetTerm.term);
    setSelectedIndex(-1);
    setSearchResult(null);
    if (presetTerm.term.trim().length >= MIN_PREFIX_LENGTH) {
      fetchSuggestions(presetTerm.term);
    }
  }, [presetTerm]);

  const handleSubmit = async (selectedQuery) => {
    const q = (selectedQuery ?? query).trim();
    if (!q) return;

    setSuggestions([]);
    setQuery(q);
    try {
      const res = await axios.post(`${API_BASE}/search`, null, { params: { q } });
      setSearchResult(res.data.message);
    } catch (err) {
      setError("Search failed.");
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.min(prev + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.max(prev - 1, -1));
    } else if (e.key === "Escape") {
      setSuggestions([]);
      setSelectedIndex(-1);
    } else if (e.key === "Enter") {
      const q = selectedIndex >= 0 ? suggestions[selectedIndex] : query;
      handleSubmit(q);
    }
  };

  return (
    <div className="search-bar-container">
      <div className="search-bar">
        <input
          type="text"
          className="search-input"
          placeholder="Search Wikipedia..."
          value={query}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          autoFocus
        />
        <button className="search-button" onClick={() => handleSubmit()}>
          {loading ? <span className="spinner" /> : "Search"}
        </button>
      </div>

      {error && <div className="error-message">{error}</div>}

      {searchResult && (
        <div className="search-result-banner">
          {searchResult} — "{query}"
        </div>
      )}

      {suggestions.length > 0 && (
        <Suggestions
          suggestions={suggestions}
          query={query}
          server={serverInfo}
          selectedIndex={selectedIndex}
          onSelect={handleSubmit}
        />
      )}
    </div>
  );
}
