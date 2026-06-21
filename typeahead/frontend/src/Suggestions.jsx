function highlightPrefix(text, prefix) {
  const lowerText = text.toLowerCase();
  const lowerPrefix = prefix.trim().toLowerCase();
  if (!lowerPrefix || !lowerText.startsWith(lowerPrefix)) return text;

  return (
    <>
      <strong>{text.slice(0, lowerPrefix.length)}</strong>
      {text.slice(lowerPrefix.length)}
    </>
  );
}

export default function Suggestions({ suggestions, query, server, selectedIndex, onSelect }) {
  return (
    <ul className="suggestions-list">
      {suggestions.map((suggestion, i) => (
        <li
          key={suggestion}
          className={`suggestion-item ${i === selectedIndex ? "highlighted" : ""}`}
          onMouseDown={() => onSelect(suggestion)}
        >
          <span className="suggestion-text">{highlightPrefix(suggestion, query)}</span>
          <span className="server-badge">{server}</span>
        </li>
      ))}
    </ul>
  );
}
