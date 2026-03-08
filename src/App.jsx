import { useState, useCallback, useRef, useEffect } from "react";
import * as XLSX from "xlsx";
import Papa from "papaparse";

// ─── Palette & Globals ────────────────────────────────────────────────────────
const FONTS = `@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap');`;

const style = `
  ${FONTS}
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0b0c0f;
    --surface: #111318;
    --surface2: #181b22;
    --border: #1f2330;
    --border2: #2a2f40;
    --accent: #4af0a0;
    --accent2: #f0a04a;
    --accent3: #4a9ef0;
    --danger: #f04a6a;
    --warn: #f0c04a;
    --text: #e8eaf2;
    --text2: #8b90a8;
    --text3: #555a72;
    --font-head: 'Syne', sans-serif;
    --font-mono: 'DM Mono', monospace;
    --radius: 6px;
    --shadow: 0 4px 24px rgba(0,0,0,0.5);
  }
  body { background: var(--bg); color: var(--text); font-family: var(--font-mono); }
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
`;

// ─── QC Engine (Pure JS) ──────────────────────────────────────────────────────

function inferTypes(df) {
  const types = {};
  df.columns.forEach(col => {
    const vals = df.rows.map(r => r[col]).filter(v => v !== null && v !== undefined && v !== "");
    const nums = vals.filter(v => !isNaN(Number(v)));
    types[col] = nums.length / Math.max(vals.length, 1) > 0.7 ? "numeric" : "categorical";
  });
  return types;
}

function runQC(df, rules) {
  const flags = [];
  const types = inferTypes(df);
  const rows = df.rows;

  // 1. Missing value check
  df.columns.forEach(col => {
    const missing = rows.filter(r => r[col] === null || r[col] === undefined || r[col] === "");
    const rate = missing.length / rows.length;
    if (rate > 0) {
      flags.push({
        check: "Missing Values",
        column: col,
        severity: rate > 0.2 ? "critical" : rate > 0.05 ? "warning" : "info",
        count: missing.length,
        rate: (rate * 100).toFixed(1) + "%",
        detail: `${missing.length} missing values (${(rate * 100).toFixed(1)}%)`,
      });
    }
  });

  // 2. Range checks (numeric columns — z-score outliers)
  df.columns.forEach(col => {
    if (types[col] !== "numeric") return;
    const vals = rows.map(r => Number(r[col])).filter(v => !isNaN(v));
    if (vals.length < 4) return;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const std = Math.sqrt(vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length);
    if (std === 0) return;
    const outliers = rows.filter(r => {
      const v = Number(r[col]);
      return !isNaN(v) && Math.abs((v - mean) / std) > 3;
    });
    if (outliers.length > 0) {
      flags.push({
        check: "Statistical Outliers",
        column: col,
        severity: "warning",
        count: outliers.length,
        rate: ((outliers.length / rows.length) * 100).toFixed(1) + "%",
        detail: `${outliers.length} values >3σ from mean (μ=${mean.toFixed(2)}, σ=${std.toFixed(2)})`,
      });
    }
  });

  // 3. Duplicate row check
  const seen = new Set();
  let dupeCount = 0;
  rows.forEach(r => {
    const key = JSON.stringify(r);
    if (seen.has(key)) dupeCount++;
    seen.add(key);
  });
  if (dupeCount > 0) {
    flags.push({
      check: "Duplicate Records",
      column: "ALL",
      severity: "critical",
      count: dupeCount,
      rate: ((dupeCount / rows.length) * 100).toFixed(1) + "%",
      detail: `${dupeCount} exact duplicate rows detected`,
    });
  }

  // 4. Constant columns (no variation)
  df.columns.forEach(col => {
    const unique = new Set(rows.map(r => r[col])).size;
    if (unique === 1) {
      flags.push({
        check: "Constant Column",
        column: col,
        severity: "info",
        count: rows.length,
        rate: "100%",
        detail: `Column has no variation — all values identical`,
      });
    }
  });

  // 5. High cardinality check for short datasets
  df.columns.forEach(col => {
    if (types[col] !== "categorical") return;
    const unique = new Set(rows.map(r => r[col])).size;
    if (unique / rows.length > 0.95 && rows.length > 20) {
      flags.push({
        check: "High Cardinality",
        column: col,
        severity: "info",
        count: unique,
        rate: ((unique / rows.length) * 100).toFixed(0) + "%",
        detail: `${unique} unique values out of ${rows.length} rows — may be an ID column`,
      });
    }
  });

  // 6. Custom logic rules
  if (rules && rules.length > 0) {
    rules.forEach(rule => {
      if (!rule.ifCol || !rule.ifVal || !rule.thenCol) return;
      const violations = rows.filter(r => {
        const trigger = String(r[rule.ifCol]).toLowerCase() === String(rule.ifVal).toLowerCase();
        if (!trigger) return false;
        if (rule.thenCondition === "must_be_null") return r[rule.thenCol] !== null && r[rule.thenCol] !== undefined && r[rule.thenCol] !== "";
        if (rule.thenCondition === "must_not_be_null") return r[rule.thenCol] === null || r[rule.thenCol] === undefined || r[rule.thenCol] === "";
        return false;
      });
      if (violations.length > 0) {
        flags.push({
          check: "Logic Violation",
          column: `${rule.ifCol} → ${rule.thenCol}`,
          severity: "critical",
          count: violations.length,
          rate: ((violations.length / rows.length) * 100).toFixed(1) + "%",
          detail: `${violations.length} rows: when ${rule.ifCol}="${rule.ifVal}", ${rule.thenCol} ${rule.thenCondition.replace(/_/g, " ")}`,
        });
      }
    });
  }

  return { flags, types };
}

function computeEDA(df, types) {
  const stats = {};
  df.columns.forEach(col => {
    const vals = df.rows.map(r => r[col]).filter(v => v !== null && v !== undefined && v !== "");
    const missing = df.rows.length - vals.length;
    if (types[col] === "numeric") {
      const nums = vals.map(Number).filter(v => !isNaN(v)).sort((a, b) => a - b);
      const mean = nums.reduce((a, b) => a + b, 0) / Math.max(nums.length, 1);
      const median = nums[Math.floor(nums.length / 2)] ?? null;
      const std = Math.sqrt(nums.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(nums.length, 1));
      const q1 = nums[Math.floor(nums.length * 0.25)] ?? null;
      const q3 = nums[Math.floor(nums.length * 0.75)] ?? null;
      stats[col] = { type: "numeric", count: nums.length, missing, mean: mean.toFixed(2), median, std: std.toFixed(2), min: nums[0], max: nums[nums.length - 1], q1, q3, hist: buildHist(nums, 8) };
    } else {
      const freq = {};
      vals.forEach(v => { freq[v] = (freq[v] || 0) + 1; });
      const sorted = Object.entries(freq).sort((a, b) => b[1] - a[1]).slice(0, 8);
      stats[col] = { type: "categorical", count: vals.length, missing, unique: Object.keys(freq).length, topValues: sorted };
    }
  });
  return stats;
}

function buildHist(nums, bins) {
  if (!nums.length) return [];
  const min = nums[0], max = nums[nums.length - 1];
  if (min === max) return [{ label: String(min), count: nums.length }];
  const step = (max - min) / bins;
  const buckets = Array.from({ length: bins }, (_, i) => ({ label: (min + i * step).toFixed(1), count: 0 }));
  nums.forEach(n => {
    const idx = Math.min(Math.floor((n - min) / step), bins - 1);
    buckets[idx].count++;
  });
  return buckets;
}

// ─── File Parser ─────────────────────────────────────────────────────────────

async function parseFile(file) {
  return new Promise((resolve, reject) => {
    const ext = file.name.split(".").pop().toLowerCase();
    if (ext === "csv") {
      Papa.parse(file, {
        header: true, skipEmptyLines: true,
        complete: r => {
          const rows = r.data.map(row => {
            const clean = {};
            Object.keys(row).forEach(k => { clean[k] = row[k] === "" ? null : row[k]; });
            return clean;
          });
          resolve({ columns: r.meta.fields, rows });
        },
        error: reject,
      });
    } else if (ext === "xlsx" || ext === "xls") {
      const reader = new FileReader();
      reader.onload = e => {
        const wb = XLSX.read(e.target.result, { type: "array" });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const data = XLSX.utils.sheet_to_json(ws, { defval: null });
        if (!data.length) return reject(new Error("Empty sheet"));
        resolve({ columns: Object.keys(data[0]), rows: data });
      };
      reader.onerror = reject;
      reader.readAsArrayBuffer(file);
    } else {
      reject(new Error("Unsupported format. Use CSV or XLSX."));
    }
  });
}

// ─── Tiny Components ─────────────────────────────────────────────────────────

const Badge = ({ children, color = "accent" }) => {
  const colors = { accent: "#4af0a0", warn: "#f0c04a", danger: "#f04a6a", info: "#4a9ef0", text: "#8b90a8" };
  return (
    <span style={{
      display: "inline-block", padding: "2px 8px", borderRadius: 3,
      fontSize: 10, fontFamily: "var(--font-mono)", fontWeight: 500, letterSpacing: "0.08em",
      background: colors[color] + "18", color: colors[color], border: `1px solid ${colors[color]}30`,
    }}>{children}</span>
  );
};

const severityColor = s => s === "critical" ? "danger" : s === "warning" ? "warn" : "info";

const MiniBar = ({ value, max, color = "#4af0a0" }) => {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div style={{ background: "#1f2330", borderRadius: 2, height: 6, width: "100%", overflow: "hidden" }}>
      <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 2, transition: "width 0.6s ease" }} />
    </div>
  );
};

// ─── Histogram ────────────────────────────────────────────────────────────────

const Histogram = ({ data, color = "#4af0a0" }) => {
  if (!data || !data.length) return null;
  const max = Math.max(...data.map(d => d.count));
  return (
    <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: 48, width: "100%", marginTop: 8 }}>
      {data.map((d, i) => (
        <div key={i} title={`${d.label}: ${d.count}`}
          style={{
            flex: 1, background: color + "60", borderRadius: "2px 2px 0 0",
            height: `${max > 0 ? (d.count / max) * 100 : 0}%`,
            minHeight: d.count > 0 ? 3 : 0,
            border: `1px solid ${color}40`, cursor: "default",
            transition: "height 0.4s ease",
          }} />
      ))}
    </div>
  );
};

// ─── Main App ─────────────────────────────────────────────────────────────────

export default function App() {
  const [files, setFiles] = useState([]);
  const [activeFile, setActiveFile] = useState(null);
  const [tab, setTab] = useState("qc"); // "qc" | "eda" | "data"
  const [rules, setRules] = useState([]);
  const [newRule, setNewRule] = useState({ ifCol: "", ifVal: "", thenCol: "", thenCondition: "must_be_null" });
  const [dragging, setDragging] = useState(false);
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");
  const [dataPage, setDataPage] = useState(0);
  const dropRef = useRef();
  const PAGE_SIZE = 20;

  const processFile = async (file) => {
    try {
      const df = await parseFile(file);
      const { flags, types } = runQC(df, rules);
      const eda = computeEDA(df, types);
      const entry = { id: Date.now() + Math.random(), name: file.name, df, flags, types, eda, size: file.size };
      setFiles(prev => [...prev, entry]);
      setActiveFile(entry);
      setTab("qc");
      setDataPage(0);
    } catch (e) {
      alert("Error loading file: " + e.message);
    }
  };

  const onDrop = useCallback(e => {
    e.preventDefault();
    setDragging(false);
    Array.from(e.dataTransfer.files).forEach(processFile);
  }, [rules]);

  const onFileInput = e => {
    Array.from(e.target.files).forEach(processFile);
    e.target.value = "";
  };

  const addRule = () => {
    if (!newRule.ifCol || !newRule.ifVal || !newRule.thenCol) return;
    setRules(prev => [...prev, { ...newRule, id: Date.now() }]);
    setNewRule({ ifCol: "", ifVal: "", thenCol: "", thenCondition: "must_be_null" });
  };

  const rerunQC = (fileEntry) => {
    const { flags, types } = runQC(fileEntry.df, rules);
    const eda = computeEDA(fileEntry.df, types);
    const updated = { ...fileEntry, flags, types, eda };
    setFiles(prev => prev.map(f => f.id === fileEntry.id ? updated : f));
    if (activeFile?.id === fileEntry.id) setActiveFile(updated);
  };

  const exportReport = () => {
    if (!activeFile) return;
    const rows = activeFile.flags.map(f => ({
      Check: f.check, Column: f.column, Severity: f.severity,
      Count: f.count, Rate: f.rate, Detail: f.detail,
    }));
    const ws = XLSX.utils.json_to_sheet(rows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "QC Flags");

    // EDA summary
    const edaRows = Object.entries(activeFile.eda).map(([col, s]) => ({
      Column: col, Type: s.type, Count: s.count, Missing: s.missing,
      Mean: s.mean ?? "", Median: s.median ?? "", StdDev: s.std ?? "",
      Min: s.min ?? "", Max: s.max ?? "", UniqueValues: s.unique ?? "",
    }));
    const ws2 = XLSX.utils.json_to_sheet(edaRows);
    XLSX.utils.book_append_sheet(wb, ws2, "EDA Summary");

    XLSX.writeFile(wb, `QC_Report_${activeFile.name.replace(/\.\w+$/, "")}.xlsx`);
  };

  const f = activeFile;

  // Sorted + paged data view
  const sortedRows = f ? [...f.df.rows].sort((a, b) => {
    if (!sortCol) return 0;
    const av = a[sortCol], bv = b[sortCol];
    const an = Number(av), bn = Number(bv);
    const numSort = !isNaN(an) && !isNaN(bn) ? an - bn : String(av ?? "").localeCompare(String(bv ?? ""));
    return sortDir === "asc" ? numSort : -numSort;
  }) : [];
  const pagedRows = sortedRows.slice(dataPage * PAGE_SIZE, (dataPage + 1) * PAGE_SIZE);
  const totalPages = f ? Math.ceil(f.df.rows.length / PAGE_SIZE) : 0;

  const critCount = f ? f.flags.filter(x => x.severity === "critical").length : 0;
  const warnCount = f ? f.flags.filter(x => x.severity === "warning").length : 0;
  const infoCount = f ? f.flags.filter(x => x.severity === "info").length : 0;

  return (
    <div style={{ minHeight: "100vh", background: "var(--bg)", display: "flex", flexDirection: "column" }}>
      <style>{style}</style>

      {/* ── Header ── */}
      <header style={{
        borderBottom: "1px solid var(--border)", padding: "14px 28px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        background: "var(--surface)", position: "sticky", top: 0, zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            width: 32, height: 32, background: "var(--accent)", borderRadius: 6,
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#0b0c0f" strokeWidth="2.5" strokeLinecap="round">
              <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
            </svg>
          </div>
          <div>
            <div style={{ fontFamily: "var(--font-head)", fontWeight: 800, fontSize: 15, letterSpacing: "0.02em", color: "var(--text)" }}>QC ENGINE</div>
            <div style={{ fontSize: 10, color: "var(--text3)", letterSpacing: "0.1em" }}>SURVEY QUALITY CONTROL</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {f && (
            <button onClick={exportReport} style={{
              background: "var(--accent)", color: "#0b0c0f", border: "none", borderRadius: "var(--radius)",
              padding: "7px 14px", fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 500, cursor: "pointer",
              letterSpacing: "0.05em",
            }}>↓ Export Report</button>
          )}
          <label style={{
            background: "var(--surface2)", border: "1px solid var(--border2)", borderRadius: "var(--radius)",
            padding: "7px 14px", fontFamily: "var(--font-mono)", fontSize: 11, cursor: "pointer", color: "var(--text2)",
          }}>
            + Add File
            <input type="file" accept=".csv,.xlsx,.xls" multiple onChange={onFileInput} style={{ display: "none" }} />
          </label>
        </div>
      </header>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>

        {/* ── Sidebar ── */}
        <aside style={{
          width: 220, background: "var(--surface)", borderRight: "1px solid var(--border)",
          display: "flex", flexDirection: "column", overflowY: "auto",
        }}>
          {/* File list */}
          <div style={{ padding: "12px 0" }}>
            <div style={{ padding: "0 16px 8px", fontSize: 9, letterSpacing: "0.12em", color: "var(--text3)", fontWeight: 500 }}>FILES</div>
            {files.length === 0 && (
              <div style={{ padding: "8px 16px", fontSize: 11, color: "var(--text3)" }}>No files loaded</div>
            )}
            {files.map(file => (
              <div key={file.id}
                onClick={() => { setActiveFile(file); setTab("qc"); setDataPage(0); }}
                style={{
                  padding: "8px 16px", cursor: "pointer", borderLeft: `2px solid ${activeFile?.id === file.id ? "var(--accent)" : "transparent"}`,
                  background: activeFile?.id === file.id ? "var(--surface2)" : "transparent",
                  transition: "all 0.15s",
                }}>
                <div style={{ fontSize: 11, color: activeFile?.id === file.id ? "var(--text)" : "var(--text2)", wordBreak: "break-all", lineHeight: 1.4 }}>{file.name}</div>
                <div style={{ fontSize: 10, color: "var(--text3)", marginTop: 2 }}>
                  {file.df.rows.length} rows · {file.flags.filter(f => f.severity === "critical").length} crit
                </div>
              </div>
            ))}
          </div>

          <div style={{ borderTop: "1px solid var(--border)", padding: "12px 0" }}>
            <div style={{ padding: "0 16px 8px", fontSize: 9, letterSpacing: "0.12em", color: "var(--text3)", fontWeight: 500 }}>LOGIC RULES</div>
            {rules.map((r, i) => (
              <div key={r.id} style={{ padding: "6px 16px", fontSize: 10, color: "var(--text2)", lineHeight: 1.5 }}>
                <span style={{ color: "var(--accent3)" }}>{r.ifCol}={r.ifVal}</span>
                <span style={{ color: "var(--text3)" }}> → </span>
                <span style={{ color: "var(--warn)" }}>{r.thenCol} {r.thenCondition.replace(/_/g, " ")}</span>
                <span onClick={() => setRules(prev => prev.filter((_, j) => j !== i))}
                  style={{ marginLeft: 6, color: "var(--danger)", cursor: "pointer", fontSize: 11 }}>×</span>
              </div>
            ))}
            <div style={{ padding: "6px 16px", display: "flex", flexDirection: "column", gap: 4 }}>
              {["ifCol", "ifVal", "thenCol"].map(k => (
                <input key={k} placeholder={k === "ifCol" ? "if column" : k === "ifVal" ? "if value" : "then column"}
                  value={newRule[k]}
                  onChange={e => setNewRule(p => ({ ...p, [k]: e.target.value }))}
                  style={{
                    background: "var(--surface2)", border: "1px solid var(--border)", borderRadius: 3,
                    padding: "4px 6px", fontSize: 10, color: "var(--text)", fontFamily: "var(--font-mono)", outline: "none",
                  }} />
              ))}
              <select value={newRule.thenCondition}
                onChange={e => setNewRule(p => ({ ...p, thenCondition: e.target.value }))}
                style={{
                  background: "var(--surface2)", border: "1px solid var(--border)", borderRadius: 3,
                  padding: "4px 6px", fontSize: 10, color: "var(--text2)", fontFamily: "var(--font-mono)",
                }}>
                <option value="must_be_null">must be null</option>
                <option value="must_not_be_null">must not be null</option>
              </select>
              <button onClick={addRule} style={{
                background: "var(--surface2)", border: "1px solid var(--border2)", borderRadius: 3,
                padding: "5px", fontSize: 10, color: "var(--accent)", cursor: "pointer", fontFamily: "var(--font-mono)",
              }}>+ Add Rule</button>
              {files.length > 0 && rules.length > 0 && (
                <button onClick={() => { if (activeFile) rerunQC(activeFile); }} style={{
                  background: "var(--accent)18", border: "1px solid var(--accent)40", borderRadius: 3,
                  padding: "5px", fontSize: 10, color: "var(--accent)", cursor: "pointer", fontFamily: "var(--font-mono)",
                }}>↺ Rerun QC</button>
              )}
            </div>
          </div>
        </aside>

        {/* ── Main Content ── */}
        <main style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>

          {/* Drop zone or content */}
          {!f ? (
            <div
              ref={dropRef}
              onDragOver={e => { e.preventDefault(); setDragging(true); }}
              onDragLeave={() => setDragging(false)}
              onDrop={onDrop}
              style={{
                flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                border: `2px dashed ${dragging ? "var(--accent)" : "var(--border)"}`,
                margin: 32, borderRadius: 12, cursor: "pointer", transition: "border 0.2s",
                background: dragging ? "var(--accent)05" : "transparent",
              }}>
              <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke={dragging ? "#4af0a0" : "#555a72"} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="17 8 12 3 7 8" />
                <line x1="12" y1="3" x2="12" y2="15" />
              </svg>
              <div style={{ marginTop: 16, fontFamily: "var(--font-head)", fontSize: 18, fontWeight: 700, color: "var(--text2)" }}>
                Drop files here
              </div>
              <div style={{ marginTop: 6, fontSize: 12, color: "var(--text3)" }}>Supports CSV, XLSX, XLS</div>
              <label style={{
                marginTop: 20, background: "var(--surface2)", border: "1px solid var(--border2)", borderRadius: "var(--radius)",
                padding: "9px 20px", fontSize: 12, cursor: "pointer", color: "var(--text2)", fontFamily: "var(--font-mono)",
              }}>
                Browse files
                <input type="file" accept=".csv,.xlsx,.xls" multiple onChange={onFileInput} style={{ display: "none" }} />
              </label>
            </div>
          ) : (
            <>
              {/* Tabs + stats bar */}
              <div style={{
                borderBottom: "1px solid var(--border)", padding: "0 24px",
                display: "flex", alignItems: "center", gap: 0, background: "var(--surface)",
              }}>
                {["qc", "eda", "data"].map(t => (
                  <button key={t} onClick={() => setTab(t)} style={{
                    padding: "12px 18px", border: "none", background: "transparent",
                    borderBottom: `2px solid ${tab === t ? "var(--accent)" : "transparent"}`,
                    color: tab === t ? "var(--text)" : "var(--text3)", fontFamily: "var(--font-mono)",
                    fontSize: 11, letterSpacing: "0.08em", cursor: "pointer",
                    textTransform: "uppercase", fontWeight: tab === t ? 500 : 400,
                  }}>
                    {t === "qc" ? "QC Report" : t === "eda" ? "EDA" : "Data Preview"}
                  </button>
                ))}
                <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
                  <Badge color="danger">{critCount} critical</Badge>
                  <Badge color="warn">{warnCount} warn</Badge>
                  <Badge color="info">{infoCount} info</Badge>
                  <span style={{ fontSize: 10, color: "var(--text3)", marginLeft: 4 }}>
                    {f.df.rows.length.toLocaleString()} rows · {f.df.columns.length} cols
                  </span>
                </div>
              </div>

              <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>

                {/* ── QC Tab ── */}
                {tab === "qc" && (
                  <div>
                    {/* Score card */}
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 24 }}>
                      {[
                        { label: "Total Flags", val: f.flags.length, color: "var(--text)", bg: "var(--surface2)" },
                        { label: "Critical", val: critCount, color: "#f04a6a", bg: "#f04a6a08" },
                        { label: "Warnings", val: warnCount, color: "#f0c04a", bg: "#f0c04a08" },
                        { label: "Info", val: infoCount, color: "#4a9ef0", bg: "#4a9ef008" },
                      ].map(c => (
                        <div key={c.label} style={{
                          background: c.bg, border: `1px solid var(--border)`, borderRadius: "var(--radius)",
                          padding: "16px 20px",
                        }}>
                          <div style={{ fontSize: 10, color: "var(--text3)", letterSpacing: "0.1em", textTransform: "uppercase" }}>{c.label}</div>
                          <div style={{ fontSize: 32, fontFamily: "var(--font-head)", fontWeight: 800, color: c.color, marginTop: 4 }}>{c.val}</div>
                        </div>
                      ))}
                    </div>

                    {f.flags.length === 0 ? (
                      <div style={{
                        textAlign: "center", padding: 60, color: "var(--accent)",
                        fontFamily: "var(--font-head)", fontSize: 18, fontWeight: 700,
                      }}>
                        ✓ No issues detected
                        <div style={{ fontSize: 12, color: "var(--text3)", fontFamily: "var(--font-mono)", marginTop: 8, fontWeight: 400 }}>
                          Dataset passed all checks
                        </div>
                      </div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                        {f.flags.map((flag, i) => (
                          <div key={i} style={{
                            background: "var(--surface)", border: `1px solid var(--border)`,
                            borderLeft: `3px solid ${flag.severity === "critical" ? "#f04a6a" : flag.severity === "warning" ? "#f0c04a" : "#4a9ef0"}`,
                            borderRadius: "var(--radius)", padding: "14px 18px",
                            display: "grid", gridTemplateColumns: "1fr auto auto auto",
                            gap: 16, alignItems: "center",
                          }}>
                            <div>
                              <div style={{ fontSize: 12, fontWeight: 500, color: "var(--text)", marginBottom: 3 }}>
                                {flag.check}
                                <span style={{ marginLeft: 8, fontSize: 10, color: "var(--text3)" }}>
                                  on <span style={{ color: "var(--accent3)" }}>{flag.column}</span>
                                </span>
                              </div>
                              <div style={{ fontSize: 11, color: "var(--text3)" }}>{flag.detail}</div>
                            </div>
                            <Badge color={severityColor(flag.severity)}>{flag.severity}</Badge>
                            <div style={{ textAlign: "right" }}>
                              <div style={{ fontSize: 16, fontFamily: "var(--font-head)", fontWeight: 700, color: "var(--text)" }}>{flag.count}</div>
                              <div style={{ fontSize: 10, color: "var(--text3)" }}>rows</div>
                            </div>
                            <div style={{ width: 60 }}>
                              <div style={{ fontSize: 11, color: "var(--text2)", textAlign: "right", marginBottom: 3 }}>{flag.rate}</div>
                              <MiniBar value={flag.count} max={f.df.rows.length}
                                color={flag.severity === "critical" ? "#f04a6a" : flag.severity === "warning" ? "#f0c04a" : "#4a9ef0"} />
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* ── EDA Tab ── */}
                {tab === "eda" && (
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 12 }}>
                    {Object.entries(f.eda).map(([col, s]) => (
                      <div key={col} style={{
                        background: "var(--surface)", border: "1px solid var(--border)",
                        borderRadius: "var(--radius)", padding: 16,
                      }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
                          <div style={{ fontSize: 12, fontWeight: 500, color: "var(--text)", wordBreak: "break-all", flex: 1, marginRight: 8 }}>{col}</div>
                          <Badge color={s.type === "numeric" ? "accent" : "info"}>{s.type}</Badge>
                        </div>

                        {s.type === "numeric" ? (
                          <div>
                            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 12px", fontSize: 11 }}>
                              {[["Mean", s.mean], ["Median", s.median], ["Std Dev", s.std], ["Min", s.min], ["Max", s.max], ["Missing", s.missing]].map(([k, v]) => (
                                <div key={k}>
                                  <span style={{ color: "var(--text3)" }}>{k}: </span>
                                  <span style={{ color: "var(--text)" }}>{v ?? "—"}</span>
                                </div>
                              ))}
                            </div>
                            <Histogram data={s.hist} color="#4af0a0" />
                          </div>
                        ) : (
                          <div>
                            <div style={{ fontSize: 11, color: "var(--text3)", marginBottom: 8 }}>
                              {s.unique} unique · {s.missing} missing
                            </div>
                            {s.topValues.map(([v, c], i) => (
                              <div key={i} style={{ marginBottom: 5 }}>
                                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 2 }}>
                                  <span style={{ color: "var(--text2)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "70%" }}>{v}</span>
                                  <span style={{ color: "var(--text3)" }}>{c}</span>
                                </div>
                                <MiniBar value={c} max={s.count} color="#4a9ef0" />
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {/* ── Data Tab ── */}
                {tab === "data" && (
                  <div>
                    <div style={{ overflowX: "auto", borderRadius: "var(--radius)", border: "1px solid var(--border)" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: "var(--font-mono)" }}>
                        <thead>
                          <tr style={{ background: "var(--surface2)" }}>
                            <th style={{ padding: "8px 10px", textAlign: "left", color: "var(--text3)", fontSize: 10, letterSpacing: "0.06em", borderBottom: "1px solid var(--border)", width: 40 }}>#</th>
                            {f.df.columns.map(col => (
                              <th key={col}
                                onClick={() => { setSortCol(col); setSortDir(s => sortCol === col && s === "asc" ? "desc" : "asc"); }}
                                style={{
                                  padding: "8px 10px", textAlign: "left", color: sortCol === col ? "var(--accent)" : "var(--text2)",
                                  fontSize: 10, letterSpacing: "0.06em", borderBottom: "1px solid var(--border)",
                                  cursor: "pointer", whiteSpace: "nowrap", userSelect: "none",
                                }}>
                                {col} {sortCol === col ? (sortDir === "asc" ? "↑" : "↓") : ""}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {pagedRows.map((row, ri) => (
                            <tr key={ri} style={{ borderBottom: "1px solid var(--border)", transition: "background 0.1s" }}
                              onMouseEnter={e => e.currentTarget.style.background = "var(--surface2)"}
                              onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                              <td style={{ padding: "6px 10px", color: "var(--text3)" }}>{dataPage * PAGE_SIZE + ri + 1}</td>
                              {f.df.columns.map(col => (
                                <td key={col} style={{
                                  padding: "6px 10px", color: row[col] === null || row[col] === "" ? "var(--text3)" : "var(--text)",
                                  maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                                }}>
                                  {row[col] === null || row[col] === undefined ? <span style={{ fontStyle: "italic" }}>null</span> : String(row[col])}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 12, fontSize: 11, color: "var(--text3)" }}>
                      <span>Showing {dataPage * PAGE_SIZE + 1}–{Math.min((dataPage + 1) * PAGE_SIZE, f.df.rows.length)} of {f.df.rows.length}</span>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button onClick={() => setDataPage(p => Math.max(0, p - 1))} disabled={dataPage === 0}
                          style={{
                            background: "var(--surface2)", border: "1px solid var(--border)", borderRadius: 3,
                            padding: "4px 10px", fontSize: 11, color: "var(--text2)", cursor: "pointer",
                            opacity: dataPage === 0 ? 0.4 : 1,
                          }}>← Prev</button>
                        <button onClick={() => setDataPage(p => Math.min(totalPages - 1, p + 1))} disabled={dataPage >= totalPages - 1}
                          style={{
                            background: "var(--surface2)", border: "1px solid var(--border)", borderRadius: 3,
                            padding: "4px 10px", fontSize: 11, color: "var(--text2)", cursor: "pointer",
                            opacity: dataPage >= totalPages - 1 ? 0.4 : 1,
                          }}>Next →</button>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </>
          )}
        </main>
      </div>
    </div>
  );
}
