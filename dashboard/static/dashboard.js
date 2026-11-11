/* Run selection is shared across pages through URL query parameters. */
"use strict";
const $ = id => document.getElementById(id);
const defaults = JSON.parse($("defaults").textContent);
const query = new URLSearchParams(location.search);
const page = document.body.dataset.page;
const state = {collections: [], collection: null, run: null, summary: null,
  basin: query.get("basin") || "", section: query.get("section"),
  period: query.get("period") || defaults.period, source: query.get("source") || "all"};
const colors = {green: "#2a995f", yellow: "#e7bd27", orange: "#ee862b", red: "#d44949", unknown: "#89969e"};
const labels = {green: "Below thresholds", yellow: "Level 1", orange: "Level 2", red: "Level 3", unknown: "Unavailable"};
const number = value => value == null ? "—" : value.toLocaleString(undefined, {maximumFractionDigits: 2});
const timeLabel = time => time ? time.replace("T", " ").slice(0, 16) : "—";
let map, markers, overlay, summaryController, chartController;

function node(tag, text, className) {
  const el = document.createElement(tag);
  if (text != null) el.textContent = text;
  if (className) el.className = className;
  return el;
}
function notice(message, error = false) {
  $("notice").hidden = !message;
  $("notice").textContent = message || "";
  $("notice").className = error ? "error" : "";
}
async function get(url, signal) {
  const response = await fetch(url, {signal});
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}
function options(select, entries, selected) {
  if (!select) return;
  select.replaceChildren(...entries.map(([value, label]) => new Option(label, value)));
  if (entries.some(([v]) => String(v) === String(selected))) select.value = selected;
}
function url(path, section = state.section, basin = state.basin) {
  const params = new URLSearchParams();
  if (state.collection) params.set("collection", state.collection.id);
  if (state.run) params.set("run", state.run.id);
  if (basin) params.set("basin", basin);
  if (section != null) params.set("section", section);
  params.set("period", state.period);
  params.set("source", state.source);
  return path + "?" + params;
}
function syncUrl() {
  history.replaceState(null, "", url(location.pathname));
  document.querySelectorAll("nav a").forEach(a => {a.href = url(new URL(a.href).pathname);});
}
function api(suffix = "") {
  return `/api/runs/${encodeURIComponent(state.collection.id)}/${state.run.id}${suffix}`;
}
function collectionCards() {
  if (!$("collections")) return;
  $("collections").replaceChildren(...state.collections.map(c => {
    const button = node("button", null, "collection-card" + (c.id === state.collection?.id ? " selected" : ""));
    button.type = "button";
    button.append(node("span", c.mode, "card-mode"), node("strong", c.name),
      node("span", c.runs.length ? `${c.runs.length} available run${c.runs.length === 1 ? "" : "s"} · Latest ${timeLabel(c.runs[0].time)}` : "No results available", "hint"));
    button.addEventListener("click", () => selectCollection(c.id));
    return button;
  }));
}
async function refresh() {
  try {
    const previous = state.collection?.id || query.get("collection") || defaults.collection;
    const previousRun = state.run?.id || query.get("run");
    const data = await get("/api/runs");
    state.collections = data.collections;
    options($("collection"), data.collections.map(c => [c.id, `${c.name} (${c.mode})`]), previous);
    await selectCollection($("collection").value, previousRun);
  } catch (error) {notice(error.message, true);}
}
async function selectCollection(id, preferredRun) {
  summaryController?.abort(); chartController?.abort();
  if (state.collection && state.collection.id !== id) state.source = "all";
  state.collection = state.collections.find(c => c.id === id);
  if (!state.collection) {notice("No enabled run configurations are declared.", true); return;}
  $("collection").value = id;
  $("mode-badge").textContent = state.collection.mode === "probabilistic" ? "Probabilistic · ensemble" : "Deterministic";
  collectionCards();
  const runs = state.collection.runs;
  state.run = runs.find(r => r.id === preferredRun) || runs[0] || null;
  const dates = [...new Set(runs.map(r => r.date))].sort();
  $("date").min = dates[0] || ""; $("date").max = dates.at(-1) || "";
  $("date").disabled = !runs.length;
  $("date").value = state.run?.date || "";
  await runsForDate(state.run?.id);
}
async function runsForDate(preferred) {
  const runs = state.collection.runs.filter(r => r.date === $("date").value);
  options($("run"), runs.length ? runs.map(r => [r.id, timeLabel(r.time)]) : [["", "No runs on this date"]], preferred);
  $("run").disabled = !runs.length;
  state.run = runs.find(r => r.id === $("run").value) || null;
  await loadSummary();
}
function clearResults(message) {
  state.summary = null;
  markers?.clear(); overlay?.setPosition(undefined);
  for (const id of ["section-rows", "counts", "threshold-cards", "section", "basin"]) $(id)?.replaceChildren();
  for (const id of ["section-count", "map-count", "section-details", "chart-note", "status-description"]) if ($(id)) $(id).textContent = "";
  if ($("hydrograph")) {if (window.Plotly) Plotly.purge("hydrograph"); $("hydrograph").textContent = message; $("chart-title").textContent = "No time series available";}
}
async function loadSummary() {
  summaryController?.abort(); chartController?.abort();
  summaryController = new AbortController();
  const signal = summaryController.signal;
  syncUrl();
  if (page === "maps") {
    const cfg = state.collection.maps;
    $("maps-note").textContent = `Selected configuration: ${state.collection.name}. ${cfg.enabled ? "A gridded reader still needs to be connected for this configuration." : "Gridded publication is disabled for this configuration."}`;
    notice(state.run ? "" : "This declared configuration has no available results.");
    return;
  }
  if (!state.run) {clearResults("No results available."); notice("No results found for this configuration and date. Choose another date or configuration."); return;}
  clearResults("Loading results…");
  notice("Loading results…");
  try {
    const data = await get(api(`?period=${state.period}&source=${encodeURIComponent(state.source)}`), signal);
    state.summary = data;
    const warnings = state.collection.issues.length ? `${state.collection.issues.length} file(s) were skipped because their run timestamps could not be parsed. ` : "";
    notice(warnings + (!data.start ? "No timestamps fall within the selected period." : ""));
    const sources = data.sources.filter(s => state.collection.mode !== "probabilistic" || s.id !== "discharge_observed");
    options($("source"), [["all", "All available series"], ...sources.map(s => [s.id, s.name])], state.source);
    if ($("period")) $("period").value = state.period;
    if (page === "overview") {
      options($("basin"), [["", "All basins"], ...data.basins.map(b => [b, b])], state.basin);
      state.basin = $("basin").value;
      $("status-description").textContent = data.mode === "probabilistic"
        ? `Colour: probability of a member exceeding a threshold at any selected time ≥ ${data.probability_cutoff * 100}%. Selected simulated products are combined per member; observations are context only. ${timeLabel(data.start)} → ${timeLabel(data.end)}.`
        : `Colour: maximum discharge across selected series and times, compared with available section thresholds (strictly greater). ${timeLabel(data.start)} → ${timeLabel(data.end)}.`;
      renderOverview();
    } else {
      options($("basin"), data.basins.map(b => [b, b]), state.basin);
      state.basin = $("basin").value;
      await selectBasin();
    }
    syncUrl();
  } catch (error) {
    if (error.name !== "AbortError") {clearResults("This result could not be loaded."); notice(error.message, true);}
  }
}
function filteredSections() {
  return (state.summary?.sections || []).filter(s => !state.basin || s.basin === state.basin);
}
function renderOverview() {
  const sections = filteredSections();
  const priority = {red:3,orange:2,yellow:1,green:0,unknown:-1};
  const sorted = [...sections].sort((a,b) => priority[b.status] - priority[a.status] || a.name.localeCompare(b.name));
  $("counts").replaceChildren(...Object.keys(labels).map(status => {
    const card = node("div", null, `status-card ${status}`);
    card.append(node("strong", String(sections.filter(s => s.status === status).length)), node("span", labels[status]));
    return card;
  }));
  $("section-count").textContent = `· ${sections.length}`;
  $("section-rows").replaceChildren(...sorted.map(section => {
    const row = node("tr");
    const name = node("td"); const link = node("a", section.name);
    link.href = url("/time-series", section.index, section.basin);
    name.append(link, node("small", section.basin));
    const badge = node("span", null, "status-label");
    badge.append(node("i", null, `dot ${section.status}`), document.createTextNode(labels[section.status]));
    const cell = node("td"); cell.append(badge);
    row.append(name, node("td", number(section.peak)), cell);
    return row;
  }));
  renderMap(sections); syncUrl();
}
function initMap() {
  if (!$("section-map")) return;
  if (!window.ol) {$("section-map").textContent = "Map library unavailable. Check internet access; section results remain available in the table."; return;}
  markers = new ol.source.Vector();
  overlay = new ol.Overlay({element: $("map-popup"), positioning: "bottom-center", offset: [0, -12], autoPan: {animation: {duration: 200}}});
  const tiles = new ol.source.OSM();
  tiles.once("tileloaderror", () => notice("The background map is unavailable. Section markers and results remain available."));
  map = new ol.Map({target: "section-map", layers: [new ol.layer.Tile({source: tiles}), new ol.layer.Vector({source: markers})],
    overlays: [overlay], view: new ol.View({center: ol.proj.fromLonLat([13.1, 43.3]), zoom: 8})});
  map.on("singleclick", event => {
    const feature = map.forEachFeatureAtPixel(event.pixel, f => f);
    if (!feature) {overlay.setPosition(undefined); return;}
    const s = feature.get("section"); const popup = $("map-popup");
    const close = node("button", "×"); close.type = "button"; close.setAttribute("aria-label", "Close section details");
    close.onclick = () => overlay.setPosition(undefined);
    const link = node("a", "View section time series →"); link.href = url("/time-series", s.index, s.basin);
    popup.replaceChildren(close, node("strong", s.name), node("p", `${s.basin} · ${labels[s.status]}`), node("p", `Peak: ${number(s.peak)} m³/s`));
    s.thresholds.forEach((value, i) => {
      const probability = s.probabilities[i];
      popup.append(node("p", `Level ${i+1}: ${number(value)} m³/s${probability == null ? "" : ` · ${(probability*100).toFixed(1)}% exceedance`}`));
    });
    popup.append(link); popup.hidden = false; overlay.setPosition(feature.getGeometry().getCoordinates());
  });
  map.on("pointermove", e => {map.getTargetElement().style.cursor = map.hasFeatureAtPixel(e.pixel) ? "pointer" : "";});
}
function renderMap(sections) {
  if (!markers) return;
  markers.clear(); overlay.setPosition(undefined);
  const geocoded = sections.filter(s => s.longitude != null && s.latitude != null && Math.abs(s.longitude) <= 180 && Math.abs(s.latitude) <= 90);
  for (const section of geocoded) {
    const feature = new ol.Feature({geometry: new ol.geom.Point(ol.proj.fromLonLat([section.longitude, section.latitude])), section});
    feature.setStyle(new ol.style.Style({image: new ol.style.Circle({radius: 6, fill: new ol.style.Fill({color: colors[section.status]}), stroke: new ol.style.Stroke({color: "white", width: 1.5})})}));
    markers.addFeature(feature);
  }
  $("map-count").textContent = `${geocoded.length} / ${sections.length} located`;
  if (geocoded.length) map.getView().fit(markers.getExtent(), {padding: [45,45,45,45], maxZoom: 13});
}
async function selectBasin() {
  state.basin = $("basin").value;
  const sections = filteredSections();
  options($("section"), sections.map(s => [String(s.index), s.name]), state.section);
  state.section = sections.length ? $("section").value : null;
  await renderChart(); syncUrl();
}
async function renderChart() {
  chartController?.abort(); chartController = new AbortController();
  if (state.section == null || !state.run) return;
  $("chart-title").textContent = "Loading hydrograph…";
  $("section-details").textContent = ""; $("chart-note").textContent = ""; $("threshold-cards").replaceChildren();
  if (window.Plotly) Plotly.purge("hydrograph");
  try {
    const data = await get(api(`/sections/${state.section}`), chartController.signal);
    const section = data.section;
    $("chart-title").textContent = `${section.name} · ${section.basin}`;
    $("section-details").textContent = `Catchment ${number(section.area)} km² · ${number(section.latitude)}° N, ${number(section.longitude)}° E`;
    const traceColors = {discharge_simulated_analysis: "#087d88", discharge_simulated_openloop: "#5679bc", discharge_observed: "#273e47"};
    const traces = []; const missing = [];
    for (const series of data.traces) {
      const color = traceColors[series.id];
      if (series.kind === "line") {
        if (series.values.every(v => v == null)) {missing.push(series.name); continue;}
        traces.push({x:data.time, y:series.values, name:series.name, type:"scatter", mode:"lines", line:{color,width:2}, connectgaps:false});
      } else {
        if (series.values[2].every(v => v == null)) {missing.push(series.name); continue;}
        const q = series.quantiles;
        for (const [low, high, opacity] of [[0,4,.10],[1,3,.20]]) {
          const rgb = color === "#087d88" ? "8,125,136" : "86,121,188";
          traces.push({x:data.time,y:series.values[low],type:"scatter",mode:"lines",line:{width:0},showlegend:false,hoverinfo:"skip",connectgaps:false,legendgroup:series.id});
          traces.push({x:data.time,y:series.values[high],type:"scatter",mode:"lines",line:{width:0},fill:"tonexty",fillcolor:`rgba(${rgb},${opacity})`,name:`${series.name} · ${q[low]*100}–${q[high]*100}%`,legendgroup:series.id,connectgaps:false});
        }
        traces.push({x:data.time,y:series.values[2],type:"scatter",mode:"lines",name:`${series.name} · median`,line:{color,width:2},legendgroup:series.id,connectgaps:false});
      }
    }
    const shapes = [{type:"line",xref:"x",x0:data.run.time,x1:data.run.time,yref:"paper",y0:0,y1:1,line:{color:"#78909c",dash:"dash",width:1}}];
    const thresholdCards = section.thresholds.map((threshold, i) => {
      const color = [colors.yellow,colors.orange,colors.red][i];
      if (threshold != null) traces.push({x:[data.time[0],data.time.at(-1)],y:[threshold,threshold],type:"scatter",mode:"lines",name:`Level ${i+1}`,line:{color,width:1.5,dash:"dash"},hovertemplate:`Level ${i+1}: %{y} m³/s<extra></extra>`});
      const card = node("div", null, `status-card ${["yellow","orange","red"][i]}`);
      card.append(node("strong", number(threshold)), node("span", `Level ${i+1} · ${threshold == null ? "not defined" : "m³/s"}`)); return card;
    });
    $("threshold-cards").replaceChildren(...thresholdCards);
    $("hydrograph").replaceChildren();
    if (window.Plotly) {
      await Plotly.newPlot("hydrograph",traces,{margin:{t:25,r:20,b:85,l:65},paper_bgcolor:"white",plot_bgcolor:"white",font:{family:"system-ui",color:"#49616b",size:12},xaxis:{title:"Time (as stored)",gridcolor:"#edf2f3",range:[data.time[0],data.time.at(-1)]},yaxis:{title:"Discharge (m³/s)",rangemode:"tozero",gridcolor:"#edf2f3"},legend:{orientation:"h",y:-.24},hovermode:"x unified",shapes},{responsive:true,displaylogo:false});
    } else {$("hydrograph").textContent = "Chart library unavailable. Check internet access to load Plotly.";}
    $("chart-note").textContent = (data.mode === "probabilistic" ? "Bands show the configured ensemble quantiles; member availability may vary with time. " : "") + (missing.length ? `No data: ${missing.join(", ")}.` : "");
  } catch (error) {if (error.name !== "AbortError") {$("chart-title").textContent = "Unable to load hydrograph"; notice(error.message,true);}}
}
$("collection").addEventListener("change", () => {state.source="all"; selectCollection($("collection").value);});
$("date").addEventListener("change", () => runsForDate());
$("run").addEventListener("change", () => {state.run=state.collection.runs.find(r=>r.id===$("run").value); loadSummary();});
$("refresh").addEventListener("click", refresh);
$("period")?.addEventListener("change", () => {state.period=$("period").value; loadSummary();});
$("source")?.addEventListener("change", () => {state.source=$("source").value; loadSummary();});
$("basin")?.addEventListener("change", () => {state.basin=$("basin").value; page==="overview" ? renderOverview() : selectBasin();});
$("section")?.addEventListener("change", () => {state.section=$("section").value; renderChart(); syncUrl();});
initMap(); refresh();
