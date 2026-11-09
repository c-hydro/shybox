"use strict";
const $ = id => document.getElementById(id);
const params = new URLSearchParams(location.search);
const stamp = value => value ? value.replace(/\.\d+(?=Z$)/, "").replace("T", " ").replace("Z", " UTC") : "—";
let controller;
$("date").value = params.get("date") || "";
function node(tag, text, cls) {
  const result = document.createElement(tag);
  if (text != null) result.textContent = text;
  if (cls) result.className = cls;
  return result;
}
function status(value) {return node("span", value.replaceAll("_", " "), `execution-status ${value}`);}
async function refresh() {
  controller?.abort(); controller = new AbortController();
  const selected = $("application").value || params.get("application") || "";
  const query = new URLSearchParams();
  if ($("date").value) query.set("date", $("date").value);
  // Fetch all declarations so switching filters never removes selector options.
  try {
    const response = await fetch("/api/monitoring?" + query, {signal:controller.signal});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Unable to load monitoring records.");
    $("notice").hidden = true;
    $("last-update").textContent = "Updated " + stamp(data.generated_at);
    $("application").replaceChildren(new Option("All applications", ""),
      ...data.applications.map(app => new Option(app.name, app.id)));
    $("application").value = data.applications.some(app => app.id === selected) ? selected : "";
    const filter = $("application").value;
    const applications = data.applications.filter(app => !filter || app.id === filter);
    $("applications").replaceChildren(...applications.map(app => {
      const card = node("article", null, "collection-card");
      card.append(node("strong", app.name), status(app.status), node("span", app.run_name, "hint"));
      if (app.legacy) card.append(node("span", "Legacy references · outputs " + app.legacy.output_status + " · exit status unknown", "hint"));
      if (app.expected_run_time) card.append(node("span", "Expected " + stamp(app.expected_run_time), "hint"));
      if (Object.keys(app.counts).length) card.append(node("span", Object.entries(app.counts).map(([key,value])=>`${value} ${key}`).join(" · "), "hint"));
      return card;
    }));
    if (!applications.length) $("applications").append(node("p", "No applications declared. Configure monitoring.folder and monitoring.applications in the dashboard JSON.", "hint"));
    const records = data.executions.filter(record => !filter || record.application_id === filter);
    $("execution-count").textContent = `${records.length} attempts`;
    $("executions").replaceChildren(...records.map(record => {
      const row = node("tr");
      const app = node("td", record.application); app.append(node("small", record.run_name));
      const run = node("td", stamp(record.run_time));
      run.append(node("small", record.ensemble == null ? "Deterministic / no member" : "Member " + record.ensemble));
      const state = node("td"); state.append(status(record.status));
      const times = node("td", stamp(record.started_at)); times.append(node("small", stamp(record.finished_at)));
      const details = node("td", record.output_status);
      const expanded = node("details"); expanded.append(node("summary", "Details"));
      expanded.append(node("p", "Attempt: " + record.execution_id), node("p", "Heartbeat: " + stamp(record.updated_at)));
      if (record.task_id) expanded.append(node("p", "Task: " + record.task_id));
      if (record.error) expanded.append(node("pre", record.error));
      for (const output of record.outputs || []) expanded.append(node("p", `${output.available ? "Available" : "Missing"}: ${output.path}`));
      details.append(expanded);
      row.append(app, run, state, times, node("td", record.duration_seconds == null ? "—" : `${record.duration_seconds.toFixed(1)} s`), node("td", record.exit_code == null ? "—" : String(record.exit_code)), details);
      return row;
    }));
    if (!records.length) {const row = node("tr"); const cell = node("td", "No execution records for this selection."); cell.colSpan=7; row.append(cell); $("executions").append(row);}
    $("issues").hidden = !data.issues.length;
    $("issue-list").replaceChildren(...data.issues.map(issue=>node("li",issue)));
    await refreshMemory();
    history.replaceState(null,"", "/monitoring" + (query.size || filter ? "?" : "") + query + (filter ? (query.size ? "&" : "") + "application=" + encodeURIComponent(filter) : ""));
  } catch(error) {
    if (error.name === "AbortError") return;
    $("notice").hidden=false; $("notice").className="error"; $("notice").textContent=error.message;
    $("last-update").textContent="Refresh failed · previous data may be outdated";
  }
}
$("refresh").addEventListener("click",refresh);
$("application").addEventListener("change",()=>{params.delete("application"); refresh();});
$("date").addEventListener("change",refresh);
setInterval(()=>{if (!document.hidden) refresh();},30000);
refresh();

async function refreshMemory() {
  try {
    const response = await fetch('/api/monitoring/memory');
    if (!response.ok) throw new Error('RAM snapshot could not be read.');
    const data = await response.json();
    $('memory-time').textContent = data.generated_at ? stamp(data.generated_at) + ' · ' + data.status : '';
    $('memory-host').textContent = data.host?.memory_total_mib ? `Host RAM: ${data.host.memory_used_percent.toFixed(1)}% used · ${(data.host.memory_available_mib/1024).toFixed(1)} GiB available` : 'No RAM samples available. Start the process RAM collector to publish measurements.';
    $('memory-targets').replaceChildren(...data.targets.map(target=>{const row=node('tr');for(const value of [target.label,target.status,target.rss_mib?.toFixed(1)??'—',target.peak_rss_mib?.toFixed(1)??'—',target.process_count,target.level])row.append(node('td',String(value)));return row;}));
    $('ruler-report').hidden=!data.ruler;$('ruler-json').textContent=data.ruler?JSON.stringify(data.ruler,null,2):'';
    if(data.generated_at){$('memory-plot').src='/api/monitoring/memory/plot?t='+encodeURIComponent(data.generated_at);$('memory-plot').hidden=false;}
  }catch(error){$('memory-host').textContent=error.message;}
}
$('memory-plot').addEventListener('error',()=>{$('memory-plot').hidden=true;});
