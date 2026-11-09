"use strict";
const $=id=>document.getElementById(id);
const query=new URLSearchParams(location.search);
let products=[],selected=query.get("product"),controller;
const stamp=value=>value ? value.replace("T"," ").replace("Z"," UTC") : "—";
function node(tag,text,cls){const el=document.createElement(tag);if(text!=null)el.textContent=text;if(cls)el.className=cls;return el;}
function options(id,values,label){const select=$(id),current=select.value;select.replaceChildren(new Option(label,""),...[...new Set(values.filter(Boolean))].sort().map(value=>new Option(value.replaceAll("_"," "),value)));if([...select.options].some(option=>option.value===current))select.value=current;}
function render(){
  const kind=$("kind").value,date=$("date").value;
  const base=products.filter(p=>(kind==="all"||p.kind===kind)&&(!date||p.date===date));
  options("publication",base.map(p=>p.publication_time),"All times");
  options("group",base.map(p=>p.group),"All collections");
  options("basin",base.map(p=>p.basin),"All basins");
  options("window",base.map(p=>p.window),"All windows");
  $("basin").disabled=kind==="maps";$("window").disabled=kind==="time_series";
  const matches=base.filter(p=>(!$("publication").value||p.publication_time===$("publication").value)&&(!$("group").value||p.group===$("group").value)&&(!$("basin").value||p.basin===$("basin").value)&&(!$("window").value||p.window===$("window").value));
  $("figure-count").textContent=`${matches.length} products`;
  selected=matches.some(p=>p.id===selected)?selected:matches[0]?.id;
  $("products").replaceChildren(...matches.map(product=>{const button=node("button",null,"figure-option");button.type="button";button.setAttribute("aria-pressed",String(product.id===selected));button.append(node("strong",product.title),node("small",[product.group.replaceAll("_"," "),product.window,product.basin].filter(Boolean).join(" · ")),node("small",stamp(product.publication_time)));button.addEventListener("click",()=>{selected=product.id;render();});return button;}));
  if(!matches.length)$("products").append(node("p","No published products for this selection.","hint"));
  show(matches.find(p=>p.id===selected));
}
function show(product){
  const available=Boolean(product);
  for(const id of ["figure-image","full-image","figure-actions"])$(id).hidden=!available;
  $("empty-selection").hidden=available;
  $("figure-details").replaceChildren();$("source-runs").replaceChildren();
  $("figure-title").textContent=product?.title||"No product selected";
  $("figure-window").textContent=product?.window||"";
  $("figure-summary").textContent=product?`${product.group.replaceAll("_"," ")} · Published ${stamp(product.publication_time)}`:"";
  const params=new URLSearchParams();for(const id of ["kind","date","publication","group","basin","window"])if($(id).value)params.set(id,$(id).value);
  if(product)params.set("product",product.id);history.replaceState(null,"",location.pathname+"?"+params);
  if(!product){$("figure-image").removeAttribute("src");return;}
  $("figure-image").src=product.image_url;$("figure-image").alt=product.title;
  $("full-image").href=product.image_url;
  $("download-image").href=product.image_url+"?download=1";
  $("download-metadata").href=product.metadata_url+"?download=1";
  for(const [label,value]of [["Variable",product.variable],["Units",product.units],["Method",product.method],["Window direction",product.direction],["First valid time",product.time_from&&stamp(product.time_from)],["Last valid time",product.time_to&&stamp(product.time_to)],["Basin",product.basin],["Section code",product.section_code]])if(value)$("figure-details").append(node("dt",label),node("dd",value));
  for(const source of product.sources){const card=node("article");card.append(node("h3",source.label.replaceAll("_"," ")),node("p",source.run_name||"Source run"),node("p","Initialization: "+stamp(source.run_time)),node("p","Simulation start: "+stamp(source.time_start)));$("source-runs").append(card);}
}
async function refresh(){
  controller?.abort();controller=new AbortController();
  try{const response=await fetch("/api/figures",{signal:controller.signal});if(!response.ok)throw new Error("Unable to load published figures.");const data=await response.json();products=data.products;$("notice").hidden=true;
    const dates=[...new Set(products.map(p=>p.date))].sort();$("date").min=dates[0]||"";$("date").max=dates.at(-1)||"";if(!$("date").value)$("date").value=dates.at(-1)||"";
    $("issues").hidden=!data.issues.length;$("issue-list").replaceChildren(...data.issues.map(issue=>node("li",issue)));render();
  }catch(error){if(error.name!=="AbortError"){$("notice").hidden=false;$("notice").className="error";$("notice").textContent=error.message;}}
}
$("kind").value=query.get("kind")||document.body.dataset.kind;
for(const id of ["date","publication","group","basin","window"])if(query.get(id)){if($(id).tagName==="SELECT")$(id).append(new Option(query.get(id),query.get(id)));$(id).value=query.get(id);}
for(const id of ["kind","date","publication","group","basin","window"])$(id).addEventListener("change",()=>{if(id==="kind")for(const other of ["publication","group","basin","window"])$(other).value="";render();});
$("refresh").addEventListener("click",refresh);$("figure-image").addEventListener("error",()=>{$("notice").hidden=false;$("notice").textContent="This published image is unavailable. Refresh the catalogue.";});refresh();
