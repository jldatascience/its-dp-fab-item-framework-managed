#!/usr/bin/env python3
"""Étape 2 — VALEURS + ÉLÉMENTS via Fabric CLI (fab), workspaces DEV.

Récupère, via `fab api` :
  - GUID des workspaces Framework DEV et MDS DEV ;
  - GUID de CHAQUE item (DataPipeline, Notebook, Lakehouse...) des 2 workspaces
    (= cibles des InvokePipeline) ;
  - connexions et gateways (pour confirmer les noms ; MODOP 0.5 : aucune règle parameter.yml) ;
  - les ACTIVITÉS InvokePipeline / TridentNotebook À L'INTÉRIEUR de chaque pipeline, lues
    depuis le tenant via getDefinition (nom, workspaceId, pipelineId/notebookId, connexion).

Pré-requis : pip install ms-fabric-cli  ;  fab auth login
Workspaces privatisés : lancer depuis le réseau privé ou un notebook Fabric.

Sorties : guids_dev.json , connexions_gateways.json , activites_fabric_<repo>.csv
"""
import json, base64, csv, subprocess, sys, pathlib

BASE = pathlib.Path(__file__).resolve().parent  # sorties À CÔTÉ du script

WS_FRAMEWORK_DEV = "FR - DP - DEV - FAB - Framework - MANAGED"   # noms EXACTS (portail)
WS_ADP_MDS_DEV   = "FR - DP - DEV - FAB - ADP - MDS - MANAGED"
REF_TYPES = {"InvokePipeline", "TridentNotebook"}

def _run(args):
    try:
        return subprocess.run(["fab", *args], capture_output=True, text=True)
    except FileNotFoundError:
        sys.exit("Fabric CLI introuvable. pip install ms-fabric-cli, puis `fab auth login`.")

def _body(stdout, ctx):
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        sys.exit(f"Sortie `fab api {ctx}` non JSON (adapter à ta version de fab) :\n{stdout[:400]}")
    if isinstance(data, dict) and "text" in data and isinstance(data["text"], (dict, list)):
        data = data["text"]
    return data

def fab_get(endpoint):
    r = _run(["api", "-X", "get", endpoint])
    if r.returncode != 0:
        sys.exit(f"Echec `fab api get {endpoint}` : {r.stderr.strip() or r.stdout.strip()}")
    d = _body(r.stdout, endpoint)
    return d.get("value", d) if isinstance(d, dict) else d

def fab_post(endpoint):
    r = _run(["api", "-X", "post", endpoint])
    return (r.returncode, _body(r.stdout, endpoint) if r.stdout.strip() else {})

# ---------- workspaces & items ----------
def find_ws(nom):
    for ws in fab_get("workspaces"):
        if ws.get("displayName", "").strip() == nom.strip():
            return ws["id"]
    sys.exit(f"Workspace introuvable (nom exact ? droits ?) : {nom!r}")

def items(ws_id):
    return fab_get(f"workspaces/{ws_id}/items")   # liste d'items avec id/type/displayName

def items_map(its):
    return {f'{it["type"]}/{it["displayName"]}': it["id"] for it in its}

# ---------- activités InvokePipeline/Notebook, lues dans le tenant (getDefinition) ----------
def walk(acts, pipeline, out):
    for a in acts or []:
        tp = a.get("typeProperties", {}) or {}
        if a.get("type") in REF_TYPES:
            conn = None
            for v in (tp.get("externalReferences") or {}).values():
                conn = v if isinstance(v, str) else conn
            out.append({"pipeline": pipeline, "activite": a.get("name"), "type": a.get("type"),
                        "workspaceId": tp.get("workspaceId"), "pipelineId": tp.get("pipelineId"),
                        "notebookId": tp.get("notebookId"), "connection": conn})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if isinstance(tp.get(key), list): walk(tp[key], pipeline, out)
        for case in (tp.get("cases") or []):
            if isinstance(case, dict): walk(case.get("activities"), pipeline, out)

def activites_pipelines(ws_id, its):
    """Pour chaque DataPipeline du workspace : getDefinition -> pipeline-content.json -> activités."""
    out = []
    for it in its:
        if it.get("type") != "DataPipeline":
            continue
        code, body = fab_post(f"workspaces/{ws_id}/items/{it['id']}/getDefinition")
        parts = ((body or {}).get("definition") or {}).get("parts") or []
        if not parts:
            print(f"   (def. indisponible pour {it['displayName']} — getDefinition async/202 ? à vérifier)")
            continue
        for p in parts:
            if p.get("path", "").endswith("pipeline-content.json"):
                try:
                    doc = json.loads(base64.b64decode(p["payload"]).decode("utf-8"))
                except Exception:
                    continue
                walk((doc.get("properties") or {}).get("activities"), it["displayName"], out)
    return out

# ---------- exécution ----------
fw_id, mds_id = find_ws(WS_FRAMEWORK_DEV), find_ws(WS_ADP_MDS_DEV)
fw_items, mds_items = items(fw_id), items(mds_id)

json.dump({"workspaces": {"WS_FRAMEWORK_DEV": fw_id, "WS_ADP_MDS_DEV": mds_id},
           "framework_items": items_map(fw_items), "mds_items": items_map(mds_items)},
          open(BASE / "guids_dev.json", "w"), indent=2, ensure_ascii=False)

try: conns = [{"id": c.get("id"), "displayName": c.get("displayName"),
               "connectivityType": c.get("connectivityType")} for c in fab_get("connections")]
except SystemExit: conns = []
try: gws = [{"id": g.get("id"), "displayName": g.get("displayName"), "type": g.get("type")}
            for g in fab_get("gateways")]
except SystemExit: gws = []
json.dump({"connections": conns, "gateways": gws}, open(BASE / "connexions_gateways.json", "w"),
          indent=2, ensure_ascii=False)

# activités InvokePipeline/Notebook par workspace (depuis le tenant)
for label, ws_id, its in (("item-framework", fw_id, fw_items), ("adp-mds", mds_id, mds_items)):
    acts = activites_pipelines(ws_id, its)
    with open(BASE / f"activites_fabric_{label}.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["pipeline", "activite", "type", "workspaceId", "pipelineId", "notebookId", "connection"])
        for a in acts:
            w.writerow([a["pipeline"], a["activite"], a["type"], a["workspaceId"],
                        a["pipelineId"], a["notebookId"], a["connection"]])
    print(f"[{label}] {len(its)} items | {len(acts)} activités InvokePipeline/Notebook (tenant) -> activites_fabric_{label}.csv")

print(f"Framework DEV = {fw_id} | MDS DEV = {mds_id} | connexions={len(conns)} gateways={len(gws)}")
print("-> guids_dev.json , connexions_gateways.json , activites_fabric_*.csv")
