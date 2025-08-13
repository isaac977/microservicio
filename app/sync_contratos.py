# sync_contratos.py
import os, asyncio, aiohttp, logging
from urllib.parse import urlencode
from tqdm import tqdm

def _s(x): return "" if x is None else str(x).strip()

FIELDS_MAP = {
    "nom_emp": "nom_emp",
    "emp_pat": "paterno",
    "emp_mat": "materno",
    "emp_nom": "nombre",
    "emp_curp": "curp",
}

def _norm_internal_row(r):
    # Acepta claves en distintos formatos desde PHP
    return {
        "rfc": _s(r.get("rfc") or r.get("RFCC")),
        "id_ext": _s(r.get("id_ext") or r.get("KCVEEMPC_EXT") or r.get("kcveempc_ext")),
        "nom_emp": _s(r.get("nom_emp") or r.get("NOM_EMPC")),
        "emp_pat": _s(r.get("emp_pat") or r.get("EMP_PAT")),
        "emp_mat": _s(r.get("emp_mat") or r.get("EMP_MAT")),
        "emp_nom": _s(r.get("emp_nom") or r.get("EMP_NOM")),
        "emp_curp": _s(r.get("emp_curp") or r.get("EMP_CURP")),
        "istatus": _s(r.get("istatus") or r.get("ISTATUS")),
    }

def _build_create_payload(ext):
    return {
        "id_ext": _s(ext.get("clave_empleado")),
        "rfc": _s(ext.get("rfc")),
        "nom_emp": _s(ext.get("nom_emp")),
        "emp_pat": _s(ext.get("paterno")),
        "emp_mat": _s(ext.get("materno")),
        "emp_nom": _s(ext.get("nombre")),
        "emp_curp": _s(ext.get("curp")),
    }

def _build_update_payload(rfc, ext, istatus="A"):
    return {
        "rfc": _s(rfc),
        "id_ext": _s(ext.get("clave_empleado")),
        "nom_emp": _s(ext.get("nom_emp")),
        "emp_pat": _s(ext.get("paterno")),
        "emp_mat": _s(ext.get("materno")),
        "emp_nom": _s(ext.get("nombre")),
        "emp_curp": _s(ext.get("curp")),
        "istatus": istatus,
    }

def _build_deactivate_payload(rfc):
    # Tu PHP: si id_ext == 0 -> ISTATUS='B'
    return {"rfc": _s(rfc), "id_ext": 0}

async def _get_json(session, url):
    async with session.get(url) as r:
        t = await r.text()
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} {url} | {t[:200]}")
        try:
            return await r.json()
        except Exception:
            raise RuntimeError(f"JSON inválido de {url} | {t[:200]}")

async def _post_json(session, url, payload):
    async with session.post(url, json=payload) as r:
        txt = await r.text()
        ok = r.status == 200
        return ok, r.status, txt

async def _fetch_internal_list(url):
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=60)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        js = await _get_json(s, url)
    # Normaliza a lista
    if isinstance(js, list):
        data = js
    elif isinstance(js, dict):
        for k in ("data","items","rows","result"):
            if isinstance(js.get(k), list):
                data = js[k]; break
        else:
            data = []
    # Mapea campos
    rows = []
    for r in data:
        if not isinstance(r, dict): continue
        rows.append(_norm_internal_row(r))
    return rows

def _needs_update(int_row, ext_row):
    # Compara id_ext y campos nombre/curp
    if _s(int_row.get("id_ext")) != _s(ext_row.get("clave_empleado")):
        return True
    for i_key, e_key in FIELDS_MAP.items():
        if _s(int_row.get(i_key)) != _s(ext_row.get(e_key)):
            return True
    # Si está en B y existe afuera, también actualiza a A
    if _s(int_row.get("istatus")).upper() == "B":
        return True
    return False

async def sync_contratos(external_rows: list[dict]):
    """
    external_rows: filas de CONTRATO del externo (las que ya construyes en main.py)
                   claves esperadas: rfc, clave_empleado, nom_emp, paterno, materno, nombre, curp
    """
    LIST_URL   = os.getenv("INTERNAL_CONTRATOS_LIST_URL")
    CREATE_URL = os.getenv("INTERNAL_CONTRATOS_CREATE_URL")
    UPDATE_URL = os.getenv("INTERNAL_CONTRATOS_UPDATE_URL")

    if not (LIST_URL and UPDATE_URL and CREATE_URL):
        print("SYNC_CONTRATOS: faltan endpoints en .env"); return {"created":0,"updated":0,"deactivated":0,"errors":0}

    # 1) Carga internos
    internos = await _fetch_internal_list(LIST_URL)
    internal_by_rfc = {r["rfc"]: r for r in internos if r.get("rfc")}
    logging.info("Internos contratos: %d", len(internal_by_rfc))

    # 2) Índice externos por RFC (si hay duplicado, toma el primero)
    external_by_rfc = {}
    for e in external_rows:
        if _s(e.get("tipo")).upper() != "CONTRATO":
            continue
        rfc = _s(e.get("rfc"))
        if not rfc:
            continue
        if rfc not in external_by_rfc:
            external_by_rfc[rfc] = e

    # 3) Construir acciones
    to_create, to_update, to_deactivate = [], [], []

    # 3a) Crear o actualizar existentes
    for rfc, ext in external_by_rfc.items():
        if rfc in internal_by_rfc:
            if _needs_update(internal_by_rfc[rfc], ext):
                to_update.append(_build_update_payload(rfc, ext, istatus="A"))
        else:
            to_create.append(_build_create_payload(ext))

    # 3b) Desactivar los internos que ya no están en el externo
    for rfc, int_row in internal_by_rfc.items():
        if rfc not in external_by_rfc:
            to_deactivate.append(_build_deactivate_payload(rfc))

    # 4) Ejecutar
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=60)
    created=updated=deactivated=errors=0
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # updates
        for p in tqdm(to_update, desc="Actualizando contratos", unit="reg"):
            ok, status, txt = await _post_json(session, UPDATE_URL, p)
            if ok: updated += 1
            else: errors += 1; logging.error("Update RFC %s -> HTTP %s %s", p.get("rfc"), status, txt[:200])

        # creates
        for p in tqdm(to_create, desc="Creando contratos", unit="reg"):
            ok, status, txt = await _post_json(session, CREATE_URL, p)
            if ok: created += 1
            else: errors += 1; logging.error("Create RFC %s -> HTTP %s %s", p.get("rfc"), status, txt[:200])

        # deactivations
        for p in tqdm(to_deactivate, desc="Bajando a B", unit="reg"):
            ok, status, txt = await _post_json(session, UPDATE_URL, p)  # id_ext=0 -> ISTATUS='B'
            if ok: deactivated += 1
            else: errors += 1; logging.error("Deactivate RFC %s -> HTTP %s %s", p.get("rfc"), status, txt[:200])

    summary = {"created":created,"updated":updated,"deactivated":deactivated,"errors":errors}
    logging.info("Sync contratos -> %s", summary)
    print("Sync contratos ->", summary)
    return summary
