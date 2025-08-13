import aiohttp, asyncio, logging
from urllib.parse import quote

def _s(x): return "" if x is None else str(x).strip()

PLAZA_KEYS = {
    "plaza_key","estatus_plaza","mot_mov","cod_pago",
    "unidad","subunidad","cat_puesto","horas",
    "cons_plaza","qna_ini","qna_fin"
}

def _norm_plazas(pza):
    if pza is None: return []
    if isinstance(pza, list): return pza
    if isinstance(pza, dict):
        vals = list(pza.values())
        return [pza] if (not vals or not isinstance(vals[0], (dict, list))) else vals
    return []

def _flatten(ct: str, payload: dict):
    base, contrato, plazas = [], [], []
    data = (payload or {}).get("data") or {}
    emp = data.get("emp") or {}

    for e in emp.values():
        tipo = _s(e.get("tipo")).upper()
        row = {
            "centro_trabajo": _s(payload.get("centro_trabajo") or ct),
            "rfc": _s(e.get("rfc")),
            "tipo": tipo,
            "clave_empleado": _s(e.get("clave_empleado")),
            "curp": _s(e.get("curp")),
            "nom_emp": _s(e.get("nom_emp")),
            "paterno": _s(e.get("paterno")),
            "materno": _s(e.get("materno")),
            "nombre": _s(e.get("nombre")),
        }
        if tipo == "CONTRATO":
            row["ct_contrato"] = _s(e.get("ct_contrato") or row["centro_trabajo"])
            contrato.append(row)
        else:
            base.append(row)

        # plazas solo si trae campos reales
        dp = e.get("data_plaza") or {}
        for p in _norm_plazas(dp.get("pza")):
            if not isinstance(p, dict): continue
            pr = {
                "centro_trabajo": row["centro_trabajo"],
                "rfc": row["rfc"],
                "clave_empleado": row["clave_empleado"],
            }
            has_plaza = False
            for k in PLAZA_KEYS:
                if k in p:
                    pr[k] = _s(p.get(k)); has_plaza = True
            if has_plaza:
                plazas.append(pr)

    return base, contrato, plazas

async def _fetch_ct(session, template: str, ct: str, token: str, timeout_s: int, retry_limit: int):
    url = template.format(clave=quote(ct), param=quote(token))
    why = "unknown"
    for attempt in range(retry_limit):
        try:
            async with session.get(url, timeout=timeout_s) as r:
                txt = await r.text()
                if r.status != 200:
                    why = f"HTTP {r.status}"
                else:
                    try:
                        js = await r.json()
                        msg = _s(js.get("msg")).upper()
                        if not msg or msg == "OK":
                            return {"ok": True, "ct": ct, "json": js}
                        why = msg
                    except Exception:
                        why = "JSON"
        except Exception as e:
            why = str(e)
        if attempt < retry_limit - 1:
            await asyncio.sleep(2 ** attempt)
    logging.debug("Fallo CT %s -> %s", ct, why)
    return {"ok": False, "ct": ct, "why": why}

async def recabar_pairs(template: str, pairs: list[tuple[str,str]], concurrency: int = 50, retry_limit: int = 3):
    base_rows, contrato_rows, plaza_rows, fail_rows = [], [], [], []
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_connect=20, sock_read=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [_fetch_ct(session, template, ct.strip().upper(), tok, 30, retry_limit) for ct, tok in pairs]
        for fut in asyncio.as_completed(tasks):
            res = await fut
            if not res.get("ok"):
                fail_rows.append({"ct": res.get("ct",""), "why": res.get("why","")})
                continue
            ct = res["ct"]
            b, c, p = _flatten(ct, res["json"])
            base_rows.extend(b); contrato_rows.extend(c); plaza_rows.extend(p)
    return base_rows, contrato_rows, plaza_rows, fail_rows
