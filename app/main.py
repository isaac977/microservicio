# main.py — CLI con barras y CSV
import os, csv, asyncio, aiohttp, logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from tqdm import tqdm
from fetcher import recabar_pairs

# -------- logging --------
def setup_logging():
    log_dir = os.getenv("LOG_DIR", "logs").strip() or "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_level    = os.getenv("LOG_LEVEL", "INFO").upper()
    console_level = os.getenv("CONSOLE_LOG_LEVEL", "ERROR").upper()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in list(root.handlers):
        root.removeHandler(h)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, console_level, logging.ERROR))
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "empleados_cli.log"),
        when="midnight", backupCount=7, encoding="utf-8"
    )
    fh.setLevel(getattr(logging, file_level, logging.INFO))
    fh.setFormatter(fmt)
    root.addHandler(fh)
    logging.getLogger("aiohttp.client").setLevel(logging.WARNING)

# -------- utils --------
def _s(x): return "" if x is None else str(x).strip()

async def fetch_keys(keys_url: str):
    timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(keys_url) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"KEYS_URL HTTP {r.status}: {txt[:200]}")
            try:
                js = await r.json()
            except Exception:
                raise RuntimeError(f"KEYS_URL JSON inválido: {txt[:200]}")

    items = []
    if isinstance(js, list):
        items = js
    elif isinstance(js, dict):
        for k in ("data", "items", "rows", "result"):
            if isinstance(js.get(k), list):
                items = js[k]; break
        if not items:
            items = [v for v in js.values() if isinstance(v, dict)]

    out = []
    for it in items:
        if not isinstance(it, dict): continue
        ct  = _s(it.get("clave") or it.get("oclave") or it.get("centro_trabajo") or it.get("ct"))
        tok = _s(it.get("token") or it.get("param"))
        if ct: out.append({"ct": ct, "token": tok})
    return out

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def write_csv(path, rows, headers, desc):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in tqdm(rows, desc=desc, unit="fila"):
            w.writerow({k: r.get(k, "") for k in headers})

# -------- runner --------
async def run():
    # Cargar .env sobreescribiendo variables previas del entorno
    load_dotenv(override=True)
    setup_logging()

    TEMPLATE   = (os.getenv("DATA_URL_TEMPLATE") or "").strip()
    TOKEN_DEF  = _s(os.getenv("EXTERNAL_PARAM"))
    KEYS_URL   = (os.getenv("KEYS_URL") or "").strip()
    OUTDIR     = (os.getenv("OUTPUT_DIR", "output") or "output").strip()
    CONC       = int(os.getenv("CONCURRENCY", "50"))
    RETRIES    = int(os.getenv("RETRY_LIMIT", "3"))
    LIMIT_env  = os.getenv("LIMIT"); LIMIT = int(LIMIT_env) if LIMIT_env and LIMIT_env.isdigit() else None
    CTS_env    = [c.strip().upper() for c in os.getenv("CTS","").split(",") if c.strip()]
    DO_SYNC_CONTR = os.getenv("SYNC_CONTRATOS","0").strip().lower() in ("1","true","yes","on")

    if "{clave}" not in TEMPLATE or "{param}" not in TEMPLATE:
        print("ERROR: DATA_URL_TEMPLATE inválida en .env"); return

    pairs = []
    if KEYS_URL:
        print(f"Leyendo CTs de KEYS_URL: {KEYS_URL}")
        items = await fetch_keys(KEYS_URL)
        if LIMIT: items = items[:LIMIT]
        for it in items:
            ct  = it["ct"]
            tok = it["token"] or TOKEN_DEF
            if tok: pairs.append((ct, tok))
        if not pairs:
            print("ERROR: sin CTs con token. Define EXTERNAL_PARAM o devuelve token/param en KEYS_URL."); return
    else:
        if not CTS_env:
            print("ERROR: define KEYS_URL o CTS=CT1,CT2,…"); return
        if not TOKEN_DEF:
            print("ERROR: falta EXTERNAL_PARAM para CTS."); return
        pairs = [(ct, TOKEN_DEF) for ct in CTS_env]

    pairs = sorted(pairs, key=lambda x: x[0])
    print(f"Consultando {len(pairs)} CT(s)…")

    base_rows, contrato_rows, plazas_rows, fail_rows = await recabar_pairs(
        TEMPLATE, pairs, concurrency=CONC, retry_limit=RETRIES
    )

    print(f"Descarga OK. BASE={len(base_rows)}  CONTRATO={len(contrato_rows)}  PLAZAS={len(plazas_rows)}")
    logging.info("CT OK=%d  FAIL=%d", len(pairs)-len(fail_rows), len(fail_rows))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    hb = ["centro_trabajo","rfc","tipo","clave_empleado","curp","nom_emp","paterno","materno","nombre"]
    hc = hb + ["ct_contrato"]

    PLAZA_KEYS = {"plaza_key","estatus_plaza","mot_mov","cod_pago","unidad","subunidad","cat_puesto","horas","cons_plaza","qna_ini","qna_fin"}
    seen = {"centro_trabajo","rfc","clave_empleado"}
    for r in plazas_rows: seen.update(r.keys())
    ordered = [h for h in ["centro_trabajo","rfc","clave_empleado",*PLAZA_KEYS] if h in seen]
    hp = ordered + sorted([h for h in seen if h not in ordered])

    base_path   = os.path.join(OUTDIR, f"{ts}_empleados_base_EXTERNO.csv")
    cont_path   = os.path.join(OUTDIR, f"{ts}_empleados_contrato_EXTERNO.csv")
    plazas_path = os.path.join(OUTDIR, f"{ts}_plazas_EXTERNO.csv")
    fail_path   = os.path.join(OUTDIR, f"{ts}_ct_fallos_EXTERNO.csv")

    write_csv(base_path,   base_rows,    hb, "Escribiendo empleados base")
    write_csv(cont_path,   contrato_rows, hc, "Escribiendo empleados contrato")
    write_csv(plazas_path, plazas_rows,  hp, "Escribiendo plazas")
    write_csv(fail_path,   fail_rows,   ["ct","why"], "Escribiendo CT fallidos")

    print("CSV listos:")
    print(" ", base_path)
    print(" ", cont_path)
    print(" ", plazas_path)
    print(" ", fail_path)

    if DO_SYNC_CONTR:
        try:
            from sync_contratos import sync_contratos
            summary = await sync_contratos(contrato_rows)
            print("Sync contratos ->", summary)
        except Exception as e:
            logging.error("Fallo en sync_contratos: %s", e, exc_info=True)
            print("ERROR en sync_contratos:", e)

if __name__ == "__main__":
    asyncio.run(run())
