import pandas as pd
import re

# ==============================
# CONFIG
# ==============================
INPUT_PATH = "data/PERFIL MICROBIOLOGICO.xlsx"
ANTIB_PATH = "data/LISTA ANTIBIOTICOS.xlsx"
ANTIB_SHEET = "CONSOLIDADO"
SALIDA_PATH = "data/salida.xlsx"

ANTVALOR_SET = {
    "SENSIBLE", "RESISTENTE", "INTERMEDIO",
    "NEGATIVO", "POSITIVO", "NEG", "POS",
    "SENSIB", "INTERMEDIA", "RESISTEN", "RESISTENT", "RESISTE"
}

# ==============================
# FUNCIONES
# ==============================
def detectar_columna_texto(df):
    candidates = [c for c in df.columns if df[c].dtype == "object"]
    if not candidates:
        return None
    avg_lens = {}
    for c in candidates:
        lens = df[c].dropna().astype(str).map(len)
        avg_lens[c] = lens.mean() if len(lens) > 0 else 0
    if not avg_lens:
        return None
    best = max(avg_lens.items(), key=lambda x: x[1])
    if best[1] < 50:
        return None
    return best[0]


def normalizar_token(t):
    return re.sub(r"[^\wÁÉÍÓÚÑáéíóúñ()/-]", "", str(t)).upper()


def limpiar_cmi(valor):
    if not valor:
        return ""
    m = re.search(r"-?\d+(?:\.\d+)?", str(valor))
    return m.group(0) if m else ""


def limpiar_nombre_antibiotico(raw):
    if not raw:
        return ""
    s = str(raw)
    s = s.replace(">=", "").replace("<=", "").replace(">", "").replace("<", "")
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip()
    s = re.sub(r"[^\w /\-()]", "", s, flags=re.UNICODE).upper()
    return s.strip(":,- ")


def es_antivalor_truncado(tok):
    t = normalizar_token(tok)
    for target in ANTVALOR_SET:
        if t.startswith(target[:4]):
            return True
    return False


def preprocesar_texto(texto):
    texto = str(texto or "")
    texto = texto.replace("\r", "")
    texto = re.sub(r"\b0\s+0\b", "\n", texto)
    texto = re.sub(r"[ \t]{2,}", " ", texto)
    return texto.strip()


def extraer_blee(texto):
    texto = str(texto or "")
    for linea in texto.splitlines():
        if re.search(r"(?i)\bBLEE\b", linea):
            if re.search(r"(?i)pos", linea):
                return "Positivo"
            if re.search(r"(?i)neg", linea):
                return "Negativo"
    return ""


def extraer_microorganismos(texto):
    texto = preprocesar_texto(texto)
    patrones = [
        r"(?mi)microorganism\w*\s+aislado\s*:?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 ()./-]+)",
        r"(?mi)microorganism\w*\s*:?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 ()./-]+)",
        r"(?mi)microorganismo\s*[:\s-]*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 ()./-]+)",
        r"(?mi)\bais[^\w]{0,3}lado[:\s-]*([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 ()./-]+)",
    ]
    for p in patrones:
        m = re.search(p, texto)
        if m:
            val = m.group(1).strip().rstrip(".")
            val = re.split(r"\s{2,}|RESULTADO|RECUENTO|AMIKACINA|BLEE", val)[0]
            return val.strip()
    return ""


def extraer_antibioticos_cmi_valor(texto):
    texto = preprocesar_texto(texto)
    resultados = []

    for linea in texto.splitlines():
        linea = linea.strip()
        if not linea:
            continue

        # limpieza de símbolos extra
        linea = re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóúñ0-9 /\-<>=.()+µ]", "", linea)

        # patrón más permisivo (nombre + CMI opcional + interpretación)
        m = re.match(
            r"^([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 /()\-]+?)\s*(?:([<>]=?\s*-?\d*\.?\d+|-?\d+\.?\d*)\s*)?([A-Za-zÁÉÍÓÚÑáéíóúñ()\-+]+)$",
            linea
        )
        if not m:
            continue

        antib_raw, cmi_raw, val_raw = m.groups()
        antib = limpiar_nombre_antibiotico(antib_raw)
        cmi = limpiar_cmi(cmi_raw)
        val_tok = normalizar_token(val_raw)

        if not antib:
            continue

        if es_antivalor_truncado(val_tok):
            if val_tok.startswith("SENS"):
                val_norm = "Sensible"
            elif val_tok.startswith("RES"):
                val_norm = "Resistente"
            elif val_tok.startswith("INT"):
                val_norm = "Intermedio"
            elif val_tok.startswith("NEG"):
                val_norm = "Negativo"
            elif val_tok.startswith("POS"):
                val_norm = "Positivo"
            else:
                val_norm = val_raw.capitalize()
        else:
            val_norm = val_raw.capitalize()

        antib_upper = antib.upper()
        valid = any(
            antib_upper.startswith(w) or w.startswith(antib_upper)
            for w in set_antib
        )
        if valid:
            resultados.append((antib_upper, cmi, val_norm))

    if not resultados:
        return [("", "", "")]
    seen, out = set(), []
    for a, c, v in resultados:
        key = (a, c, v)
        if key not in seen:
            seen.add(key)
            out.append((a, c, v))
    return out


def dividir_bloques_por_microorganismo(texto):
    texto = preprocesar_texto(texto)
    if not re.search(r"(?mi)^\s*(\d+\.)|(^\*\s*Microorganismo)", texto):
        return [texto]
    bloques = re.split(r"(?mi)(?:^\s*\d+\.\s*$|(?=^\*\s*Microorganismo))", texto)
    bloques = [b.strip() for b in bloques if b.strip()]
    return bloques


def extraer_todo_por_bloques(texto):
    texto = str(texto or "").strip()
    bloques = dividir_bloques_por_microorganismo(texto)
    resultados = []

    for bloque in bloques:
        micro = extraer_microorganismos(bloque)
        antibs = extraer_antibioticos_cmi_valor(bloque)

        if not micro and antibs == [("", "", "")]:
            continue

        if antibs == [("", "", "")]:
            resultados.append({
                "Microorganismo": micro,
                "Antibiotico": "",
                "CMI": "",
                "ANTVALOR": ""
            })
        else:
            for a, c, v in antibs:
                if not (a or c or v or micro):
                    continue
                resultados.append({
                    "Microorganismo": micro,
                    "Antibiotico": a,
                    "CMI": c,
                    "ANTVALOR": v
                })

    if not resultados:
        return [{"Microorganismo": "", "Antibiotico": "", "CMI": "", "ANTVALOR": ""}]
    return resultados


# ==============================
# LECTURA DE ARCHIVOS
# ==============================
sheets = pd.read_excel(INPUT_PATH, sheet_name=["C. EXT", "URGENCIAS"], engine="openpyxl")
df = pd.concat(sheets.values(), ignore_index=True)

antib_df = pd.read_excel(ANTIB_PATH, sheet_name=ANTIB_SHEET, engine="openpyxl")
columna_antib = antib_df.columns[0]
lista_antib = [limpiar_nombre_antibiotico(x) for x in antib_df[columna_antib].dropna()]
set_antib = set(lista_antib)

# ==============================
# PIPELINE
# ==============================
text_col = detectar_columna_texto(df)
if text_col is None:
    for cand in ["RESULTADO", "Resultado", "resultado"]:
        if cand in df.columns:
            text_col = cand
            break
if text_col is None:
    raise RuntimeError("No se detectó la columna de texto del informe")

df["Resultados_bloques"] = df[text_col].apply(extraer_todo_por_bloques)
df_explotado = df.explode("Resultados_bloques", ignore_index=True)
detalles = pd.DataFrame(df_explotado["Resultados_bloques"].tolist())
df_final = pd.concat(
    [df_explotado.drop(columns=["Resultados_bloques", text_col], errors="ignore"), detalles],
    axis=1
)
df_final["BLEE"] = df[text_col].apply(extraer_blee)

df_final.to_excel(SALIDA_PATH, index=False)
print(f"✅ Archivo generado correctamente: {SALIDA_PATH}")
