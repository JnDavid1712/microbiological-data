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
    s = str(valor).strip()
    s = re.sub(r"\s+", "", s) 
    return s


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
    # Se expande para incluir el formato "0 0 (CRC)"
    texto = re.sub(r"\b0\s+0(?:\s+\(CRC\))?\b", "\n", texto, flags=re.I)
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
    # limpieza ligera pero manteniendo estructura
    texto = texto.replace("\xa0", " ")
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\r+", "\n", texto)
    texto = re.sub(r"\n{2,}", "\n", texto).strip()

    # --- INICIO DE LA CORRECCIÓN ---
    
    # 1. Buscar encabezado clásico
    # (AJUSTADO) Se cambia \s+ por [ \t]+ para que no cruce saltos de línea
    patron_micro = re.compile(
        r"(?im)^[ \t]*MICROORGANISMO[:\s]+([A-Za-zÁÉÍÓÚÑáéíóúñ\.]{2,}(?:[ \t]+[A-Za-zÁÉÍÓÚÑáéíóúñ\.]{1,}){0,4})"
    )
    m = patron_micro.search(texto)
    if m:
        nombre = m.group(1)
    else:
        # 2. Buscar formato tipo "Microorganismo   Salmonella..."
        # (AJUSTADO) Se cambia \s* por [ \t]* y \s+ por [ \t]+
        patron_inline = re.compile(
            r"(?i)(?<!Este\s)microorganismo[ \t]*[:\-]?[ \t]*([A-Za-zÁÉÍÓÚÑáéíóúñ\.]{3,}(?:[ \t]+[A-Za-zÁÉÍÓÚÑáéíóúñ\.]{2,}){0,4})"
        )
        m = patron_inline.search(texto)
        if m:
            nombre = m.group(1)
        else:
            # 3. Buscar microorganismo por nombre conocido (fallback)
            patron_directo = re.compile(
                r"(?i)\b(Escherichia\s+col[ia]?|Klebsiella\s+pneumo[a-z]*|Enterococcus\s+fae[a-z]*|Proteus\s+mirab[a-z]*|Staphylococcus\s+aureus|Salmonella\s+enterica(?:\s+ssp\s+enterica)?|Acinetobacter\s+baum[a-z]*)"
            )
            m = patron_directo.search(texto)
            nombre = m.group(1) if m else ""
    
    # --- FIN DE LA CORRECCIÓN ---

    if not nombre:
        return "No identificado"

    # Normalizar
    nombre = re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóúñ\s\-\.]", "", nombre)
    reemplazos = {
        r"\bcol\b": "coli",
        r"\bmirabil\b": "mirabilis",
        r"\baur\b": "aureus",
        r"\bpneu\b": "pneumoniae",
        r"\baero\b": "aerogenes",
        r"\bbaum\b": "baumannii",
        r"\bfa\b": "faecalis",
    }
    for k, v in reemplazos.items():
        nombre = re.sub(k, v, nombre, flags=re.I)

    return " ".join(p.capitalize() for p in nombre.split())


def extraer_antibioticos_cmi_valor(texto):
    # Ya no usamos preprocesar_texto aquí, para mantener el texto
    # lo más original posible y no confundir las líneas.
    texto = str(texto or "").strip() 
    texto = texto.replace("\r", "")
    resultados = []

    for linea in texto.splitlines():
        linea = linea.strip()
        if not linea:
            continue

        # 1. Limpieza de caracteres no deseados
        linea = re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóúñ0-9 /:\-<>=.()+µ]", "", linea)
        
        # 2. Eliminamos los sufijos " 0 0 " (o variaciones) de la linea para aislar la info
        # Esto es crucial.
        linea = re.sub(r"\s+0\s+0(?:\s+\(CRC\))?\s*$", "", linea, flags=re.I)
        
        # Regex (Simplificado y robusto):
        # Grupo 1: Antibiótico (lo más amplio al inicio)
        # Grupo 2: CMI (opcional, permite operadores y espacios)
        # Grupo 3: Valor (la palabra clave)
        m = re.match(
            r"^([A-Za-zÁÉÍÓÚÑáéíóúñ0-9 /()\-]+?)\s*"
            r"(?:([<>]=?\s*\d*\.?\d*)\s*)?"
            r"([A-Za-zÁÉÍÓÚÑáéíóúñ()\-+]+)$",
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
        # Verificación contra la lista de antibióticos
        valid = any(
            antib_upper.startswith(w) or w.startswith(antib_upper)
            for w in set_antib
        )
        if valid:
            resultados.append((antib_upper, cmi, val_norm))

    if not resultados:
        return [("", "", "")]
    # Eliminación de duplicados
    seen, out = set(), []
    for a, c, v in resultados:
        key = (a, c, v)
        if key not in seen:
            seen.add(key)
            out.append((a, c, v))
    return out

def dividir_bloques_por_microorganismo(texto):
    texto = preprocesar_texto(texto)
    if not re.search(r"(?mi)(^\s*\d+\.)|(^\*\s*Microorganismo)", texto):
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
        if not micro:
            micro = "No identificado"

        antibs = extraer_antibioticos_cmi_valor(bloque)

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
        return [{"Microorganismo": "No identificado", "Antibiotico": "", "CMI": "", "ANTVALOR": ""}]
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

# ==============================
# SALIDA FINAL
# ==============================
df_final.to_excel(SALIDA_PATH, index=False)
print(f"✅ Archivo generado correctamente: {SALIDA_PATH}")