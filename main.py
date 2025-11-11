import pandas as pd
import re

# ==============================
# CONFIG
# ==============================
INPUT_PATH = "data/PERFIL MICROBIOLOGICO.xlsx"
ANTIB_PATH = "data/LISTA ANTIBIOTICOS.xlsx"
ANTIB_SHEET = "CONSOLIDADO"
SALIDA_PATH = "data/Perfil Microbiologico Ordenado.xlsx"

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
    # Corrige CMI como "<= 1" a "<=1"
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
    # Optimizado: Comprobar solo con 4 caracteres
    t_trunc = t[:4]
    for target in ANTVALOR_SET:
        if t_trunc == target[:4]:
            return True
    return False


# ===================================================================
# --- FUNCIÓN CORREGIDA #1 ---
# Esta es la corrección más importante.
# Limpia \xa0 PRIMERO, y luego usa un regex más robusto para 0 0.
# ===================================================================
def preprocesar_texto(texto):
    texto = str(texto or "")
    
    # 1. Limpieza universal
    texto = texto.replace("\xa0", " ")
    texto = texto.replace("\r", "")

    # --- INICIO DE LA CORRECCIÓN ---
    # 2. Eliminar el artefacto de Excel para "carriage return" (_x000D_)
    #    Esto es lo que está causando "X000DAMIKACINA"
    #    Usamos re.sub con flags=re.I para ignorar mayúsculas/minúsculas
    
    texto = re.sub(r"_x000D_", "", texto, flags=re.I)
    
    # --- FIN DE LA CORRECCIÓN ---

    # 3. Reemplazar "0 0" por saltos de línea (tu regex con "*" es correcto)
    texto = re.sub(r"[\s\t]*0\s+0(?:\s+\(CRC\))?[\s\t]*", "\n", texto, flags=re.I)
    
    # 4. Limpieza de espacios y saltos
    texto = re.sub(r"[ \t]{2,}", " ", texto)
    texto = re.sub(r"\n{2,}", "\n", texto) 
    return texto.strip()


def extraer_blee(texto):
    texto = str(texto or "")
    for linea in texto.splitlines():
        # Busca BLEE y luego POS o NEG en la *misma* línea
        if re.search(r"(?i)\bBLEE\b", linea):
            if re.search(r"(?i)pos", linea):
                return "Positivo"
            if re.search(r"(?i)neg", linea):
                return "Negativo"
    return ""

def extraer_microorganismos(texto):
    # --- SE ELIMINA LA LIMPIEZA REDUNDANTE ---
    # El texto ya viene pre-procesado
    
    # 1. Buscar encabezado clásico
    # (Se añade \s* al inicio para que coincida con líneas con sangría)
    patron_micro = re.compile(
        r"(?im)^\s*MICROORGANISMO[:\s]+([A-Za-zÁÉÍÓÚÑáéíóúñ\.]{2,}(?:[ \t]+[A-Za-zÁÉÍÓÚÑáéíóúñ\.]{1,}){0,4})"
    )
    m = patron_micro.search(texto)
    if m:
        nombre = m.group(1)
    else:
        # 2. Buscar formato tipo "Microorganismo   Salmonella..."
        patron_inline = re.compile(
            r"(?i)(?<!Este\s)microorganismo[ \t]*[:\-]?[ \t]*([A-Za-zÁÉÍÓÚÑáéíóúñ\.]{3,}(?:[ \t]+[A-Za-zÁÉÍÓÚÑáéíóúñ\.]{2,}){0,4})"
        )
        m = patron_inline.search(texto)
        if m:
            nombre = m.group(1)
        else:
            # 3. Buscar microorganismo por nombre conocido (fallback)
            patron_directo = re.compile(
                r"(?i)\b(Escherichia\s+col[ia]?|Klebsiella\s+pneu[a-z]*|Enterococcus\s+fae[a-z]*|Proteus\s+mirab[a-z]*|Staphylococcus\s+aureus|Salmonella\s+enterica(?:\s+ssp\s+enterica)?|Acinetobacter\s+baum[a-z]*|Pseudomonas\s+aer[a-z]*)"
            )
            m = patron_directo.search(texto)
            nombre = m.group(1) if m else ""
    
    if not nombre:
        return "No identificado"

    # Normalizar
    nombre = re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóúñ\s\-\.]", "", nombre)
    reemplazos = {
        r"\bcol\b": "coli",
        r"\bmirabil\b": "mirabilis",
        r"\baur\b": "aureus",
        r"\bpneu\b": "pneumoniae",
        r"\baero\b": "aeruginosa", # Corregido para tu ejemplo de Pseudomonas
        r"\bbaum\b": "baumannii",
        r"\bfa\b": "faecalis",
    }
    for k, v in reemplazos.items():
        nombre = re.sub(k, v, nombre, flags=re.I)

    return " ".join(p.capitalize() for p in nombre.split())

# ===================================================================
# --- FUNCIÓN CORREGIDA #2 ---
# Esta es la segunda corrección.
# Usa un regex para capturar CMI con espacios (ej. "<= 1")
# ===================================================================
def extraer_antibioticos_cmi_valor(texto):
    texto = str(texto or "").strip() 
    texto = texto.replace("\r", "")
    resultados = []
    
    # Este patrón regex busca el antibiótico (Grupo 1) y la CMI (Grupo 2)
    # Es capaz de manejar "ANTIBIOTICO 4" y "ANTIBIOTICO <= 1"
    patron_cmi_completo = re.compile(r"^(.*?)\s+([<>]=?\s*\d+\.?\d*|\d+\.?\d*)$")

    for linea in texto.splitlines():
        linea_orig = linea.strip()
        if not linea_orig:
            continue

        # 1. PRIMERO limpiamos el "0 0" al final
        linea = re.sub(r"\s+0\s+0(?:\s+\(CRC\))?\s*$", "", linea_orig, flags=re.I)
        
        # 2. Limpieza de caracteres no deseados
        linea = re.sub(r"[^A-Za-zÁÉÍÓÚÑáéíóúñ0-9 /:\-<>=.()+µ]", "", linea)
        linea = linea.strip()
        
        if not linea:
            continue
        
        # 3. Dividir por espacios y analizar tokens
        tokens = linea.split()
        if len(tokens) < 2:
            continue
        
        # Identificar qué tokens son valores (SENSIBLE, RESISTENTE, etc)
        antivalor_idx = None
        for i, tok in enumerate(tokens):
            tok_norm = normalizar_token(tok)
            if es_antivalor_truncado(tok_norm):
                antivalor_idx = i
                break
        
        if antivalor_idx is None:
            continue
        
        # El valor es el token identificado
        val_raw = tokens[antivalor_idx]
        
        # Todo antes del valor es potencialmente antibiótico + CMI
        partes_antes = tokens[:antivalor_idx]
        if not partes_antes:
            continue
        
        # Reconstruir la parte anterior (antibiótico + CMI)
        texto_anterior = " ".join(partes_antes)
        
        antib_raw = None
        cmi_raw = None
        
        # Aplicar el regex para separar antibiótico de CMI
        m_cmi = patron_cmi_completo.search(texto_anterior)
        
        if m_cmi:
            # Regex encontró ambos: Antibiótico (grupo 1) y CMI (grupo 2)
            antib_raw = m_cmi.group(1).strip()
            cmi_raw = m_cmi.group(2).strip()
        else:
            # Si el regex falla (ej. solo es "BLEE"), tomar todo como antibiótico
            antib_raw = texto_anterior.strip()
            cmi_raw = ""

        antib = limpiar_nombre_antibiotico(antib_raw)
        cmi = limpiar_cmi(cmi_raw) if cmi_raw else ""
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
        
        # No agregar "BLEE" como un antibiótico
        if antib_upper == "BLEE":
            valid = False
            
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
    if not re.search(r"(?mi)(^\s*\d+\.)|(^\*\s*Microorganismo)", texto):
        return [texto]
    
    bloques = re.split(r"(?mi)(?:^\s*\d+\.\s*$|(?=^\*\s*Microorganismo))", texto)
    bloques = [b.strip() for b in bloques if b.strip()]
    return bloques


def extraer_todo_por_bloques(texto):
    # --- INICIO DE LA CORRECCIÓN ---
    # Se pre-procesa el texto UNA SOLA VEZ al inicio.
    texto_procesado = preprocesar_texto(str(texto or ""))
    # --- FIN DE LA CORRECCIÓN ---

    bloques = dividir_bloques_por_microorganismo(texto_procesado)
    resultados = []

    for bloque in bloques:
        # Todas las funciones ahora reciben el MISMO bloque limpio
        micro = extraer_microorganismos(bloque)
        if not micro:
            micro = "No identificado"

        blee = extraer_blee(bloque)
        
        antibs = extraer_antibioticos_cmi_valor(bloque)

        if antibs == [("", "", "")]:
            # No se encontraron antibióticos
            resultados.append({
                "Microorganismo": micro,
                "Antibiotico": "",
                "CMI": "",
                "ANTVALOR": "",
                "BLEE": blee
            })
        else:
            # Se encontraron antibióticos
            for a, c, v in antibs:
                if not (a or c or v or micro):
                    continue
                resultados.append({
                    "Microorganismo": micro,
                    "Antibiotico": a,
                    "CMI": c,
                    "ANTVALOR": v,
                    "BLEE": blee
                })

    if not resultados:
        return [{"Microorganismo": "No identificado", "Antibiotico": "", "CMI": "", "ANTVALOR": "", "BLEE": ""}]
    return resultados


# ==============================
# LECTURA DE ARCHIVOS
# ==============================
print("Leyendo archivos...")
# Lee las hojas en un diccionario
sheets_dict = pd.read_excel(INPUT_PATH, sheet_name=["C. EXT", "URGENCIAS"], engine="openpyxl")

# Itera por el diccionario para agregar la columna 'Origen' a cada DataFrame
all_dfs = []
for sheet_name, sheet_df in sheets_dict.items():
    sheet_df['Origen'] = sheet_name  # <-- Aquí creamos la nueva columna
    all_dfs.append(sheet_df)

# Concatena los DataFrames ya etiquetados
df = pd.concat(all_dfs, ignore_index=True)

antib_df = pd.read_excel(ANTIB_PATH, sheet_name=ANTIB_SHEET, engine="openpyxl")
columna_antib = antib_df.columns[0]
lista_antib = [limpiar_nombre_antibiotico(x) for x in antib_df[columna_antib].dropna()]
set_antib = set(lista_antib)
print(f"Lista de {len(set_antib)} antibióticos cargada.")

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

print("Procesando resultados...")
df["Resultados_bloques"] = df[text_col].apply(extraer_todo_por_bloques)
df_explotado = df.explode("Resultados_bloques", ignore_index=True)

print("Ensamblando DataFrame final...")
detalles = pd.DataFrame(df_explotado["Resultados_bloques"].tolist())
df_final = pd.concat(
    [df_explotado.drop(columns=["Resultados_bloques", text_col], errors="ignore"), detalles],
    axis=1
)

# --- CORRECCIÓN DE LÓGICA BLEE ---
# La lógica de BLEE estaba fuera de lugar. Ahora se extrae "por bloque"
# dentro de extraer_todo_por_bloques. Así que la siguiente línea ya no es necesaria
# df_final["BLEE"] = df[text_col].apply(extraer_blee)
# En su lugar, nos aseguramos de que la columna exista
if "BLEE" not in df_final.columns:
    df_final["BLEE"] = ""

# --- Mover la columna Origen al inicio ---
print("Reordenando columnas para poner 'Origen' al inicio...")
cols = df_final.columns.tolist()
# Mueve la columna 'Origen' a la posición 0
cols.insert(0, cols.pop(cols.index('Origen')))
df_final = df_final[cols]
# --- Fin del reordenamiento ---


# ==============================
# SALIDA FINAL
# ==============================
df_final.to_excel(SALIDA_PATH, index=False)
print(f"✅ Archivo generado correctamente: {SALIDA_PATH}")