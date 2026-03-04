import pandas as pd
import re
import io

# =============================================================================
# ETAPA 1 — LIMPEZA DO SINAN
# =============================================================================
print("=== [1/4] Carregando e limpando SINAN ===")

df_sinan = pd.read_csv('SINAN_EIXO_2.csv', sep=',')

# Converte data de notificação para datetime
df_sinan['DT_NOTIFIC'] = pd.to_datetime(df_sinan['DT_NOTIFIC'], format='%d/%m/%Y', errors='coerce')

# Extrai colunas de data para facilitar merge semanal/diário
df_sinan['DATA'] = df_sinan['DT_NOTIFIC'].dt.date
df_sinan['ANO']  = df_sinan['DT_NOTIFIC'].dt.year
df_sinan['MES']  = df_sinan['DT_NOTIFIC'].dt.month
df_sinan['SEM_EPIDEM'] = df_sinan['DT_NOTIFIC'].dt.isocalendar().week.astype('Int64')

# Padroniza sexo
df_sinan['CS_SEXO'] = df_sinan['CS_SEXO'].str.upper().str.strip()

# Preenche nulos em colunas categóricas com 'Ignorado'
for col in ['CS_SEXO', 'CS_RACA', 'CS_ESCOL_N', 'EVOLUCAO']:
    df_sinan[col] = df_sinan[col].fillna('Ignorado')

# Preenche nulos numéricos com a mediana
for col in ['IDADE_ANOS', 'SEM_NOT', 'NU_ANO', 'ID_REGIONA', 'ID_MN_RESI', 'ID_RG_RESI']:
    df_sinan[col] = df_sinan[col].fillna(df_sinan[col].median())

# Remove linhas sem data (impossíveis de cruzar)
antes = len(df_sinan)
df_sinan.dropna(subset=['DT_NOTIFIC'], inplace=True)
print(f"  Linhas removidas por data inválida: {antes - len(df_sinan)}") 
print(f"  SINAN limpo: {len(df_sinan)} linhas")

# =============================================================================
# ETAPA 2 — LIMPEZA DO INMET
# =============================================================================
print("\n=== [2/4] Carregando e limpando INMET ===")

# O INMET vem com cada linha encapsulada em aspas duplas externas
# e usa vírgula como separador decimal (ex: ""23","0"" = 23.0).
# Estratégia: lê linha a linha, remove aspas extras e normaliza decimais.

linhas_limpas = []
colunas_inmet = None

col_names = [
    'DATA', 'HORA_UTC',
    'TEMP_INS_C', 'TEMP_MAX_C', 'TEMP_MIN_C',
    'UMI_INS_PCT', 'UMI_MAX_PCT', 'UMI_MIN_PCT',
    'ORVALHO_INS_C', 'ORVALHO_MAX_C', 'ORVALHO_MIN_C',
    'PRESSAO_INS_HPA', 'PRESSAO_MAX_HPA', 'PRESSAO_MIN_HPA',
    'VEL_VENTO_MS', 'DIR_VENTO_MS', 'RAJ_VENTO_MS',
    'RADIACAO_KJM2', 'CHUVA_MM'
]

with open('INMET_EIXO_2.csv', encoding='latin-1') as f:
    for i, linha in enumerate(f):
        # Remove BOM, aspas externas e quebra de linha
        linha = linha.strip().strip('"')
        # Normaliza decimal: substitui padrão ","<dígito> por .<dígito>
        linha = re.sub(r'",(\d)', r'.\1', linha)
        # Remove aspas restantes
        linha = linha.replace('"', '')
        # Divide por ;
        campos = linha.split(';')

        if i == 0:
            # Cabeçalho — ignora e usa nossos nomes padronizados
            continue
        if len(campos) < 2:
            continue
        linhas_limpas.append(campos[:19])  # Garante no máx. 19 campos

df_inmet = pd.DataFrame(linhas_limpas, columns=col_names[:len(linhas_limpas[0])])

# Converte DATA para datetime
df_inmet['DATA'] = pd.to_datetime(df_inmet['DATA'], format='%d/%m/%Y', errors='coerce')

# Converte colunas numéricas (tudo exceto DATA e HORA_UTC)
colunas_num = col_names[2:]
for col in colunas_num:
    if col in df_inmet.columns:
        df_inmet[col] = pd.to_numeric(df_inmet[col], errors='coerce')

# Preenche nulos numéricos com a mediana da coluna
for col in colunas_num:
    if col in df_inmet.columns:
        df_inmet[col] = df_inmet[col].fillna(df_inmet[col].median())

df_inmet.dropna(subset=['DATA'], inplace=True)
print(f"  INMET limpo: {len(df_inmet)} linhas horárias")

# =============================================================================
# ETAPA 3 — AGREGA INMET PARA DIÁRIO
# (média/soma por dia; o SINAN é diário, o INMET é horário)
# =============================================================================
print("\n=== [3/4] Agregando INMET para nível diário ===")

agg_dict = {c: 'mean' for c in colunas_num if c in df_inmet.columns}
if 'CHUVA_MM' in df_inmet.columns:
    agg_dict['CHUVA_MM'] = 'sum'   # Chuva acumula no dia

df_inmet_diario = df_inmet.groupby('DATA').agg(agg_dict).reset_index()
print(f"  INMET diário: {len(df_inmet_diario)} dias")

# =============================================================================
# ETAPA 4 — MERGE: contagem de casos por dia + dados climáticos
# =============================================================================
print("\n=== [4/4] Cruzando SINAN + INMET ===")

# Agrega SINAN: conta casos por dia e município
df_sinan['DATA'] = pd.to_datetime(df_sinan['DATA'])
casos_diarios = (
    df_sinan.groupby(['DATA', 'ID_MUNICIP'])
    .size()
    .reset_index(name='CASOS_DIA')
)

# Merge pelo campo DATA
df_merged = casos_diarios.merge(df_inmet_diario, on='DATA', how='left')

# Calcula variação percentual de casos (rolling 7 dias) por município
df_merged.sort_values(['ID_MUNICIP', 'DATA'], inplace=True)
df_merged['CASOS_7D_MEDIA'] = (
    df_merged.groupby('ID_MUNICIP')['CASOS_DIA']
    .transform(lambda x: x.rolling(7, min_periods=1).mean())
)
df_merged['VARIACAO_7D_PCT'] = (
    df_merged.groupby('ID_MUNICIP')['CASOS_DIA']
    .transform(lambda x: x.pct_change(periods=7) * 100)
).round(2)

# ── Regra de alerta (ajuste os limiares com o colega de Medicina) ──────────
LIMIAR_TEMP_C       = 28.0   # Temperatura média acima de X°C
LIMIAR_CHUVA_MM     = 10.0   # Chuva acumulada no dia acima de X mm
LIMIAR_VARIACAO_PCT = 15.0   # Aumento de casos nos últimos 7 dias acima de X%

if 'TEMP_INS_C' in df_merged.columns and 'CHUVA_MM' in df_merged.columns:
    df_merged['ALERTA'] = (
        (df_merged['TEMP_INS_C']      >= LIMIAR_TEMP_C)      &
        (df_merged['CHUVA_MM']        >= LIMIAR_CHUVA_MM)    &
        (df_merged['VARIACAO_7D_PCT'] >= LIMIAR_VARIACAO_PCT)
    ).map({True: 'VERMELHO', False: 'VERDE'})
else:
    df_merged['ALERTA'] = 'SEM_DADOS_CLIMA'

# =============================================================================
# EXPORTA CSVs LIMPOS para importar no PostgreSQL
# =============================================================================
df_sinan.to_csv('casos_dengue.csv', index=False)
df_inmet_diario.to_csv('dados_climaticos.csv', index=False)
df_merged.to_csv('alertas_gerados.csv', index=False)

print(f"\n✔ casos_dengue.csv     → {len(df_sinan)} linhas")
print(f"✔ dados_climaticos.csv → {len(df_inmet_diario)} linhas")
print(f"✔ alertas_gerados.csv  → {len(df_merged)} linhas")

# Resumo de alertas
if 'ALERTA' in df_merged.columns:
    print("\n── Distribuição de alertas ──")
    print(df_merged['ALERTA'].value_counts().to_string())

print("\n=== COLUNAS DISPONÍVEIS PARA O POSTGRES ===")
print("casos_dengue:    ", df_sinan.columns.tolist())
print("dados_climaticos:", df_inmet_diario.columns.tolist())
print("alertas_gerados: ", df_merged.columns.tolist())
