import pandas as pd

# 1. Carregando os dados do SINAN (Epidemiológico)
# Substitua ',' por ';' se o CSV estiver em padrão brasileiro
try:
    df_sinan = pd.read_csv('SINAN_EIXO_2.csv', sep=',') 
    print("SINAN carregado com sucesso! Linhas:", len(df_sinan))
except Exception as e:
    print("Erro no SINAN:", e)

# 2. Carregando os dados do INMET (Climático)
# O parâmetro skiprows=1 ignora a primeira linha suja do cabeçalho
try:
    df_inmet = pd.read_csv('INMET_EIXO_2.csv', sep=';', skiprows=1, on_bad_lines='skip')
    print("INMET carregado com sucesso! Linhas:", len(df_inmet))
except Exception as e:
    print("Erro no INMET:", e)

# 3. Visualização rápida para o colega de Medicina avaliar
print("\n--- Amostra SINAN ---")
try:
    print(df_sinan.head(3))
except:
    print("(sem dados)")

print("\n--- Amostra INMET ---")
try:
    print(df_inmet.head(3))
except:
    print("(sem dados)")

# O PRÓXIMO PASSO: Descobrir o nome da coluna de "Data" em ambos 
# para podermos fazer o cruzamento (merge) dos dados.
