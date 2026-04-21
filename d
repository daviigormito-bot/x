"""
===============================================================================
 AUDITORIA DE NOTAS FISCAIS — Minha Base vs. Base do Cliente
===============================================================================
Gera um Excel com 6 abas, onde cada nota fiscal aparece em APENAS UMA aba:

  Aba 1: Apenas no Cliente
  Aba 2: Apenas na Minha Base
  Aba 3: Em ambas — divergência SOMENTE na data de vencimento
  Aba 4: Em ambas — divergência SOMENTE no saldo/valor
  Aba 5: Em ambas — divergência na data E no saldo
  Aba 6: Em ambas — data e saldo IGUAIS (conciliadas)

Requisitos: pandas, openpyxl
  pip install pandas openpyxl
===============================================================================
"""

import pandas as pd
from pathlib import Path

# =============================================================================
# 1. CONFIGURAÇÃO — caminhos dos arquivos
# =============================================================================
arquivo_minha_base = r"C:\Users\silva.d.6\OneDrive - Procter and Gamble\Desktop\Antecipações\MAGAZINE DOWN20260408060103.XLSX"
arquivo_cliente    = r"C:\Users\silva.d.6\OneDrive - Procter and Gamble\Desktop\Antecipações\Cotação_P&G 08.04.2026 - Atualizada.xlsx"
arquivo_saida      = r"C:\Users\silva.d.6\OneDrive - Procter and Gamble\Desktop\Antecipações\relatorio_auditoria.xlsx"

# Tolerância em reais para comparar valores (evita falso positivo por arredondamento)
TOLERANCIA_VALOR = 0.01

# Nomes das colunas (conforme confirmado nas planilhas)
col_minha = {
    "chave": "Fiscal Note",
    "data":  "Expire date",
    "valor": "Gross Amount",
}
col_cliente = {
    "chave": "Número do Título",
    "data":  "Vencimento Ajustado",
    "valor": "Saldo do Título",
}

# =============================================================================
# 2. FUNÇÕES DE LIMPEZA
# =============================================================================
def carregar_arquivo(caminho: str) -> pd.DataFrame:
    """Carrega .xlsx, .xls ou .csv automaticamente."""
    ext = Path(caminho).suffix.lower()
    if ext in (".xlsx", ".xlsm"):
        return pd.read_excel(caminho, dtype=str, engine="openpyxl")
    elif ext == ".xls":
        # Alguns sistemas salvam como .xls mas no formato xlsx. Tenta openpyxl,
        # depois cai para xlrd (requer: pip install xlrd==1.2.0)
        try:
            return pd.read_excel(caminho, dtype=str, engine="openpyxl")
        except Exception:
            return pd.read_excel(caminho, dtype=str, engine="xlrd")
    elif ext == ".csv":
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                return pd.read_csv(caminho, dtype=str, sep=None, engine="python", encoding=enc)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Não foi possível ler o CSV: {caminho}")
    raise ValueError(f"Formato não suportado: {ext}")


def limpar_chave(serie: pd.Series) -> pd.Series:
    """Normaliza a chave primária (Fiscal Note / Número do Título)."""
    s = serie.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)     # "12345.0" -> "12345"
    s = s.str.replace(r"\s+", "", regex=True)      # tira espaços internos
    s = s.str.upper()
    # s = s.str.lstrip("0")                        # descomente se quiser ignorar zeros à esquerda
    return s


def limpar_valor(serie: pd.Series) -> pd.Series:
    """Converte valores em formato brasileiro (R$ 1.234,56) para float."""
    s = serie.astype(str).str.strip()
    s = s.str.replace("R$", "", regex=False)
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace("\xa0", "", regex=False)

    def _converter(x: str):
        if x in ("", "nan", "None", "-"):
            return 0.0
        if "," in x:
            x = x.replace(".", "").replace(",", ".")
        try:
            return float(x)
        except ValueError:
            return 0.0

    return s.apply(_converter)


def limpar_data(serie: pd.Series) -> pd.Series:
    """Converte para datetime normalizando ao início do dia (dd/mm/aaaa)."""
    return pd.to_datetime(serie, dayfirst=True, errors="coerce").dt.normalize()


# =============================================================================
# 3. PIPELINE
# =============================================================================
def preparar_df(df: pd.DataFrame, cols: dict, origem: str) -> pd.DataFrame:
    """Aplica limpeza e renomeia para nomes padronizados."""
    faltando = [c for c in cols.values() if c not in df.columns]
    if faltando:
        raise KeyError(
            f"Colunas não encontradas no arquivo ({origem}): {faltando}\n"
            f"Colunas disponíveis: {list(df.columns)}"
        )

    out = pd.DataFrame({
        "chave": limpar_chave(df[cols["chave"]]),
        f"data_{origem}":  limpar_data(df[cols["data"]]),
        f"valor_{origem}": limpar_valor(df[cols["valor"]]),
    })

    out = out[out["chave"].str.len() > 0].copy()

    if out["chave"].duplicated().any():
        print(f"  ⚠ {out['chave'].duplicated().sum()} chave(s) duplicada(s) em {origem} — agregando.")
        out = out.groupby("chave", as_index=False).agg({
            f"data_{origem}":  "min",
            f"valor_{origem}": "sum",
        })

    return out


def auditar(df_cli: pd.DataFrame, df_meu: pd.DataFrame) -> dict:
    """Executa a comparação e devolve um dicionário de DataFrames (um por aba)."""
    df = df_cli.merge(df_meu, on="chave", how="outer", indicator=True)

    apenas_cliente = df[df["_merge"] == "left_only"].drop(columns="_merge")
    apenas_minha   = df[df["_merge"] == "right_only"].drop(columns="_merge")

    ambos = df[df["_merge"] == "both"].drop(columns="_merge").copy()
    ambos["diff_data"]  = ambos["data_cliente"] != ambos["data_minha"]
    ambos["diff_valor"] = (ambos["valor_cliente"] - ambos["valor_minha"]).abs() > TOLERANCIA_VALOR
    ambos["diferenca_valor"] = (ambos["valor_cliente"] - ambos["valor_minha"]).round(2)

    so_data     = ambos[ ambos["diff_data"] & ~ambos["diff_valor"]]
    so_valor    = ambos[~ambos["diff_data"] &  ambos["diff_valor"]]
    data_valor  = ambos[ ambos["diff_data"] &  ambos["diff_valor"]]
    conciliadas = ambos[~ambos["diff_data"] & ~ambos["diff_valor"]]

    cols_comp = ["chave", "data_cliente", "data_minha",
                 "valor_cliente", "valor_minha", "diferenca_valor"]

    return {
        "1. Apenas Cliente":       apenas_cliente[["chave", "data_cliente", "valor_cliente"]],
        "2. Apenas Minha Base":    apenas_minha[["chave", "data_minha", "valor_minha"]],
        "3. Divergencia Data":     so_data[cols_comp],
        "4. Divergencia Valor":    so_valor[cols_comp],
        "5. Divergencia Data+Val": data_valor[cols_comp],
        "6. Conciliadas":          conciliadas[cols_comp],
    }


def exportar_excel(abas: dict, caminho: str) -> None:
    """Exporta o dicionário de DataFrames num único .xlsx."""
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        for nome, df in abas.items():
            df.to_excel(writer, sheet_name=nome[:31], index=False)
            ws = writer.sheets[nome[:31]]
            for col_idx, col in enumerate(df.columns, start=1):
                largura = max(len(str(col)),
                              df[col].astype(str).str.len().max() if len(df) else 0)
                ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(largura + 2, 40)


# =============================================================================
# 4. EXECUÇÃO
# =============================================================================
def main():
    print("📂 Carregando arquivos...")
    df_meu_raw = carregar_arquivo(arquivo_minha_base)
    df_cli_raw = carregar_arquivo(arquivo_cliente)
    print(f"   Minha Base: {len(df_meu_raw)} linhas | Cliente: {len(df_cli_raw)} linhas")

    print("🧹 Limpando e padronizando...")
    df_meu = preparar_df(df_meu_raw, col_minha,   "minha")
    df_cli = preparar_df(df_cli_raw, col_cliente, "cliente")

    print("🔎 Auditando...")
    abas = auditar(df_cli, df_meu)

    print("\n📊 Resumo:")
    for nome, df in abas.items():
        print(f"   {nome:30s} → {len(df):>6} notas")

    print(f"\n💾 Gerando {arquivo_saida}...")
    exportar_excel(abas, arquivo_saida)
    print("✅ Concluído!")


if __name__ == "__main__":
    main()
