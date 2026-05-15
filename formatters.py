"""
Funções auxiliares de formatação e construção de estruturas de dados.
"""

import pandas as pd
from config import NA_VALUE


def option_list(df: pd.DataFrame, value_col: str, label_col: str | None = None) -> list[dict]:
    """Cria lista de opções com contagem de registros únicos."""
    count = df.groupby(value_col, dropna=False)["nr_cnpj"].nunique().reset_index(name="count")
    if label_col:
        labels = df[[value_col, label_col]].drop_duplicates(subset=[value_col])
        count = count.merge(labels, on=value_col, how="left")
    count = count.sort_values(["count", value_col], ascending=[False, True])
    out = []
    for row in count.itertuples(index=False):
        value = str(getattr(row, value_col))
        label = str(getattr(row, label_col)) if label_col else value
        out.append({"value": value, "label": label, "count": int(row.count)})
    return out


def count_informative_revenue(empresas: pd.DataFrame) -> int:
    """Conta empresas com faturamento informado."""
    return int(empresas["ds_faixa_faturamento_grupo"].ne(NA_VALUE).sum())
