"""
Funções de enriquecimento de dados: agregações, patentes e relacionamentos.
"""

import pandas as pd
from loaders import load_patentes_inpi_stats, load_ncm_depara


def attach_patent_stats(empresas: pd.DataFrame, patentes: pd.DataFrame) -> pd.DataFrame:
    """Anexa estatísticas de patentes aos dados de empresas."""
    empresas = empresas.copy()
    empresas["cnpj_raiz"] = empresas["nr_cnpj"].astype(str).str[:8]
    direct = patentes.set_index("cnpj")
    root_patents = (
        patentes.groupby("cnpj_raiz", dropna=False)
        .agg(
            patent_ids=("patent_ids", lambda values: sorted({pid for value in values if isinstance(value, list) for pid in value})),
            primeiro_ano=("primeiro_ano", "min"),
            ultimo_ano=("ultimo_ano", "max"),
        )
        .reset_index()
    )
    root_patents["patentes_raiz"] = root_patents["patent_ids"].apply(len)
    root_ipc = (
        patentes.explode("ipc_top")
        .groupby("cnpj_raiz", dropna=False)
        .agg(
            ipc_top=("ipc_top", lambda values: sorted({str(value) for value in values if str(value)})),
        )
        .reset_index()
    )
    root = (
        root_patents.merge(root_ipc, on="cnpj_raiz", how="left")
        .set_index("cnpj_raiz")
    )
    empresas["patentes"] = empresas["nr_cnpj"].map(direct["patentes"]).fillna(0).astype("int64")
    empresas["patentes_raiz"] = empresas["cnpj_raiz"].map(root["patentes_raiz"]).fillna(0).astype("int64")
    direct_ids = empresas["nr_cnpj"].map(direct["patent_ids"])
    root_ids = empresas["cnpj_raiz"].map(root["patent_ids"])
    empresas["patentes_ids"] = [
        direct_value if isinstance(direct_value, list) and direct_value else (root_value if isinstance(root_value, list) else [])
        for direct_value, root_value in zip(direct_ids, root_ids)
    ]
    empresas["patentes_total"] = empresas["patentes_ids"].apply(len).astype("int64")
    empresas["tem_patente"] = empresas["patentes_total"] > 0
    empresas["ipc_top"] = empresas["nr_cnpj"].map(direct["ipc_top"])
    root_ipc = empresas["cnpj_raiz"].map(root["ipc_top"])
    empresas["ipc_top"] = [
        direct_ipc if isinstance(direct_ipc, list) and direct_ipc else (raiz_ipc if isinstance(raiz_ipc, list) else [])
        for direct_ipc, raiz_ipc in zip(empresas["ipc_top"], root_ipc)
    ]
    return empresas


def enrich_empresas_with_patents(empresas: pd.DataFrame) -> pd.DataFrame:
    """Enriquece dados de empresas com informações de patentes."""
    patentes = load_patentes_inpi_stats()
    return attach_patent_stats(empresas, patentes)
