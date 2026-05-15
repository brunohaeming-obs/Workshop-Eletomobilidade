"""
Funções de carregamento e leitura de dados das fontes.
"""

import pandas as pd
import geopandas as gpd

from config import (
    GEOJSON_CACHE, CNAE_XLSX, NCM_XLSX, COMPONENTES_XLSX, COMPONENTES_XLSX_V2,
    PATENTES_INPI_XLSX, EMPRESAS_FATURAMENTO_XLSX, EMPRESAS_CSV,
    UF_ORDER, NA_VALUE
)
from utils import normalize_code, normalize_ncm_code, normalize_cnpj, slugify, cnae_group_from_subclass


def load_municipios() -> gpd.GeoDataFrame:
    """Carrega a malha municipal local, ou baixa pelo geobr se o cache não existir."""
    if GEOJSON_CACHE.exists():
        municipios = gpd.read_file(GEOJSON_CACHE)
    else:
        import geobr
        municipios = geobr.read_municipality(
            code_muni="all", year=2020, simplified=True, verbose=False
        )
        municipios = municipios[municipios["abbrev_state"].isin(UF_ORDER)].copy()
        municipios.to_file(GEOJSON_CACHE, driver="GeoJSON")

    municipios = municipios.to_crs(epsg=4326)
    municipios["code_muni"] = municipios["code_muni"].astype("int64")
    return municipios[municipios["abbrev_state"].isin(UF_ORDER)].copy()


def load_cnae_depara() -> pd.DataFrame:
    """Carrega de-para de CNAE com descrições."""
    cols = [
        "Código grupo CNAE ",
        "Grupo",
        "COD_DIV",
        "Divisão",
        "Intensidade tecnológica por GRUPO",
        "Intensidade tecnológica por DIVISÃO",
    ]
    cnae = pd.read_excel(CNAE_XLSX, usecols=cols).drop_duplicates()
    cnae = cnae.rename(
        columns={
            "Código grupo CNAE ": "cnae_grupo",
            "Grupo": "cnae_grupo_nome",
            "COD_DIV": "cnae_divisao",
            "Divisão": "cnae_divisao_nome",
            "Intensidade tecnológica por GRUPO": "intensidade_grupo",
            "Intensidade tecnológica por DIVISÃO": "intensidade_divisao",
        }
    )
    cnae["cnae_grupo"] = normalize_code(cnae["cnae_grupo"])
    cnae["cnae_divisao"] = normalize_code(cnae["cnae_divisao"])
    text_cols = [
        "cnae_grupo_nome",
        "cnae_divisao_nome",
        "intensidade_grupo",
        "intensidade_divisao",
    ]
    for col in text_cols:
        cnae[col] = cnae[col].fillna(NA_VALUE).astype(str).str.strip()
    return cnae.drop_duplicates(subset=["cnae_grupo"])


def load_ncm_depara() -> pd.DataFrame:
    """Carrega de-para de NCM com grupos CNAE associados."""
    cols = [
        "COD_SH4",
        "Código NCM 8 dígitos",
        "NO_NCM_POR",
        "NO_SH4",
        "Produto",
        "CNAE grupo",
        "Descrição CNAE grupo",
        "CNAE divisão",
        "Descrição CNAE divisão",
        "Intensidade tecnológica",
    ]
    ncm = pd.read_excel(NCM_XLSX, sheet_name="Dim_ncm_cnae_sh4", usecols=cols)
    ncm = ncm.rename(
        columns={
            "COD_SH4": "sh4",
            "Código NCM 8 dígitos": "ncm",
            "NO_NCM_POR": "ncm_nome",
            "NO_SH4": "sh4_nome",
            "Produto": "produto",
            "CNAE grupo": "cnae_grupo",
            "Descrição CNAE grupo": "cnae_grupo_nome",
            "CNAE divisão": "cnae_divisao",
            "Descrição CNAE divisão": "cnae_divisao_nome",
            "Intensidade tecnológica": "intensidade",
        }
    )
    ncm["ncm"] = normalize_code(ncm["ncm"])
    ncm["sh4"] = normalize_code(ncm["sh4"])
    ncm["cnae_grupo"] = normalize_code(ncm["cnae_grupo"])
    ncm["cnae_divisao"] = normalize_code(ncm["cnae_divisao"])
    for col in [
        "ncm_nome",
        "sh4_nome",
        "produto",
        "cnae_grupo_nome",
        "cnae_divisao_nome",
        "intensidade",
    ]:
        ncm[col] = ncm[col].fillna(NA_VALUE).astype(str).str.strip()
    return ncm.drop_duplicates(
        subset=["ncm", "cnae_grupo", "cnae_divisao"]
    ).copy()


def load_componentes_depara() -> pd.DataFrame:
    """Carrega de-para de componentes de eletromobilidade com NCM, criticidade e complexidade."""
    cols = ["GT", "Arquitetura", "Subsistema", "Componente", "Codigo NCM", "Descricao NCM", "Criticidade", "Complexidade"]
    componentes = pd.read_excel(
        COMPONENTES_XLSX_V2,
        sheet_name="NCM_Eletromobilidade",
        usecols=cols,
        dtype={"GT": str, "Arquitetura": str, "Subsistema": str, "Componente": str, "Codigo NCM": str, "Descricao NCM": str, "Criticidade": "int64", "Complexidade": "int64"},
    )
    componentes = componentes.rename(
        columns={
            "GT": "gt",
            "Arquitetura": "arquitetura",
            "Subsistema": "subsistema",
            "Componente": "componente",
            "Codigo NCM": "ncm",
            "Descricao NCM": "ncm_descricao_gt",
            "Criticidade": "criticidade",
            "Complexidade": "complexidade",
        }
    )
    componentes["ncm"] = normalize_ncm_code(componentes["ncm"])
    for col in ["gt", "arquitetura", "subsistema", "componente", "ncm_descricao_gt"]:
        componentes[col] = componentes[col].fillna(NA_VALUE).astype(str).str.strip()

    keys = ["gt", "subsistema", "componente"]
    unique_keys = componentes[keys].drop_duplicates().copy()
    used: dict[str, int] = {}
    ids = []
    for row in unique_keys.itertuples(index=False):
        base = slugify(f"{row.gt}-{row.subsistema}-{row.componente}")[:72].strip("-")
        suffix = used.get(base, 0)
        used[base] = suffix + 1
        ids.append(base if suffix == 0 else f"{base}-{suffix + 1}")
    unique_keys["component_id"] = ids
    componentes = componentes.merge(unique_keys, on=keys, how="left")
    return componentes.drop_duplicates(subset=["component_id", "ncm"]).copy()


def load_patentes_inpi_stats() -> pd.DataFrame:
    """Carrega estatísticas de patentes INPI para CNPJs."""
    cols = ["NO_PEDIDO", "NO_CNPJ_CPF", "CD_TIPO_PFPJ", "CD_CLASSIF", "ANO"]
    patentes = pd.read_excel(PATENTES_INPI_XLSX, usecols=cols, dtype=str)
    patentes = patentes[patentes["CD_TIPO_PFPJ"].eq("J")].copy()
    patentes["cnpj"] = normalize_cnpj(patentes["NO_CNPJ_CPF"])
    patentes = patentes[patentes["cnpj"].str.len().eq(14) & patentes["cnpj"].ne("00000000000000")]
    patentes["cnpj_raiz"] = patentes["cnpj"].str[:8]
    patentes["ipc"] = (
        patentes["CD_CLASSIF"]
        .fillna("")
        .astype(str)
        .str.replace(" ", "", regex=False)
        .str[:4]
    )
    patentes["ano"] = pd.to_numeric(patentes["ANO"], errors="coerce").astype("Int64")
    patentes = patentes.drop_duplicates(subset=["cnpj", "NO_PEDIDO", "ipc"])

    pedido_stats = (
        patentes.drop_duplicates(subset=["cnpj", "NO_PEDIDO"])
        .groupby("cnpj", dropna=False)
        .agg(patentes=("NO_PEDIDO", "nunique"), primeiro_ano=("ano", "min"), ultimo_ano=("ano", "max"))
        .reset_index()
    )
    pedido_ids = (
        patentes.drop_duplicates(subset=["cnpj", "NO_PEDIDO"])
        .groupby("cnpj", dropna=False)["NO_PEDIDO"]
        .apply(lambda values: sorted({str(value).strip() for value in values if str(value).strip()}))
        .reset_index(name="patent_ids")
    )
    ipc_stats = (
        patentes.groupby(["cnpj", "ipc"], dropna=False)["NO_PEDIDO"]
        .nunique()
        .reset_index(name="n")
        .sort_values(["cnpj", "n", "ipc"], ascending=[True, False, True])
    )
    top_ipc = (
        ipc_stats.groupby("cnpj", sort=False)
        .head(6)
        .groupby("cnpj")["ipc"]
        .apply(lambda values: [value for value in values if value])
        .reset_index(name="ipc_top")
    )
    stats = pedido_stats.merge(top_ipc, on="cnpj", how="left").merge(pedido_ids, on="cnpj", how="left")
    stats["ipc_top"] = stats["ipc_top"].apply(lambda value: value if isinstance(value, list) else [])
    stats["patent_ids"] = stats["patent_ids"].apply(lambda value: value if isinstance(value, list) else [])
    stats["cnpj_raiz"] = stats["cnpj"].str[:8]
    return stats


def load_faturamento_depara() -> pd.DataFrame:
    """Carrega de-para de faturamento por CNPJ."""
    faturamento = pd.read_excel(
        EMPRESAS_FATURAMENTO_XLSX,
        usecols=["nr_cnpj", "ds_faixa_faturamento_grupo"],
    )
    faturamento["nr_cnpj"] = (
        faturamento["nr_cnpj"]
        .fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(14)
    )
    faturamento["ds_faixa_faturamento_grupo"] = (
        faturamento["ds_faixa_faturamento_grupo"]
        .fillna(NA_VALUE)
        .astype(str)
        .str.strip()
    )
    return faturamento.drop_duplicates(subset=["nr_cnpj"])


def load_empresas() -> pd.DataFrame:
    """Carrega dados de empresas da RFB com todos os enriquecimentos."""
    usecols = [
        "nr_cnpj",
        "nm_porte_obs_Novo",
        "nm_razao_social",
        "cd_cnae_fiscal_principal",
        "cd_cnae_fiscal_secundaria",
        "sg_uf",
        "cd_municipio_ibge",
    ]
    df = pd.read_csv(
        EMPRESAS_CSV,
        usecols=usecols,
        dtype={
            "nr_cnpj": "string",
            "nm_porte_obs_Novo": "string",
            "nm_razao_social": "string",
            "cd_cnae_fiscal_principal": "string",
            "cd_cnae_fiscal_secundaria": "string",
            "sg_uf": "string",
            "cd_municipio_ibge": "Int64",
        },
    )
    df = df.rename(columns={"nm_razao_social": "nm_razao_social_empresarial"})
    df = df[df["sg_uf"].isin(UF_ORDER)].copy()
    df["cd_municipio_ibge"] = pd.to_numeric(
        df["cd_municipio_ibge"], errors="coerce"
    ).astype("Int64")
    df = df.dropna(subset=["cd_municipio_ibge"])
    df["cd_municipio_ibge"] = df["cd_municipio_ibge"].astype("int64")
    df["nr_cnpj"] = df["nr_cnpj"].fillna("").astype(str).str.replace(r"\D", "", regex=True)
    df["nr_cnpj"] = df["nr_cnpj"].str.zfill(14)
    faturamento = load_faturamento_depara()
    cnpjs_ativos = set(faturamento["nr_cnpj"])
    df = df[df["nr_cnpj"].isin(cnpjs_ativos)].copy()
    df["nm_porte_obs_Novo"] = df["nm_porte_obs_Novo"].fillna(NA_VALUE).astype(str)
    df["nm_razao_social_empresarial"] = (
        df["nm_razao_social_empresarial"].fillna(NA_VALUE).astype(str).str.strip()
    )
    df["cnae_principal"] = (
        df["cd_cnae_fiscal_principal"].fillna(NA_VALUE).astype(str).str.strip()
    )
    df["cnae_secundario"] = (
        df["cd_cnae_fiscal_secundaria"].fillna(NA_VALUE).astype(str).str.strip()
    )
    df = df.merge(faturamento, on="nr_cnpj", how="left")
    df["ds_faixa_faturamento_grupo"] = (
        df["ds_faixa_faturamento_grupo"].fillna(NA_VALUE).astype(str).str.strip()
    )
    df["cnae_grupo"] = cnae_group_from_subclass(df["cd_cnae_fiscal_principal"])

    municipios = load_municipios()[["code_muni", "name_muni"]].rename(
        columns={"code_muni": "cd_municipio_ibge", "name_muni": "nm_municipio"}
    )
    df = df.merge(municipios, on="cd_municipio_ibge", how="left")
    df["nm_municipio"] = df["nm_municipio"].fillna(NA_VALUE).astype(str)

    cnae = load_cnae_depara()
    df = df.merge(cnae, on="cnae_grupo", how="left")
    fill_cols = [
        "cnae_grupo_nome",
        "cnae_divisao",
        "cnae_divisao_nome",
        "intensidade_grupo",
        "intensidade_divisao",
    ]
    for col in fill_cols:
        df[col] = df[col].fillna(NA_VALUE).astype(str)
    return df


def component_cnae_map() -> pd.DataFrame:
    """Carrega mapa de componentes com CNAE associados."""
    componentes = load_componentes_depara()
    ncm = load_ncm_depara()[
        ["ncm", "ncm_nome", "cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome", "intensidade"]
    ].drop_duplicates()
    mapped = componentes.merge(ncm, on="ncm", how="left")
    for col in ["ncm_nome", "cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome", "intensidade"]:
        mapped[col] = mapped[col].fillna(NA_VALUE).astype(str).str.strip()
    return mapped
