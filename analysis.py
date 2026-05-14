from __future__ import annotations

import json
import re
import shutil
import unicodedata
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dados"
EMPRESAS_CSV = DATA_DIR / "empresas_rfb_2.csv"
EMPRESAS_FATURAMENTO_XLSX = DATA_DIR / "empresas_rfb_databricks.xlsx"
CNAE_XLSX = DATA_DIR / "CNAE subclasse - classe - grupo - divisão.xlsx"
NCM_XLSX = DATA_DIR / "NCM_SH4 - atualizado 07.04.2026.xlsx"
COMPONENTES_XLSX = DATA_DIR / "GT_NCM_Dados_Brutos.xlsx"
PATENTES_INPI_XLSX = DATA_DIR / "INPI - patentes_depositantes_ipc.xlsx"
GEOJSON_CACHE = DATA_DIR / "municipios_sul_geobr_2020.geojson"
OUTPUT_HTML = ROOT / "visualizacao_empresas_rfb.html"
OUTPUT_NCM_HTML = ROOT / "visualizacao_ncm_empresas.html"
OUTPUT_COMPONENTES_HTML = ROOT / "visualizacao_componentes.html"
DETAIL_DIR = ROOT / "empresas_app_data" / "municipios"
DIST_DIR = ROOT / "dist"
CNPJ_INDEX_DIR = ROOT / "empresas_app_data" / "cnpj_index"
COMPONENT_INDEX_DIR = ROOT / "empresas_app_data" / "componentes"
LEGACY_CNPJ_INDEX = ROOT / "empresas_app_data" / "cnpj_index.json"

UF_ORDER = ["PR", "SC", "RS"]
NA_VALUE = "Não informado"


def normalize_code(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(series, errors="coerce")
        .astype("Int64")
        .astype(str)
        .replace("<NA>", NA_VALUE)
    )


def normalize_ncm_code(series: pd.Series) -> pd.Series:
    digits = series.fillna("").astype(str).str.replace(r"\D", "", regex=True)
    return digits.str.zfill(8).replace({"00000000": NA_VALUE, "": NA_VALUE})


def normalize_cnpj(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(14)
    )


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text or "item"


def cnae_group_from_subclass(series: pd.Series) -> pd.Series:
    digits = series.fillna("").astype(str).str.replace(r"\D", "", regex=True)
    return digits.str.zfill(7).str[:3].replace({"000": NA_VALUE, "": NA_VALUE})


def load_municipios() -> gpd.GeoDataFrame:
    """Carrega a malha municipal local, ou baixa pelo geobr se o cache nao existir."""
    import geopandas as gpd

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
    cols = ["GT", "Arquitetura", "Subsistema", "Componente", "Codigo NCM", "Descricao NCM"]
    componentes = pd.read_excel(
        COMPONENTES_XLSX,
        sheet_name="NCM_Eletromobilidade",
        usecols=cols,
        dtype=str,
    )
    componentes = componentes.rename(
        columns={
            "GT": "gt",
            "Arquitetura": "arquitetura",
            "Subsistema": "subsistema",
            "Componente": "componente",
            "Codigo NCM": "ncm",
            "Descricao NCM": "ncm_descricao_gt",
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
    stats = pedido_stats.merge(top_ipc, on="cnpj", how="left")
    stats["ipc_top"] = stats["ipc_top"].apply(lambda value: value if isinstance(value, list) else [])
    stats["cnpj_raiz"] = stats["cnpj"].str[:8]
    return stats


def attach_patent_stats(empresas: pd.DataFrame, patentes: pd.DataFrame) -> pd.DataFrame:
    empresas = empresas.copy()
    empresas["cnpj_raiz"] = empresas["nr_cnpj"].astype(str).str[:8]
    direct = patentes.set_index("cnpj")
    root = (
        patentes.explode("ipc_top")
        .groupby("cnpj_raiz", dropna=False)
        .agg(
            patentes_raiz=("patentes", "sum"),
            primeiro_ano=("primeiro_ano", "min"),
            ultimo_ano=("ultimo_ano", "max"),
            ipc_top=("ipc_top", lambda values: sorted({str(value) for value in values if str(value)})),
        )
        .reset_index()
        .set_index("cnpj_raiz")
    )
    empresas["patentes"] = empresas["nr_cnpj"].map(direct["patentes"]).fillna(0).astype("int64")
    empresas["patentes_raiz"] = empresas["cnpj_raiz"].map(root["patentes_raiz"]).fillna(0).astype("int64")
    empresas["patentes_total"] = empresas[["patentes", "patentes_raiz"]].max(axis=1).astype("int64")
    empresas["tem_patente"] = empresas["patentes_total"] > 0
    empresas["ipc_top"] = empresas["nr_cnpj"].map(direct["ipc_top"])
    root_ipc = empresas["cnpj_raiz"].map(root["ipc_top"])
    empresas["ipc_top"] = [
        direct_ipc if isinstance(direct_ipc, list) and direct_ipc else (raiz_ipc if isinstance(raiz_ipc, list) else [])
        for direct_ipc, raiz_ipc in zip(empresas["ipc_top"], root_ipc)
    ]
    return empresas


def component_cnae_map() -> pd.DataFrame:
    componentes = load_componentes_depara()
    ncm = load_ncm_depara()[
        ["ncm", "ncm_nome", "cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome", "intensidade"]
    ].drop_duplicates()
    mapped = componentes.merge(ncm, on="ncm", how="left")
    for col in ["ncm_nome", "cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome", "intensidade"]:
        mapped[col] = mapped[col].fillna(NA_VALUE).astype(str).str.strip()
    return mapped


def load_faturamento_depara() -> pd.DataFrame:
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


def write_detail_files(empresas: pd.DataFrame) -> None:
    if DETAIL_DIR.exists():
        shutil.rmtree(DETAIL_DIR)
    DETAIL_DIR.mkdir(parents=True, exist_ok=True)
    cols = [
        "nr_cnpj",
        "nm_razao_social_empresarial",
        "nm_porte_obs_Novo",
        "ds_faixa_faturamento_grupo",
        "cnae_principal",
        "cnae_secundario",
        "cnae_grupo",
        "cnae_grupo_nome",
        "cnae_divisao",
        "cnae_divisao_nome",
        "intensidade_grupo",
        "intensidade_divisao",
    ]
    renamed = {
        "nr_cnpj": "cnpj",
        "nm_razao_social_empresarial": "razao",
        "nm_porte_obs_Novo": "porte",
        "ds_faixa_faturamento_grupo": "faturamento",
        "cnae_principal": "cnaePrincipal",
        "cnae_secundario": "cnaeSecundario",
        "cnae_grupo": "grupo",
        "cnae_grupo_nome": "grupoNome",
        "cnae_divisao": "divisao",
        "cnae_divisao_nome": "divisaoNome",
        "intensidade_grupo": "techGroup",
        "intensidade_divisao": "techDivision",
    }
    detail_page_size = 5000
    for code, part in empresas.groupby("cd_municipio_ibge", sort=False):
        records = part[cols].drop_duplicates(subset=["nr_cnpj"]).rename(columns=renamed).fillna(NA_VALUE).copy()
        records["cnpj"] = records["cnpj"].astype(str)
        records = records.sort_values(["faturamento", "razao"], kind="stable")
        city_dir = DETAIL_DIR / str(int(code))
        city_dir.mkdir(parents=True, exist_ok=True)
        columns = list(records.columns)
        total_rows = int(len(records))
        total_pages = max(1, (total_rows + detail_page_size - 1) // detail_page_size)
        manifest = {
            "columns": columns,
            "rows": total_rows,
            "pageSize": detail_page_size,
            "pages": total_pages,
        }
        (city_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        for page_idx in range(total_pages):
            start = page_idx * detail_page_size
            page = records.iloc[start : start + detail_page_size]
            (city_dir / f"{page_idx + 1}.json").write_text(
                json.dumps(
                    {"rows": page.values.tolist()},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )


def write_cnpj_index(empresas: pd.DataFrame) -> None:
    if LEGACY_CNPJ_INDEX.exists():
        LEGACY_CNPJ_INDEX.unlink()
    if CNPJ_INDEX_DIR.exists():
        shutil.rmtree(CNPJ_INDEX_DIR)
    CNPJ_INDEX_DIR.mkdir(parents=True, exist_ok=True)
    cols = [
        "nr_cnpj",
        "nm_razao_social_empresarial",
        "cd_municipio_ibge",
        "cnae_grupo",
        "cnae_divisao",
        "ds_faixa_faturamento_grupo",
    ]
    index = empresas[cols].drop_duplicates(subset=["nr_cnpj"]).copy()
    index["nr_cnpj"] = index["nr_cnpj"].astype(str)
    index["cd_municipio_ibge"] = index["cd_municipio_ibge"].astype(str)
    index = index.rename(
        columns={
            "nr_cnpj": "value",
            "nm_razao_social_empresarial": "name",
            "cd_municipio_ibge": "code",
            "cnae_grupo": "group",
            "cnae_divisao": "division",
            "ds_faixa_faturamento_grupo": "revenue",
        }
    )
    index["prefix"] = index["value"].str[:4].str.zfill(4)
    columns = [col for col in index.columns if col != "prefix"]
    manifest = {}
    for prefix, part in index.groupby("prefix", sort=True):
        payload = {"columns": columns, "rows": part[columns].values.tolist()}
        path = CNPJ_INDEX_DIR / f"{prefix}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        manifest[str(prefix)] = int(len(part))
    (CNPJ_INDEX_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def write_component_files(empresas: pd.DataFrame, mapped: pd.DataFrame) -> None:
    if COMPONENT_INDEX_DIR.exists():
        shutil.rmtree(COMPONENT_INDEX_DIR)
    COMPONENT_INDEX_DIR.mkdir(parents=True, exist_ok=True)

    cols = [
        "nr_cnpj",
        "nm_razao_social_empresarial",
        "cd_municipio_ibge",
        "sg_uf",
        "nm_municipio",
        "ds_faixa_faturamento_grupo",
        "cnae_grupo",
        "cnae_grupo_nome",
        "cnae_divisao",
        "cnae_divisao_nome",
        "patentes_total",
        "ipc_top",
    ]
    renamed = {
        "nr_cnpj": "cnpj",
        "nm_razao_social_empresarial": "razao",
        "cd_municipio_ibge": "municipioCodigo",
        "sg_uf": "uf",
        "nm_municipio": "municipio",
        "ds_faixa_faturamento_grupo": "faturamento",
        "cnae_grupo": "grupo",
        "cnae_grupo_nome": "grupoNome",
        "cnae_divisao": "divisao",
        "cnae_divisao_nome": "divisaoNome",
        "patentes_total": "patentes",
        "ipc_top": "ipc",
    }
    page_size = 2000
    manifest: dict[str, dict] = {}
    for component_id, part in mapped.groupby("component_id", sort=False):
        groups = {str(value) for value in part["cnae_grupo"] if str(value) != NA_VALUE}
        first = part.iloc[0]
        ncm_by_group = (
            part.loc[part["cnae_grupo"].ne(NA_VALUE), ["cnae_grupo", "ncm"]]
            .drop_duplicates()
            .groupby("cnae_grupo")["ncm"]
            .apply(lambda values: sorted({str(value) for value in values}))
            .to_dict()
        )
        component_dir = COMPONENT_INDEX_DIR / str(component_id)
        component_dir.mkdir(parents=True, exist_ok=True)
        if groups:
            records = empresas[empresas["cnae_grupo"].isin(groups)][cols].drop_duplicates(subset=["nr_cnpj"]).copy()
        else:
            records = empresas.iloc[0:0][cols].copy()
        records = records.rename(columns=renamed).fillna(NA_VALUE)
        records["cnpj"] = records["cnpj"].astype(str)
        records["municipioCodigo"] = records["municipioCodigo"].astype(str)
        records["gt"] = first["gt"]
        records["subsistema"] = first["subsistema"]
        records["componente"] = first["componente"]
        records["componentId"] = str(component_id)
        records["ncms"] = records["grupo"].map(lambda value: ncm_by_group.get(str(value), []))
        records["ipc"] = records["ipc"].apply(lambda value: value if isinstance(value, list) else [])
        records = records.sort_values(["patentes", "razao"], ascending=[False, True], kind="stable")
        columns = list(records.columns)
        total_rows = int(len(records))
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        component_manifest = {
            "columns": columns,
            "rows": total_rows,
            "pageSize": page_size,
            "pages": total_pages,
        }
        manifest[str(component_id)] = {
            "rows": total_rows,
            "pages": total_pages,
            "pageSize": page_size,
        }
        (component_dir / "manifest.json").write_text(
            json.dumps(component_manifest, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        for page_idx in range(total_pages):
            start = page_idx * page_size
            page = records.iloc[start : start + page_size]
            (component_dir / f"{page_idx + 1}.json").write_text(
                json.dumps(
                    {"rows": page.values.tolist()},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
    (COMPONENT_INDEX_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def option_list(df: pd.DataFrame, value_col: str, label_col: str | None = None) -> list[dict]:
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
    return int(empresas["ds_faixa_faturamento_grupo"].ne(NA_VALUE).sum())


def build_payload(empresas: pd.DataFrame, municipios: gpd.GeoDataFrame) -> dict:
    city = (
        empresas.groupby(["cd_municipio_ibge", "sg_uf", "nm_municipio"], dropna=False)
        .agg(empresas=("nr_cnpj", "nunique"), cnpjs=("nr_cnpj", "nunique"))
        .reset_index()
        .rename(columns={"cd_municipio_ibge": "code_muni"})
    )

    merged = municipios.merge(city, on="code_muni", how="inner")
    centroids = merged.to_crs(epsg=5880).geometry.centroid.to_crs(epsg=4326)
    points = []
    for row, point in zip(merged.itertuples(index=False), centroids):
        points.append(
            {
                "code": str(row.code_muni),
                "name": row.nm_municipio,
                "uf": row.sg_uf,
                "lat": round(point.y, 6),
                "lon": round(point.x, 6),
                "total": int(row.empresas),
            }
        )

    geo = merged[
        ["code_muni", "name_muni", "abbrev_state", "empresas", "geometry"]
    ].rename(
        columns={
            "code_muni": "code",
            "name_muni": "name",
            "abbrev_state": "uf",
            "empresas": "total",
        }
    )
    geo["code"] = geo["code"].astype(str)

    record_cols = [
        "cd_municipio_ibge",
        "ds_faixa_faturamento_grupo",
        "cnae_grupo",
        "cnae_divisao",
        "intensidade_grupo",
        "intensidade_divisao",
    ]
    records = (
        empresas.groupby(record_cols, dropna=False)["nr_cnpj"]
        .nunique()
        .reset_index(name="n")
        .rename(
            columns={
                "cd_municipio_ibge": "code",
                "ds_faixa_faturamento_grupo": "revenue",
                "cnae_grupo": "group",
                "cnae_divisao": "division",
                "intensidade_grupo": "techGroup",
                "intensidade_divisao": "techDivision",
            }
        )
    )
    records["code"] = records["code"].astype(str)

    group_labels = (
        empresas[["cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome"]]
        .drop_duplicates(subset=["cnae_grupo"])
        .set_index("cnae_grupo")
        .to_dict(orient="index")
    )
    division_labels = (
        empresas[["cnae_divisao", "cnae_divisao_nome"]]
        .drop_duplicates(subset=["cnae_divisao"])
        .set_index("cnae_divisao")["cnae_divisao_nome"]
        .to_dict()
    )
    ncm = load_ncm_depara()
    ncm_options = (
        ncm[["ncm", "ncm_nome", "sh4", "sh4_nome", "produto"]]
        .drop_duplicates(subset=["ncm"])
        .sort_values("ncm")
    )
    ncm_map: dict[str, dict] = {}
    for code, part in ncm.groupby("ncm", sort=False):
        first = part.iloc[0]
        ncm_map[str(code)] = {
            "ncm": str(code),
            "name": first.ncm_nome,
            "sh4": first.sh4,
            "sh4Name": first.sh4_nome,
            "product": first.produto,
            "groups": [
                {
                    "group": row.cnae_grupo,
                    "groupName": row.cnae_grupo_nome,
                    "division": row.cnae_divisao,
                    "divisionName": row.cnae_divisao_nome,
                    "tech": row.intensidade,
                }
                for row in part.itertuples(index=False)
            ],
        }

    return {
        "summary": {
            "empresas": int(len(empresas)),
            "cnpjs": int(empresas["nr_cnpj"].nunique()),
            "municipios": int(city["code_muni"].nunique()),
            "ufs": UF_ORDER,
            "sem_depara": int((empresas["cnae_grupo_nome"] == NA_VALUE).sum()),
            "ncm": int(ncm["ncm"].nunique()),
        },
        "geojson": json.loads(geo.to_json()),
        "points": points,
        "records": records.to_dict(orient="records"),
        "labels": {"groups": group_labels, "divisions": division_labels},
        "options": {
            "revenue": option_list(empresas, "ds_faixa_faturamento_grupo"),
            "group": option_list(empresas, "cnae_grupo", "cnae_grupo_nome"),
            "division": option_list(empresas, "cnae_divisao", "cnae_divisao_nome"),
            "ncm": [
                {
                    "value": str(row.ncm),
                    "label": f"{row.ncm} - {row.ncm_nome}",
                    "sh4": str(row.sh4),
                    "sh4Name": row.sh4_nome,
                    "product": row.produto,
                    "count": 0,
                }
                for row in ncm_options.itertuples(index=False)
            ],
            "cnpj": [],
        },
        "ncmMap": ncm_map,
    }


def build_ncm_payload(empresas: pd.DataFrame, municipios: gpd.GeoDataFrame) -> dict:
    ncm = load_ncm_depara()
    city = (
        empresas.groupby(["cd_municipio_ibge", "sg_uf", "nm_municipio"], dropna=False)
        .agg(empresas=("nr_cnpj", "nunique"))
        .reset_index()
        .rename(columns={"cd_municipio_ibge": "code_muni"})
    )
    merged = municipios.merge(city, on="code_muni", how="inner")
    centroids = merged.to_crs(epsg=5880).geometry.centroid.to_crs(epsg=4326)
    points = [
        {
            "code": str(row.code_muni),
            "name": row.nm_municipio,
            "uf": row.sg_uf,
            "lat": round(point.y, 6),
            "lon": round(point.x, 6),
        }
        for row, point in zip(merged.itertuples(index=False), centroids)
    ]

    geo = merged[["code_muni", "name_muni", "abbrev_state", "geometry"]].rename(
        columns={"code_muni": "code", "name_muni": "name", "abbrev_state": "uf"}
    )
    geo["code"] = geo["code"].astype(str)

    city_cnae = (
        empresas.groupby(
            [
                "cd_municipio_ibge",
                "cnae_grupo",
                "cnae_grupo_nome",
                "cnae_divisao",
                "cnae_divisao_nome",
                "intensidade_grupo",
            ],
            dropna=False,
        )["nr_cnpj"]
        .nunique()
        .reset_index(name="n")
        .rename(
            columns={
                "cd_municipio_ibge": "code",
                "cnae_grupo": "group",
                "cnae_grupo_nome": "groupName",
                "cnae_divisao": "division",
                "cnae_divisao_nome": "divisionName",
                "intensidade_grupo": "tech",
            }
        )
    )
    city_cnae["code"] = city_cnae["code"].astype(str)

    ncm_options = (
        ncm[["ncm", "ncm_nome", "sh4", "sh4_nome", "produto"]]
        .drop_duplicates(subset=["ncm"])
        .sort_values("ncm")
    )
    ncm_map: dict[str, dict] = {}
    for code, part in ncm.groupby("ncm", sort=False):
        first = part.iloc[0]
        groups = []
        for row in part.itertuples(index=False):
            groups.append(
                {
                    "group": row.cnae_grupo,
                    "groupName": row.cnae_grupo_nome,
                    "division": row.cnae_divisao,
                    "divisionName": row.cnae_divisao_nome,
                    "tech": row.intensidade,
                }
            )
        ncm_map[str(code)] = {
            "ncm": str(code),
            "name": first.ncm_nome,
            "sh4": first.sh4,
            "sh4Name": first.sh4_nome,
            "product": first.produto,
            "groups": groups,
        }

    return {
        "summary": {
            "empresas": int(len(empresas)),
            "municipios": int(city["code_muni"].nunique()),
            "ncm": int(ncm["ncm"].nunique()),
            "ufs": UF_ORDER,
        },
        "geojson": json.loads(geo.to_json()),
        "points": points,
        "cityCnae": city_cnae.to_dict(orient="records"),
        "ncmOptions": [
            {
                "value": str(row.ncm),
                "label": f"{row.ncm} - {row.ncm_nome}",
                "sh4": str(row.sh4),
                "sh4Name": row.sh4_nome,
                "product": row.produto,
            }
            for row in ncm_options.itertuples(index=False)
        ],
        "ncmMap": ncm_map,
    }


def build_componentes_payload(empresas: pd.DataFrame, municipios: gpd.GeoDataFrame, mapped: pd.DataFrame) -> dict:
    city = (
        empresas.groupby(["cd_municipio_ibge", "sg_uf", "nm_municipio"], dropna=False)
        .agg(empresas=("nr_cnpj", "nunique"))
        .reset_index()
        .rename(columns={"cd_municipio_ibge": "code_muni"})
    )
    merged = municipios.merge(city, on="code_muni", how="inner")
    centroids = merged.to_crs(epsg=5880).geometry.centroid.to_crs(epsg=4326)
    points = [
        {
            "code": str(row.code_muni),
            "name": row.nm_municipio,
            "uf": row.sg_uf,
            "lat": round(point.y, 6),
            "lon": round(point.x, 6),
        }
        for row, point in zip(merged.itertuples(index=False), centroids)
    ]

    geo = merged[["code_muni", "name_muni", "abbrev_state", "geometry"]].rename(
        columns={"code_muni": "code", "name_muni": "name", "abbrev_state": "uf"}
    )
    geo["code"] = geo["code"].astype(str)
    geo["geometry"] = geo.geometry.simplify(0.01, preserve_topology=True)

    component_rows = []
    city_components = []
    component_mappings = []
    mapping_rows = []
    gt_ncms = set(mapped["ncm"].astype(str))
    mapped_ncms = set(mapped.loc[mapped["cnae_grupo"].ne(NA_VALUE), "ncm"].astype(str))

    city_cnae = (
        empresas.groupby(
            [
                "cd_municipio_ibge",
                "sg_uf",
                "nm_municipio",
                "cnae_grupo",
                "cnae_grupo_nome",
                "cnae_divisao",
                "cnae_divisao_nome",
            ],
            dropna=False,
        )
        .agg(
            n=("nr_cnpj", "nunique"),
            cnpjs=("nr_cnpj", "nunique"),
            patented=("tem_patente", "sum"),
            patents=("patentes_total", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "cd_municipio_ibge": "code",
                "cnae_grupo": "group",
                "cnae_grupo_nome": "groupName",
                "cnae_divisao": "division",
                "cnae_divisao_nome": "divisionName",
            }
        )
    )
    city_cnae["code"] = city_cnae["code"].astype(str)

    for component_id, part in mapped.groupby("component_id", sort=False):
        first = part.iloc[0]
        groups = sorted({str(value) for value in part["cnae_grupo"] if str(value) != NA_VALUE})
        ncm_rows = (
            part[["ncm", "ncm_descricao_gt", "ncm_nome"]]
            .drop_duplicates(subset=["ncm"])
            .sort_values("ncm")
        )
        ncm_company_counts = {}
        for ncm_code, ncm_part in part.groupby("ncm", sort=False):
            ncm_groups = sorted({str(value) for value in ncm_part["cnae_grupo"] if str(value) != NA_VALUE})
            if ncm_groups:
                ncm_company_counts[str(ncm_code)] = int(
                    empresas.loc[empresas["cnae_grupo"].isin(ncm_groups), "nr_cnpj"].nunique()
                )
            else:
                ncm_company_counts[str(ncm_code)] = 0
        cnae_rows = (
            part[["cnae_grupo", "cnae_grupo_nome", "cnae_divisao", "cnae_divisao_nome", "intensidade"]]
            .drop_duplicates(subset=["cnae_grupo"])
            .sort_values("cnae_grupo")
        )
        for row in (
            part[
                [
                    "ncm",
                    "ncm_descricao_gt",
                    "ncm_nome",
                    "cnae_grupo",
                    "cnae_grupo_nome",
                    "cnae_divisao",
                    "cnae_divisao_nome",
                    "intensidade",
                ]
            ]
            .drop_duplicates()
            .itertuples(index=False)
        ):
            if row.cnae_grupo == NA_VALUE:
                continue
            mapping_rows.append(
                {
                    "component": str(component_id),
                    "ncm": str(row.ncm),
                    "group": str(row.cnae_grupo),
                }
            )
            component_mappings.append(
                {
                    "component": str(component_id),
                    "ncm": str(row.ncm),
                    "ncmName": row.ncm_nome if row.ncm_nome != NA_VALUE else row.ncm_descricao_gt,
                    "group": str(row.cnae_grupo),
                    "groupName": row.cnae_grupo_nome,
                    "division": row.cnae_divisao,
                    "divisionName": row.cnae_divisao_nome,
                    "tech": row.intensidade,
                }
            )
        if groups:
            companies = empresas[empresas["cnae_grupo"].isin(groups)].drop_duplicates(subset=["nr_cnpj"]).copy()
        else:
            companies = empresas.iloc[0:0].copy()

        if not companies.empty:
            city_stats = (
                companies.groupby(["cd_municipio_ibge", "sg_uf", "nm_municipio"], dropna=False)
                .agg(
                    empresas=("nr_cnpj", "nunique"),
                    cnpjs=("nr_cnpj", "nunique"),
                    comPatente=("tem_patente", "sum"),
                    patentes=("patentes_total", "sum"),
                )
                .reset_index()
            )
            for row in city_stats.itertuples(index=False):
                city_components.append(
                    {
                        "code": str(row.cd_municipio_ibge),
                        "component": str(component_id),
                        "n": int(row.empresas),
                        "cnpjs": int(row.cnpjs),
                        "patented": int(row.comPatente),
                        "patents": int(row.patentes),
                    }
                )
            ipc_values = companies["ipc_top"].explode().dropna().astype(str)
            top_ipc = [
                {"code": code, "count": int(count)}
                for code, count in ipc_values[ipc_values.ne("")].value_counts().head(8).items()
            ]
        else:
            top_ipc = []

        component_rows.append(
            {
                "id": str(component_id),
                "gt": first["gt"],
                "subsystem": first["subsistema"],
                "name": first["componente"],
                "ncmCount": int(ncm_rows["ncm"].nunique()),
                "cnaeCount": int(len(groups)),
                "companies": int(len(companies)),
                "patentedCompanies": int(companies["tem_patente"].sum()) if not companies.empty else 0,
                "patents": int(companies["patentes_total"].sum()) if not companies.empty else 0,
                "topIpc": top_ipc,
                "ncms": [
                    {
                        "code": str(row.ncm),
                        "gtDescription": row.ncm_descricao_gt,
                        "name": row.ncm_nome if row.ncm_nome != NA_VALUE else row.ncm_descricao_gt,
                        "companies": ncm_company_counts.get(str(row.ncm), 0),
                    }
                    for row in ncm_rows.itertuples(index=False)
                ],
                "cnaes": [
                    {
                        "group": row.cnae_grupo,
                        "groupName": row.cnae_grupo_nome,
                        "division": row.cnae_divisao,
                        "divisionName": row.cnae_divisao_nome,
                        "tech": row.intensidade,
                    }
                    for row in cnae_rows.itertuples(index=False)
                    if row.cnae_grupo != NA_VALUE
                ],
            }
        )

    component_rows = sorted(component_rows, key=lambda item: (-item["companies"], item["gt"], item["subsystem"], item["name"]))
    chain_node_keys: set[str] = set()
    chain_nodes: list[dict] = []
    chain_links: list[dict] = []
    subsystem_to_gt: dict[str, str] = {}
    for item in component_rows:
        gt_id = f"gt::{item['gt']}"
        subsystem_id = f"subsystem::{item['gt']}::{item['subsystem']}"
        component_id = f"component::{item['id']}"
        for node_id, name, depth, kind in [
            (gt_id, item["gt"], 2, "gt"),
            (subsystem_id, item["subsystem"], 1, "subsystem"),
            (component_id, item["name"], 0, "component"),
        ]:
            if node_id not in chain_node_keys:
                chain_node_keys.add(node_id)
                chain_nodes.append({"id": node_id, "name": name, "depth": depth, "kind": kind})
        chain_links.append(
            {
                "source": component_id,
                "target": subsystem_id,
                "component": item["id"],
                "subsystem": item["subsystem"],
                "gt": item["gt"],
                "value": int(item["companies"]),
            }
        )
        subsystem_to_gt[subsystem_id] = item["gt"]
    for subsystem_id, gt in subsystem_to_gt.items():
        subsystem_name = subsystem_id.split("::", 2)[2]
        value = int(
            sum(
                item["companies"]
                for item in component_rows
                if item["gt"] == gt and item["subsystem"] == subsystem_name
            )
        )
        chain_links.append(
            {
                "source": subsystem_id,
                "target": f"gt::{gt}",
                "subsystem": subsystem_name,
                "gt": gt,
                "value": value,
            }
        )
    mapping_df = pd.DataFrame(mapping_rows)
    if mapping_df.empty:
        city_component_ncm: list[dict] = []
        city_component_ncm_filter: list[dict] = []
    else:
        city_component_ncm_df = (
            city_cnae.merge(mapping_df, on="group", how="inner")
            .groupby(["code", "component", "ncm"], dropna=False)
            .agg(
                n=("n", "sum"),
                cnpjs=("cnpjs", "sum"),
                patented=("patented", "sum"),
                patents=("patents", "sum"),
            )
            .reset_index()
        )
        city_component_ncm = [
            {
                "code": str(row.code),
                "component": str(row.component),
                "ncm": str(row.ncm),
                "n": int(row.n),
                "cnpjs": int(row.cnpjs),
                "patented": int(row.patented),
                "patents": int(row.patents),
            }
            for row in city_component_ncm_df.itertuples(index=False)
        ]
        city_cnae_filter = (
            empresas.groupby(
                [
                    "cd_municipio_ibge",
                    "cnae_grupo",
                    "ds_faixa_faturamento_grupo",
                ],
                dropna=False,
            )
            .agg(
                n=("nr_cnpj", "nunique"),
                cnpjs=("nr_cnpj", "nunique"),
                patented=("tem_patente", "sum"),
                patents=("patentes_total", "sum"),
            )
            .reset_index()
            .rename(
                columns={
                    "cd_municipio_ibge": "code",
                    "cnae_grupo": "group",
                    "ds_faixa_faturamento_grupo": "revenue",
                }
            )
        )
        city_cnae_filter["code"] = city_cnae_filter["code"].astype(str)
        city_component_ncm_filter_df = (
            city_cnae_filter.merge(mapping_df, on="group", how="inner")
            .groupby(["code", "component", "ncm", "group", "revenue"], dropna=False)
            .agg(
                n=("n", "sum"),
                cnpjs=("cnpjs", "sum"),
                patented=("patented", "sum"),
                patents=("patents", "sum"),
            )
            .reset_index()
        )
        city_component_ncm_filter = [
            {
                "code": str(row.code),
                "component": str(row.component),
                "ncm": str(row.ncm),
                "group": str(row.group),
                "revenue": str(row.revenue),
                "n": int(row.n),
                "cnpjs": int(row.cnpjs),
                "patented": int(row.patented),
                "patents": int(row.patents),
            }
            for row in city_component_ncm_filter_df.itertuples(index=False)
        ]
    revenue_options = option_list(empresas, "ds_faixa_faturamento_grupo")
    group_options = option_list(empresas, "cnae_grupo", "cnae_grupo_nome")
    division_options = option_list(empresas, "cnae_divisao", "cnae_divisao_nome")
    return {
        "summary": {
            "components": len(component_rows),
            "ncmGt": len(gt_ncms),
            "ncmMapped": len(mapped_ncms),
            "ncmCoverage": round((len(mapped_ncms) / len(gt_ncms) * 100), 1) if gt_ncms else 0,
            "empresas": int(len(empresas)),
            "cnpjs": int(empresas["nr_cnpj"].nunique()),
            "patentedCompanies": int(empresas["tem_patente"].sum()),
            "patents": int(empresas["patentes_total"].sum()),
            "ufs": UF_ORDER,
        },
        "geojson": json.loads(geo.to_json()),
        "points": points,
        "components": component_rows,
        "chainNodes": chain_nodes,
        "chainLinks": chain_links,
        "cityComponents": city_components,
        "cityComponentNcm": city_component_ncm,
        "cityComponentNcmFilter": city_component_ncm_filter,
        "cityCnae": city_cnae.to_dict(orient="records"),
        "componentMappings": component_mappings,
        "options": {
            "uf": [{"value": uf, "label": uf, "count": int((empresas["sg_uf"] == uf).sum())} for uf in UF_ORDER],
            "revenue": revenue_options,
            "group": group_options,
            "division": division_options,
        },
    }


def write_ncm_html(payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NCM x Empresas</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root {{
      --bg:#06111f; --panel:#0b1728; --ink:#e8f3ff; --muted:#8aa7c4;
      --line:#1d3958; --blue:#3ba4ff; --cyan:#63d7ff;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .app {{ min-height:100vh; display:grid; grid-template-columns:minmax(380px,480px) 1fr; }}
    aside {{ max-height:100vh; overflow:auto; padding:24px; background:linear-gradient(180deg,#0b1728,#071321); border-right:1px solid var(--line); }}
    h1 {{ margin:0 0 8px; font-size:27px; line-height:1.08; }}
    .sub {{ color:var(--muted); font-size:14px; line-height:1.45; margin-bottom:18px; }}
    .stats {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px; }}
    .stat {{ border:1px solid var(--line); border-radius:8px; padding:12px; background:rgba(16,42,70,.72); }}
    .stat strong {{ display:block; color:var(--cyan); font-size:22px; line-height:1; }}
    .stat span {{ display:block; margin-top:6px; color:var(--muted); font-size:12px; }}
    .panel {{ border:1px solid var(--line); border-radius:8px; padding:14px; background:rgba(7,19,33,.72); margin-bottom:14px; }}
    label {{ display:block; margin:0 0 6px; color:#aac9e9; font-size:11px; font-weight:800; text-transform:uppercase; }}
    input, select {{ width:100%; border:1px solid #24486c; border-radius:7px; padding:10px 11px; background:#09182a; color:var(--ink); font:inherit; outline:none; }}
    select[multiple] {{ min-height:240px; padding:8px; }}
    select[multiple] option {{ padding:6px 7px; border-radius:5px; }}
    .actions a, button {{ border:1px solid #2b6ea3; border-radius:7px; padding:8px 10px; background:rgba(59,164,255,.12); color:var(--ink); font:inherit; font-size:12px; cursor:pointer; text-decoration:none; }}
    .actions a:hover, button:hover {{ background:rgba(59,164,255,.22); }}
    .actions {{ display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; margin-bottom:10px; }}
    .details {{ border:1px solid var(--line); border-radius:8px; padding:13px; background:rgba(16,42,70,.42); margin-top:14px; }}
    .details h2, .ranking h2 {{ margin:0 0 9px; font-size:15px; }}
    .details p {{ margin:5px 0; color:var(--muted); font-size:13px; line-height:1.35; }}
    .ranking {{ border-top:1px solid var(--line); padding-top:14px; margin-top:16px; }}
    .rank-row {{ display:grid; grid-template-columns:1fr auto; gap:12px; padding:9px 0; border-bottom:1px solid rgba(138,167,196,.16); font-size:13px; }}
    .rank-row small {{ display:block; color:var(--muted); margin-top:2px; }}
    .rank-row strong {{ color:var(--cyan); }}
    #map {{ width:100%; min-height:100vh; background:#06111f; }}
    .leaflet-tile-pane {{ filter:brightness(.58) saturate(.75) hue-rotate(175deg) contrast(1.15); }}
    .leaflet-popup-content-wrapper,.leaflet-popup-tip {{ background:#071321; color:var(--ink); border:1px solid var(--line); }}
    .hover-tooltip {{ background:rgba(7,19,33,.96); color:var(--ink); border:1px solid #2b6ea3; border-radius:8px; box-shadow:0 12px 32px rgba(0,0,0,.38); padding:0; max-width:430px; }}
    .hover-tooltip::before {{ display:none; }}
    .tip-box {{ min-width:310px; max-width:420px; }}
    .tip-title {{ padding:10px 12px 8px; border-bottom:1px solid rgba(138,167,196,.2); font-weight:800; font-size:13px; }}
    .tip-title small {{ display:block; color:var(--muted); margin-top:3px; font-weight:500; }}
    .tip-row {{ display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; padding:7px 12px; border-bottom:1px solid rgba(138,167,196,.12); font-size:12px; line-height:1.25; }}
    .tip-row span {{ overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#cfe8ff; }}
    .tip-row strong {{ color:var(--cyan); }}
    @media (max-width:920px) {{ .app {{ grid-template-columns:1fr; }} aside {{ max-height:none; border-right:0; }} #map {{ min-height:72vh; }} }}
  </style>
</head>
<body>
  <main class="app">
    <aside>
      <h1>NCM x Empresas</h1>
      <div class="sub">Selecione um ou mais códigos NCM para ver, no mapa, os municípios com empresas em CNAE grupo/divisão relacionados ao produto.</div>
      <section class="stats">
        <div class="stat"><strong id="statEmpresas">0</strong><span>CNPJs relacionados</span></div>
        <div class="stat"><strong id="statMunicipios">0</strong><span>municípios ativos</span></div>
        <div class="stat"><strong>{payload["summary"]["ncm"]}</strong><span>NCMs na relação</span></div>
        <div class="stat"><strong>{", ".join(payload["summary"]["ufs"])}</strong><span>UFs na análise</span></div>
      </section>
      <div class="actions"><a href="index.html">Empresas</a><a href="componentes.html">Componentes</a><button id="clearNcm" type="button">Limpar NCM</button></div>
      <section class="panel">
        <label for="ncmSearch">Procurar NCM</label>
        <input id="ncmSearch" type="search" placeholder="Digite código, produto ou descrição" />
        <label for="ncmSelect" style="margin-top:10px">Código NCM</label>
        <select id="ncmSelect" multiple size="12"></select>
      </section>
      <section class="details">
        <h2>NCM selecionado</h2>
        <div id="ncmDetails"><p>Selecione um ou mais NCMs para ver as relações com CNAE.</p></div>
      </section>
      <section class="ranking">
        <h2>Municípios em destaque</h2>
        <div id="ranking"></div>
      </section>
    </aside>
    <div id="map"></div>
  </main>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const DATA = {data};
    const br = new Intl.NumberFormat('pt-BR');
    const map = L.map('map', {{ zoomControl:false }}).setView([-27.6,-51.2],6);
    L.control.zoom({{ position:'bottomright' }}).addTo(map);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom:18, attribution:'&copy; OpenStreetMap' }}).addTo(map);
    const ncmSelect = document.getElementById('ncmSelect');
    const ncmSearch = document.getElementById('ncmSearch');
    const clearNcm = document.getElementById('clearNcm');
    const ncmDetails = document.getElementById('ncmDetails');
    const ranking = document.getElementById('ranking');
    const statEmpresas = document.getElementById('statEmpresas');
    const statMunicipios = document.getElementById('statMunicipios');
    DATA.pointsByCode = Object.fromEntries(DATA.points.map(p => [p.code,p]));

    function escapeHtml(value) {{
      return String(value).replace(/[&<>"']/g, s => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[s]));
    }}
    function normalizeText(value) {{
      return String(value).normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').toLowerCase();
    }}
    ncmSelect.innerHTML = DATA.ncmOptions.map(d => `<option value="${{escapeHtml(d.value)}}" title="${{escapeHtml(d.label)}}">${{escapeHtml(d.label)}}</option>`).join('');
    function selectedNcms() {{
      return Array.from(ncmSelect.selectedOptions).map(option => option.value);
    }}
    function selectedGroups() {{
      const groups = new Map();
      for (const ncm of selectedNcms()) {{
        for (const item of DATA.ncmMap[ncm]?.groups || []) groups.set(item.group, item);
      }}
      return groups;
    }}
    function colorScale(n) {{
      if (n >= 20000) return '#7dd3fc';
      if (n >= 10000) return '#38bdf8';
      if (n >= 5000) return '#0ea5e9';
      if (n >= 1000) return '#2563eb';
      if (n > 0) return '#1d4ed8';
      return '#172338';
    }}
    function radiusScale(n) {{ return Math.max(4, Math.min(30, 3 + Math.sqrt(n) / 8)); }}
    function countsByCity() {{
      const groups = selectedGroups();
      const counts = {{}};
      if (!groups.size) return counts;
      for (const row of DATA.cityCnae) {{
        if (groups.has(row.group)) counts[row.code] = (counts[row.code] || 0) + row.n;
      }}
      return counts;
    }}
    function cnaeBreakdown(code) {{
      const groups = selectedGroups();
      const rows = DATA.cityCnae
        .filter(row => row.code === code && groups.has(row.group))
        .sort((a,b) => b.n - a.n)
        .slice(0, 12);
      return rows;
    }}
    function tooltipHtml(point, total) {{
      const rows = cnaeBreakdown(point.code);
      const body = rows.length
        ? rows.map(row => `<div class="tip-row" title="${{escapeHtml(row.group + ' - ' + row.groupName)}}"><span>${{escapeHtml(row.division)}} / ${{escapeHtml(row.group)}} - ${{escapeHtml(row.groupName)}}</span><strong>${{br.format(row.n)}}</strong></div>`).join('')
        : '<div class="tip-row"><span>Nenhum CNAE relacionado no filtro</span><strong>0</strong></div>';
      return `<div class="tip-box"><div class="tip-title">${{escapeHtml(point.name)}} - ${{point.uf}}<small>${{br.format(total)}} CNPJs relacionados</small></div>${{body}}</div>`;
    }}
    let currentCounts = {{}};
    const polygonLayer = L.geoJSON(DATA.geojson, {{
      style: feature => ({{ color:'#25415f', weight:.35, opacity:.45, fillColor:'#172338', fillOpacity:.08 }}),
      onEachFeature: (feature, layer) => {{
        layer.on('mouseover', event => {{
          const p = feature.properties;
          const point = DATA.pointsByCode[String(p.code)] || {{ code:String(p.code), name:p.name, uf:p.uf }};
          layer.bindTooltip(tooltipHtml(point, currentCounts[String(p.code)] || 0), {{ sticky:true, direction:'top', opacity:1, className:'hover-tooltip' }}).openTooltip(event.latlng);
        }});
      }}
    }}).addTo(map);
    const pointLayer = L.layerGroup().addTo(map);

    function updateDetails() {{
      const ncms = selectedNcms();
      if (!ncms.length) {{
        ncmDetails.innerHTML = '<p>Selecione um ou mais NCMs para ver as relações com CNAE.</p>';
        return;
      }}
      const groups = selectedGroups();
      const ncmText = ncms.slice(0, 8).map(code => `<p><strong>${{escapeHtml(code)}}:</strong> ${{escapeHtml(DATA.ncmMap[code]?.name || '')}}</p>`).join('');
      ncmDetails.innerHTML = `${{ncmText}}<p><strong>${{groups.size}}</strong> grupos CNAE relacionados.</p>`;
    }}
    function update() {{
      currentCounts = countsByCity();
      pointLayer.clearLayers();
      polygonLayer.setStyle(feature => {{
        const n = currentCounts[String(feature.properties.code)] || 0;
        return {{ color:n ? '#48a7df' : '#25415f', weight:n ? .8 : .25, opacity:.62, fillColor:colorScale(n), fillOpacity:n ? .62 : .05 }};
      }});
      const rows = DATA.points.map(p => ({{...p, filtered:currentCounts[p.code] || 0}})).filter(p => p.filtered > 0).sort((a,b) => b.filtered - a.filtered);
      let total = 0;
      rows.forEach(p => {{
        total += p.filtered;
        L.circleMarker([p.lat,p.lon], {{ radius:radiusScale(p.filtered), color:'#dff6ff', weight:1, fillColor:colorScale(p.filtered), fillOpacity:.86 }})
          .bindTooltip(tooltipHtml(p, p.filtered), {{ sticky:true, direction:'top', opacity:1, className:'hover-tooltip' }})
          .bindPopup(`<strong>${{escapeHtml(p.name)}} - ${{p.uf}}</strong><br>${{br.format(p.filtered)}} CNPJs relacionados`)
          .addTo(pointLayer);
      }});
      statEmpresas.textContent = br.format(total);
      statMunicipios.textContent = br.format(rows.length);
      ranking.innerHTML = rows.slice(0,12).map(p => `<div class="rank-row"><div>${{escapeHtml(p.name)}}<small>${{p.uf}}</small></div><strong>${{br.format(p.filtered)}}</strong></div>`).join('') || '<div class="sub">Selecione NCM para ativar o mapa.</div>';
      updateDetails();
    }}
    ncmSearch.addEventListener('input', () => {{
      const term = normalizeText(ncmSearch.value.trim());
      Array.from(ncmSelect.options).forEach(option => {{
        const text = normalizeText(`${{option.textContent}} ${{option.value}}`);
        option.hidden = term ? !text.includes(term) : false;
      }});
    }});
    ncmSelect.addEventListener('input', update);
    clearNcm.addEventListener('click', () => {{
      Array.from(ncmSelect.options).forEach(option => {{ option.selected = false; option.hidden = false; }});
      ncmSearch.value = '';
      update();
    }});
    update();
  </script>
</body>
</html>"""
    OUTPUT_NCM_HTML.write_text(html, encoding="utf-8")


def write_componentes_html(payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Cadeia de Componentes</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root {{ --bg:#06111f; --panel:#0b1728; --ink:#e8f3ff; --muted:#8aa7c4; --line:#1d3958; --blue:#3ba4ff; --green:#5ee48a; --cyan:#63d7ff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .app {{ min-height:100vh; display:grid; grid-template-columns:minmax(390px,500px) 1fr; }}
    aside {{ max-height:100vh; overflow:auto; padding:24px; background:linear-gradient(180deg,#0b1728,#071321); border-right:1px solid var(--line); }}
    h1 {{ margin:0 0 8px; font-size:27px; line-height:1.08; }}
    h2 {{ margin:0 0 9px; font-size:15px; }}
    .sub {{ color:var(--muted); font-size:14px; line-height:1.45; margin-bottom:18px; }}
    .nav {{ display:flex; gap:8px; flex-wrap:wrap; margin:0 0 14px; }}
    .nav a, button {{ border:1px solid #2b6ea3; border-radius:7px; padding:8px 10px; background:rgba(59,164,255,.12); color:var(--ink); font:inherit; font-size:12px; cursor:pointer; text-decoration:none; }}
    .nav a:hover, button:hover {{ background:rgba(59,164,255,.22); }}
    .stats {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px; }}
    .stat {{ border:1px solid var(--line); border-radius:8px; padding:12px; background:rgba(16,42,70,.72); }}
    .stat strong {{ display:block; color:var(--cyan); font-size:22px; line-height:1; }}
    .stat span {{ display:block; margin-top:6px; color:var(--muted); font-size:12px; }}
    .panel {{ border:1px solid var(--line); border-radius:8px; padding:14px; background:rgba(7,19,33,.72); margin-bottom:14px; }}
    label {{ display:block; margin:0 0 6px; color:#aac9e9; font-size:11px; font-weight:800; text-transform:uppercase; }}
    input, select {{ width:100%; border:1px solid #24486c; border-radius:7px; padding:10px 11px; background:#09182a; color:var(--ink); font:inherit; outline:none; }}
    select {{ min-height:220px; padding:8px; }}
    option {{ padding:6px 7px; border-radius:5px; }}
    .toggle {{ display:flex; align-items:center; gap:8px; margin-top:10px; color:var(--muted); font-size:13px; }}
    .toggle input {{ width:auto; }}
    .details, .companies, .ranking {{ border:1px solid var(--line); border-radius:8px; padding:13px; background:rgba(16,42,70,.42); margin-top:14px; }}
    .details p {{ margin:5px 0; color:var(--muted); font-size:13px; line-height:1.35; }}
    .chips {{ display:flex; flex-wrap:wrap; gap:6px; margin-top:9px; }}
    .chip {{ border:1px solid rgba(99,215,255,.32); border-radius:999px; padding:4px 7px; color:#cfe8ff; font-size:12px; }}
    .company-row, .rank-row {{ display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; padding:9px 0; border-bottom:1px solid rgba(138,167,196,.16); font-size:13px; }}
    .company-row small, .rank-row small {{ display:block; color:var(--muted); margin-top:2px; }}
    .company-row strong, .rank-row strong {{ color:var(--cyan); }}
    .company-row.patented strong:last-child {{ color:var(--green); }}
    #map {{ width:100%; min-height:100vh; background:#06111f; }}
    .leaflet-tile-pane {{ filter:brightness(.58) saturate(.75) hue-rotate(175deg) contrast(1.15); }}
    .leaflet-popup-content-wrapper,.leaflet-popup-tip {{ background:#071321; color:var(--ink); border:1px solid var(--line); }}
    @media (max-width:920px) {{ .app {{ grid-template-columns:1fr; }} aside {{ max-height:none; border-right:0; }} #map {{ min-height:72vh; }} }}
  </style>
</head>
<body>
  <main class="app">
    <aside>
      <h1>Cadeia de Componentes</h1>
      <div class="sub">CNPJs relacionados a componentes de eletromobilidade, com maturidade tecnológica aproximada por depósitos INPI vinculados ao CNPJ.</div>
      <nav class="nav"><a href="index.html">Empresas</a></nav>
      <section class="stats">
        <div class="stat"><strong id="statEmpresas">0</strong><span>CNPJs relacionados</span></div>
        <div class="stat"><strong id="statPatentes">0</strong><span>CNPJs com patentes</span></div>
        <div class="stat"><strong>{payload["summary"]["components"]}</strong><span>componentes</span></div>
        <div class="stat"><strong>{payload["summary"]["ncmCoverage"]}%</strong><span>cobertura NCM</span></div>
      </section>
      <section class="panel">
        <label for="componentSearch">Procurar componente</label>
        <input id="componentSearch" type="search" placeholder="Digite bateria, motor, inversor, NCM ou subsistema" />
        <label for="componentSelect" style="margin-top:10px">Componente</label>
        <select id="componentSelect" size="10"></select>
        <label class="toggle"><input id="patentOnly" type="checkbox" /> Mostrar CNPJs com patentes na lista</label>
      </section>
      <section class="details">
        <h2>Componente selecionado</h2>
        <div id="componentDetails"><p>Selecione um componente para ver NCMs, CNAEs e indicadores.</p></div>
      </section>
      <section class="companies">
        <h2>Empresas</h2>
        <div id="companyList"><p class="sub">Selecione um componente para listar CNPJs relacionados.</p></div>
      </section>
      <section class="ranking">
        <h2>MunicÃ­pios em destaque</h2>
        <div id="ranking"></div>
      </section>
    </aside>
    <div id="map"></div>
  </main>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const DATA = {data};
    const br = new Intl.NumberFormat('pt-BR');
    const map = L.map('map', {{ zoomControl:false }}).setView([-27.6,-51.2],6);
    L.control.zoom({{ position:'bottomright' }}).addTo(map);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom:18, attribution:'&copy; OpenStreetMap' }}).addTo(map);
    const componentSelect = document.getElementById('componentSelect');
    const componentSearch = document.getElementById('componentSearch');
    const patentOnly = document.getElementById('patentOnly');
    const componentDetails = document.getElementById('componentDetails');
    const companyList = document.getElementById('companyList');
    const ranking = document.getElementById('ranking');
    const statEmpresas = document.getElementById('statEmpresas');
    const statPatentes = document.getElementById('statPatentes');
    const componentsById = Object.fromEntries(DATA.components.map(item => [item.id, item]));
    const pointsByCode = Object.fromEntries(DATA.points.map(point => [point.code, point]));
    const detailCache = {{}};

    function escapeHtml(value) {{
      return String(value ?? '').replace(/[&<>"']/g, char => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[char]));
    }}
    function normalizeText(value) {{
      return String(value || '').toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '');
    }}
    function fillComponents() {{
      componentSelect.innerHTML = DATA.components.map(item => `<option value="${{escapeHtml(item.id)}}" title="${{escapeHtml(item.name)}}">${{escapeHtml(item.name)}} · ${{escapeHtml(item.subsystem)}} (${{br.format(item.companies)}})</option>`).join('');
      if (componentSelect.options.length) componentSelect.options[0].selected = true;
    }}
    function selectedComponent() {{
      return componentsById[componentSelect.value] || DATA.components[0];
    }}
    function cityRows(componentId) {{
      return DATA.cityComponents.filter(row => row.component === componentId);
    }}
    function color(value, max) {{
      if (!value) return '#17304d';
      const t = Math.min(1, value / Math.max(1, max));
      return `rgb(${{28 + Math.round(26*t)}}, ${{104 + Math.round(112*t)}}, ${{100 + Math.round(55*t)}})`;
    }}
    const polygonLayer = L.geoJSON(DATA.geojson, {{
      style: () => ({{ color:'#24486c', weight:.7, fillColor:'#102a46', fillOpacity:.45 }}),
      onEachFeature: (feature, layer) => {{
        layer.on('click', () => {{
          const code = String(feature.properties.code);
          const row = currentByCity[code];
          const point = pointsByCode[code] || feature.properties;
          layer.bindPopup(`<strong>${{escapeHtml(point.name || feature.properties.name)}}</strong><br>${{row ? br.format(row.n) : 0}} empresas<br>${{row ? br.format(row.patented) : 0}} com patentes`).openPopup();
        }});
      }}
    }}).addTo(map);
    let currentByCity = {{}};
    function updateMap(rows) {{
      currentByCity = Object.fromEntries(rows.map(row => [row.code, row]));
      const max = Math.max(1, ...rows.map(row => row.n));
      polygonLayer.eachLayer(layer => {{
        const code = String(layer.feature.properties.code);
        const row = currentByCity[code];
        layer.setStyle({{ fillColor: color(row?.n || 0, max), fillOpacity: row ? .78 : .22, weight: row ? 1 : .5 }});
      }});
    }}
    function renderDetails(component) {{
      const ipc = component.topIpc.length ? component.topIpc.map(item => `<span class="chip">${{escapeHtml(item.code)}} · ${{br.format(item.count)}}</span>`).join('') : '<span class="chip">Sem IPC vinculado</span>';
      const ncms = component.ncms.slice(0, 8).map(item => `<span class="chip">${{escapeHtml(item.code)}}</span>`).join('');
      componentDetails.innerHTML = `
        <p><strong>${{escapeHtml(component.gt)}}</strong></p>
        <p>${{escapeHtml(component.subsystem)}}</p>
        <p>${{br.format(component.ncmCount)}} NCMs · ${{br.format(component.cnaeCount)}} grupos CNAE · ${{br.format(component.patents)}} depósitos INPI nas CNPJs relacionados.</p>
        <div class="chips">${{ncms}}</div>
        <div class="chips">${{ipc}}</div>
      `;
    }}
    async function loadCompanies(componentId) {{
      if (!detailCache[componentId]) {{
        const manifest = await fetch(`empresas_app_data/componentes/${{componentId}}/manifest.json`).then(response => response.json());
        const page = await fetch(`empresas_app_data/componentes/${{componentId}}/1.json`).then(response => response.json());
        detailCache[componentId] = {{ manifest, rows: page.rows.map(row => Object.fromEntries(manifest.columns.map((column, index) => [column, row[index]]))) }};
      }}
      return detailCache[componentId];
    }}
    async function renderCompanies(component) {{
      companyList.innerHTML = '<p class="sub">Carregando empresas...</p>';
      const payload = await loadCompanies(component.id);
      let rows = payload.rows;
      if (patentOnly.checked) rows = rows.filter(row => Number(row.patentes || 0) > 0);
      rows = rows.slice(0, 80);
      companyList.innerHTML = rows.length ? rows.map(row => `
        <div class="company-row ${{Number(row.patentes || 0) > 0 ? 'patented' : ''}}">
          <div><strong>${{escapeHtml(row.razao)}}</strong><small>${{escapeHtml(row.cnpj)}} · ${{escapeHtml(row.municipio)}} - ${{escapeHtml(row.uf)}}</small><small>CNAE ${{escapeHtml(row.grupo)}} · ${{escapeHtml(row.grupoNome)}}</small></div>
          <strong>${{br.format(Number(row.patentes || 0))}}</strong>
        </div>
      `).join('') : '<p class="sub">Nenhuma empresa para o filtro atual.</p>';
    }}
    function renderRanking(rows) {{
      ranking.innerHTML = rows.slice().sort((a,b) => b.n - a.n).slice(0, 10).map(row => {{
        const point = pointsByCode[row.code] || {{}};
        return `<div class="rank-row"><span>${{escapeHtml(point.name || row.code)}}<small>${{br.format(row.patented)}} CNPJs com patentes</small></span><strong>${{br.format(row.n)}}</strong></div>`;
      }}).join('') || '<p class="sub">Sem municÃ­pios relacionados.</p>';
    }}
    async function update() {{
      const component = selectedComponent();
      if (!component) return;
      const rows = cityRows(component.id);
      statEmpresas.textContent = br.format(component.companies);
      statPatentes.textContent = br.format(component.patentedCompanies);
      renderDetails(component);
      renderRanking(rows);
      updateMap(rows);
      await renderCompanies(component);
    }}
    componentSearch.addEventListener('input', () => {{
      const term = normalizeText(componentSearch.value);
      Array.from(componentSelect.options).forEach(option => {{
        const component = componentsById[option.value];
        option.hidden = term && !normalizeText(`${{component.name}} ${{component.subsystem}} ${{component.gt}} ${{component.ncms.map(item => item.code).join(' ')}}`).includes(term);
      }});
    }});
    componentSelect.addEventListener('input', update);
    patentOnly.addEventListener('change', update);
    fillComponents();
    update();
  </script>
</body>
</html>"""
    OUTPUT_COMPONENTES_HTML.write_text(html, encoding="utf-8")


def externalize_payload(html_path: Path, output_html: Path, payload_path: Path) -> None:
    html = html_path.read_text(encoding="utf-8")
    marker = "    const DATA = "
    start = html.index(marker)
    data_start = start + len(marker)
    data_end = html.index(";\n    const br =", data_start)
    payload = html[data_start:data_end]
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(payload, encoding="utf-8")

    rest_start = data_end + len(";\n    const br =")
    replacement = (
        "    let DATA = null;\n"
        "    async function initApp() {\n"
        f"      DATA = await fetch('{payload_path.relative_to(output_html.parent).as_posix()}?v=' + Date.now(), {{ cache: 'no-store' }}).then(response => response.json());\n"
        "    const br ="
    )
    html = html[:start] + replacement + html[rest_start:]
    html = html.replace(
        "  </script>\n</body>",
        "    }\n"
        "    initApp().catch(error => {\n"
        "      console.error(error);\n"
        "      document.body.innerHTML = '<main style=\"padding:24px;font-family:system-ui;color:#e8f3ff;background:#06111f;min-height:100vh\"><h1>Erro ao carregar dados</h1><p>Verifique se os arquivos em assets/data foram publicados junto com a aplicação.</p></main>';\n"
        "    });\n"
        "  </script>\n</body>",
        1,
    )
    output_html.write_text(html, encoding="utf-8")


def create_deploy_package() -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    (DIST_DIR / "assets" / "data").mkdir(parents=True)

    externalize_payload(
        OUTPUT_HTML,
        DIST_DIR / "index.html",
        DIST_DIR / "assets" / "data" / "empresas_payload.json",
    )
    externalize_payload(
        OUTPUT_NCM_HTML,
        DIST_DIR / "ncm.html",
        DIST_DIR / "assets" / "data" / "ncm_payload.json",
    )
    externalize_payload(
        OUTPUT_COMPONENTES_HTML,
        DIST_DIR / "componentes.html",
        DIST_DIR / "assets" / "data" / "componentes_payload.json",
    )

    shutil.copytree(ROOT / "empresas_app_data", DIST_DIR / "empresas_app_data")

    (DIST_DIR / "vercel.json").write_text(
        json.dumps(
            {
                "cleanUrls": True,
                "trailingSlash": False,
                "rewrites": [{"source": "/", "destination": "/index.html"}],
                "headers": [
                    {
                        "source": "/assets/data/(.*)",
                        "headers": [
                            {
                                "key": "Cache-Control",
                                "value": "public, max-age=3600",
                            }
                        ],
                    },
                    {
                        "source": "/empresas_app_data/(.*)",
                        "headers": [
                            {
                                "key": "Cache-Control",
                                "value": "public, max-age=3600",
                            }
                        ],
                    },
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (DIST_DIR / "README_DEPLOY.md").write_text(
        """# Deploy

Esta pasta e autocontida para deploy estatico.

## Teste local

```powershell
cd dist
python -m http.server 8000
```

Acesse:

```text
http://localhost:8000/
```

## Vercel

```powershell
vercel dist
vercel dist --prod
```

Arquivos principais:

- `index.html`: visualizacao principal Empresas com filtro NCM integrado.
- `ncm.html`: visualizacao auxiliar NCM x Empresas.
- `componentes.html`: visualizacao da cadeia de componentes e maturidade por patentes.
- `assets/data/*.json`: payloads agregados carregados pela aplicacao.
- `empresas_app_data/municipios/*.json`: detalhes por municipio carregados sob demanda.
- `empresas_app_data/componentes/*.json`: CNPJs relacionados a cada componente.
""",
        encoding="utf-8",
    )


def write_html(payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    ncm_label = f"{payload['summary']['ncm']:,}".replace(",", ".")
    ufs_label = ", ".join(payload["summary"]["ufs"])
    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Empresas - CNAE, NCM e CNPJ</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root {{
      --bg: #06111f;
      --panel: #0b1728;
      --panel-2: #0f2036;
      --panel-3: #102a46;
      --ink: #e8f3ff;
      --muted: #8aa7c4;
      --line: #1d3958;
      --blue: #3ba4ff;
      --cyan: #63d7ff;
      --deep: #071321;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--bg);
    }}
    .app {{
      min-height: 100vh;
      display: grid;
      grid-template-columns: minmax(360px, 460px) 1fr;
    }}
    aside {{
      background: linear-gradient(180deg, #0b1728 0%, #071321 100%);
      border-right: 1px solid var(--line);
      padding: 24px;
      overflow: auto;
      max-height: 100vh;
      z-index: 10;
      box-shadow: 18px 0 48px rgba(0,0,0,.28);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 27px;
      line-height: 1.08;
      letter-spacing: 0;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.45;
      margin-bottom: 20px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 20px;
    }}
    .stat {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: rgba(16, 42, 70, .72);
    }}
    .stat strong {{
      display: block;
      font-size: 22px;
      line-height: 1;
      color: var(--cyan);
    }}
    .stat span {{
      display: block;
      margin-top: 6px;
      font-size: 12px;
      color: var(--muted);
    }}
    .filters {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(7, 19, 33, .72);
    }}
    .top-actions {{
      margin-bottom: 12px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .top-actions a, button {{
      border: 1px solid #2b6ea3;
      border-radius: 7px;
      padding: 8px 10px;
      background: rgba(59, 164, 255, .12);
      color: var(--ink);
      font: inherit;
      font-size: 12px;
      cursor: pointer;
      text-decoration: none;
    }}
    .top-actions a:hover, button:hover {{
      background: rgba(59, 164, 255, .22);
      border-color: var(--blue);
    }}
    .filter-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 6px;
    }}
    .filter-head label {{
      margin: 0;
    }}
    .clear-filter {{
      padding: 4px 7px;
      color: #aac9e9;
      background: rgba(9, 24, 42, .82);
      font-size: 11px;
    }}
    label {{
      display: block;
      font-size: 11px;
      font-weight: 800;
      color: #aac9e9;
      margin: 0 0 6px;
      text-transform: uppercase;
    }}
    select, input {{
      width: 100%;
      border: 1px solid #24486c;
      border-radius: 7px;
      padding: 10px 11px;
      font: inherit;
      background: #09182a;
      color: var(--ink);
      outline: none;
    }}
    select[multiple] {{
      min-height: 112px;
      padding: 8px;
    }}
    select[multiple] option {{
      padding: 6px 7px;
      border-radius: 5px;
    }}
    .filter-search {{
      margin-bottom: 6px;
      padding-left: 34px;
      background:
        linear-gradient(transparent, transparent),
        #09182a;
    }}
    .search-wrap {{
      position: relative;
    }}
    .search-wrap::before {{
      content: "⌕";
      position: absolute;
      left: 11px;
      top: 50%;
      transform: translateY(-50%);
      color: var(--muted);
      font-size: 16px;
      pointer-events: none;
      z-index: 1;
    }}
    select:focus, input:focus {{
      border-color: var(--blue);
      box-shadow: 0 0 0 3px rgba(59,164,255,.16);
    }}
    .details {{
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 13px;
      background: rgba(16, 42, 70, .42);
      min-height: 92px;
    }}
    .details h2, .ranking h2 {{
      margin: 0 0 9px;
      font-size: 15px;
    }}
    .details p {{
      margin: 4px 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }}
    .ranking {{
      margin-top: 16px;
      border-top: 1px solid var(--line);
      padding-top: 16px;
    }}
    .ncm-company-panel {{
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(7, 19, 33, .72);
      overflow: hidden;
    }}
    .ncm-company-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 12px 13px;
      border-bottom: 1px solid rgba(138,167,196,.16);
    }}
    .ncm-company-head h2 {{
      margin: 0;
      font-size: 15px;
    }}
    .ncm-company-body {{
      max-height: 360px;
      overflow: auto;
    }}
    .ncm-company-row {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      padding: 9px 12px;
      border-bottom: 1px solid rgba(138,167,196,.12);
      font-size: 12px;
    }}
    .ncm-company-row strong {{
      display: block;
      color: #e8f3ff;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .ncm-company-row small {{
      display: block;
      color: var(--muted);
      margin-top: 3px;
      line-height: 1.3;
    }}
    .ncm-company-row em {{
      color: var(--cyan);
      font-style: normal;
      white-space: nowrap;
    }}
    .rank-row {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      padding: 9px 0;
      border-bottom: 1px solid rgba(138,167,196,.16);
      font-size: 13px;
    }}
    .rank-row small {{
      display: block;
      color: var(--muted);
      margin-top: 2px;
    }}
    .rank-row strong {{ color: var(--cyan); }}
    #map {{ width: 100%; min-height: 100vh; background: #06111f; }}
    .leaflet-tile-pane {{ filter: brightness(.58) saturate(.75) hue-rotate(175deg) contrast(1.15); }}
    .leaflet-popup-content-wrapper, .leaflet-popup-tip {{
      background: #071321;
      color: var(--ink);
      border: 1px solid var(--line);
    }}
    .popup-title {{ font-weight: 800; font-size: 15px; margin-bottom: 4px; }}
    .popup-meta {{ color: var(--muted); font-size: 12px; }}
    .hover-tooltip {{
      background: rgba(7, 19, 33, .96);
      color: var(--ink);
      border: 1px solid #2b6ea3;
      border-radius: 8px;
      box-shadow: 0 12px 32px rgba(0,0,0,.38);
      padding: 0;
      max-width: 390px;
    }}
    .hover-tooltip::before {{ display: none; }}
    .tip-box {{
      min-width: 280px;
      max-width: 380px;
    }}
    .tip-title {{
      padding: 10px 12px 8px;
      border-bottom: 1px solid rgba(138,167,196,.2);
      font-weight: 800;
      font-size: 13px;
    }}
    .tip-title small {{
      display: block;
      color: var(--muted);
      margin-top: 3px;
      font-weight: 500;
    }}
    .tip-row {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      padding: 7px 12px;
      border-bottom: 1px solid rgba(138,167,196,.12);
      font-size: 12px;
      line-height: 1.25;
    }}
    .tip-row:last-child {{ border-bottom: 0; }}
    .tip-row span {{
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #cfe8ff;
    }}
    .tip-row strong {{ color: var(--cyan); }}
    .legend {{
      background: rgba(7, 19, 33, .92);
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(0,0,0,.32);
      font-size: 12px;
      color: var(--muted);
    }}
    .legend b {{ color: var(--ink); }}
    .modal {{
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      background: rgba(2, 8, 18, .72);
      z-index: 10000;
    }}
    .modal.open {{ display: flex; }}
    .modal-panel {{
      width: min(1180px, 96vw);
      max-height: 88vh;
      display: flex;
      flex-direction: column;
      border: 1px solid #2b6ea3;
      border-radius: 8px;
      background: #071321;
      box-shadow: 0 24px 80px rgba(0,0,0,.56);
      overflow: hidden;
    }}
    .modal-head {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      padding: 16px;
      border-bottom: 1px solid var(--line);
      background: #0b1728;
    }}
    .modal-head h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .modal-head p {{
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .modal-tools {{
      display: grid;
      grid-template-columns: minmax(220px, 1fr) auto;
      gap: 10px;
      padding: 12px 16px;
      border-bottom: 1px solid var(--line);
      background: #081a2e;
    }}
    .table-wrap {{
      overflow: auto;
      min-height: 260px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    th, td {{
      padding: 9px 10px;
      border-bottom: 1px solid rgba(138,167,196,.16);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #0d2036;
      color: #cfe8ff;
      z-index: 1;
    }}
    td {{
      color: #d9ecff;
    }}
    .col-razao {{ min-width: 260px; }}
    .col-cnae {{ min-width: 240px; }}
    .modal-foot {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 16px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 13px;
      background: #0b1728;
    }}
    .pager {{
      display: flex;
      gap: 8px;
      align-items: center;
    }}
    @media (max-width: 920px) {{
      .app {{ grid-template-columns: 1fr; }}
      aside {{ max-height: none; border-right: 0; border-bottom: 1px solid var(--line); }}
      #map {{ min-height: 72vh; }}
      .modal {{ padding: 10px; }}
      .modal-tools {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="app">
    <aside>
      <h1>Empresas</h1>
      <div class="sub">Mapa municipal com filtros por CNAE, NCM, CNPJ e características das empresas.</div>
      <section class="stats">
        <div class="stat"><strong id="statEmpresas">0</strong><span>CNPJs no filtro</span></div>
        <div class="stat"><strong id="statMunicipios">0</strong><span>municípios ativos</span></div>
        <div class="stat"><strong>{ncm_label}</strong><span>NCMs na relação</span></div>
        <div class="stat"><strong>{ufs_label}</strong><span>UFs na análise</span></div>
      </section>

      <div class="top-actions">
        <a href="ncm.html">NCM x Empresas</a>
        <a href="componentes.html">Componentes</a>
        <button id="clearAll" type="button">Limpar todos os filtros</button>
      </div>

      <section class="filters">
        <div>
          <div class="filter-head"><label for="uf">UF</label><button class="clear-filter" data-target="uf" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="uf" type="search" placeholder="Procurar UF" /></div>
          <select id="uf" multiple size="4">
            <option value="all">PR, SC e RS</option>
            <option value="PR">Paraná</option>
            <option value="SC">Santa Catarina</option>
            <option value="RS">Rio Grande do Sul</option>
          </select>
        </div>
        <div>
          <div class="filter-head"><label for="ncm">Código NCM</label><button class="clear-filter" data-target="ncm" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="ncm" type="search" placeholder="Procurar código, produto ou descrição" /></div>
          <select id="ncm" multiple size="6"></select>
        </div>
        <div>
          <div class="filter-head"><label for="cnpj">CNPJ</label><button class="clear-filter" data-target="cnpj" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="cnpj" type="search" placeholder="Procurar CNPJ ou razão social" /></div>
          <select id="cnpj" multiple size="6"></select>
        </div>
        <div>
          <div class="filter-head"><label for="division">CNAE divisão</label><button class="clear-filter" data-target="division" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="division" type="search" placeholder="Procurar número ou nome" /></div>
          <select id="division" multiple size="6"></select>
        </div>
        <div>
          <div class="filter-head"><label for="group">CNAE grupo</label><button class="clear-filter" data-target="group" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="group" type="search" placeholder="Procurar número ou nome" /></div>
          <select id="group" multiple size="6"></select>
        </div>
        <div>
          <div class="filter-head"><label for="revenue">Faixa de faturamento</label><button class="clear-filter" data-target="revenue" type="button">Limpar</button></div>
          <div class="search-wrap"><input class="filter-search" data-target="revenue" type="search" placeholder="Procurar faixa" /></div>
          <select id="revenue" multiple size="6"></select>
        </div>
        <div>
          <label for="search">Buscar município</label>
          <input id="search" type="search" placeholder="Ex.: Joinville, Curitiba, Caxias" />
        </div>
      </section>

      <section class="details">
        <h2>Descrição CNAE</h2>
        <div id="cnaeDetails"><p>Selecione uma divisão ou grupo CNAE para ver a descrição.</p></div>
      </section>

      <section class="ncm-company-panel">
        <div class="ncm-company-head">
          <h2>CNPJs relacionados ao NCM</h2>
          <button id="refreshNcmCompanies" type="button">Atualizar</button>
        </div>
        <div id="ncmCompanyList" class="ncm-company-body">
          <div class="sub" style="padding:12px">Selecione um NCM para listar CNPJs relacionados.</div>
        </div>
      </section>

      <section class="ranking">
        <h2>Municípios em destaque</h2>
        <div id="ranking"></div>
      </section>
    </aside>
    <div id="map"></div>
  </main>

  <div id="companyModal" class="modal" aria-hidden="true">
    <section class="modal-panel">
      <div class="modal-head">
        <div>
          <h2 id="modalTitle">Empresas</h2>
          <p id="modalSubtitle"></p>
        </div>
        <button id="closeModal" type="button">Fechar</button>
      </div>
      <div class="modal-tools">
        <input id="companySearch" type="search" placeholder="Buscar razão social, CNPJ, CNAE ou faturamento" />
        <button id="clearCompanySearch" type="button">Limpar busca</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>CNPJ</th>
              <th class="col-razao">Razão social</th>
              <th>Faturamento</th>
              <th>Porte</th>
              <th>CNAE principal</th>
              <th>CNAE secundário</th>
              <th class="col-cnae">CNAE grupo</th>
              <th class="col-cnae">CNAE divisão</th>
              <th>Intensidade grupo</th>
              <th>Intensidade divisão</th>
            </tr>
          </thead>
          <tbody id="companyRows"></tbody>
        </table>
      </div>
      <div class="modal-foot">
        <span id="companyStatus"></span>
        <div class="pager">
          <button id="prevPage" type="button">Anterior</button>
          <span id="pageInfo"></span>
          <button id="nextPage" type="button">Próxima</button>
        </div>
      </div>
    </section>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const DATA = {data};
    const br = new Intl.NumberFormat('pt-BR');
    const map = L.map('map', {{ zoomControl: false }}).setView([-27.6, -51.2], 6);
    L.control.zoom({{ position: 'bottomright' }}).addTo(map);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap'
    }}).addTo(map);

    const els = {{
      uf: document.getElementById('uf'),
      revenue: document.getElementById('revenue'),
      ncm: document.getElementById('ncm'),
      cnpj: document.getElementById('cnpj'),
      group: document.getElementById('group'),
      division: document.getElementById('division'),
      search: document.getElementById('search')
    }};
    const statEmpresas = document.getElementById('statEmpresas');
    const statMunicipios = document.getElementById('statMunicipios');
    const ranking = document.getElementById('ranking');
    const cnaeDetails = document.getElementById('cnaeDetails');
    const ncmCompanyList = document.getElementById('ncmCompanyList');
    const refreshNcmCompanies = document.getElementById('refreshNcmCompanies');
    const filterSearches = document.querySelectorAll('.filter-search');
    const clearButtons = document.querySelectorAll('.clear-filter');
    const clearAllButton = document.getElementById('clearAll');
    const companyModal = document.getElementById('companyModal');
    const modalTitle = document.getElementById('modalTitle');
    const modalSubtitle = document.getElementById('modalSubtitle');
    const closeModal = document.getElementById('closeModal');
    const companySearch = document.getElementById('companySearch');
    const clearCompanySearch = document.getElementById('clearCompanySearch');
    const companyRows = document.getElementById('companyRows');
    const companyStatus = document.getElementById('companyStatus');
    const pageInfo = document.getElementById('pageInfo');
    const prevPage = document.getElementById('prevPage');
    const nextPage = document.getElementById('nextPage');
    const detailCache = {{}};
    const cnpjShardCache = {{}};
    let activeCompanies = [];
    let activeCompanyManifest = null;
    let activePoint = null;
    let currentPage = 1;

    function escapeHtml(value) {{
      return String(value).replace(/[&<>"']/g, s => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[s]));
    }}
    function fillSelect(el, options, allLabel, formatter) {{
      el.innerHTML = `<option value="all" title="${{escapeHtml(allLabel)}}">${{allLabel}}</option>` + options.map(d => {{
        const text = formatter ? formatter(d) : d.label;
        const label = d.count ? `${{text}} (${{br.format(d.count)}})` : text;
        return `<option value="${{escapeHtml(d.value)}}" title="${{escapeHtml(label)}}">${{escapeHtml(label)}}</option>`;
      }}).join('');
    }}
    fillSelect(els.revenue, DATA.options.revenue, 'Todas as faixas');
    fillSelect(els.ncm, DATA.options.ncm, 'Todos os NCMs', d => d.label);
    fillSelect(els.cnpj, DATA.options.cnpj, 'Todos os CNPJs', d => d.label);
    fillSelect(els.group, DATA.options.group, 'Todos os grupos', d => `${{d.value}} - ${{d.label}}`);
    fillSelect(els.division, DATA.options.division, 'Todas as divisões', d => `${{d.value}} - ${{d.label}}`);
    Object.values(els).forEach(el => {{
      if (el.tagName === 'SELECT') el.querySelector('option[value="all"]')?.setAttribute('selected', 'selected');
    }});
    DATA.cnpjIndex = {{}};
    Array.from(els.uf.options).forEach(option => {{
      option.title = option.textContent;
    }});

    function normalizeText(value) {{
      return String(value)
        .normalize('NFD')
        .replace(/[\\u0300-\\u036f]/g, '')
        .toLowerCase();
    }}
    function filterSelectOptions(input) {{
      const select = els[input.dataset.target];
      const term = normalizeText(input.value.trim());
      Array.from(select.options).forEach(option => {{
        const text = normalizeText(`${{option.textContent}} ${{option.value}}`);
        option.hidden = term && option.value !== 'all' ? !text.includes(term) : false;
      }});
    }}
    async function loadCnpjShard(prefix) {{
      if (cnpjShardCache[prefix]) return cnpjShardCache[prefix];
      const response = await fetch(`empresas_app_data/cnpj_index/${{prefix}}.json`);
      if (!response.ok) {{
        cnpjShardCache[prefix] = [];
        return cnpjShardCache[prefix];
      }}
      const payload = await response.json();
      cnpjShardCache[prefix] = payload.rows.map(row => Object.fromEntries(payload.columns.map((column, index) => [column, row[index]])));
      return cnpjShardCache[prefix];
    }}
    async function updateCnpjOptions(input) {{
      const raw = input.value.trim();
      const digits = raw.replace(/\\D/g, '');
      const term = normalizeText(raw);
      if (digits.length < 4 && term.length < 3) {{
        DATA.options.cnpj = [];
        fillSelect(els.cnpj, DATA.options.cnpj, 'Digite ao menos 4 dígitos do CNPJ');
        DATA.cnpjIndex = {{}};
        return;
      }}
      let items = [];
      if (digits.length >= 4) {{
        const prefixes = Array.from(new Set([digits.slice(0, 4), digits.padStart(14, '0').slice(0, 4)]));
        const chunks = await Promise.all(prefixes.map(loadCnpjShard));
        items = chunks.flat();
      }} else {{
        const manifest = await fetch('empresas_app_data/cnpj_index/manifest.json').then(response => response.json());
        const prefixes = Object.keys(manifest).slice(0, 20);
        const chunks = await Promise.all(prefixes.map(loadCnpjShard));
        items = chunks.flat();
      }}
      const filtered = items
        .filter(item => normalizeText(`${{item.value}} ${{item.name}}`).includes(term))
        .slice(0, 500);
      DATA.options.cnpj = filtered.map(item => ({{
        value: String(item.value),
        label: `${{item.value}} - ${{item.name}}`,
        code: item.code,
        group: item.group,
        division: item.division,
        revenue: item.revenue,
        count: 0
      }}));
      DATA.cnpjIndex = Object.fromEntries(DATA.options.cnpj.map(item => [String(item.value), item]));
      fillSelect(els.cnpj, DATA.options.cnpj, filtered.length ? 'Todos os CNPJs encontrados' : 'Nenhum CNPJ encontrado', d => d.label);
      resetSelect('cnpj');
    }}
    function resetSelect(key) {{
      const select = els[key];
      Array.from(select.options).forEach(option => {{
        option.selected = option.value === 'all';
        option.hidden = false;
      }});
      const search = document.querySelector(`.filter-search[data-target="${{key}}"]`);
      if (search) search.value = '';
    }}
    function resetAllFilters() {{
      ['uf', 'revenue', 'ncm', 'cnpj', 'group', 'division'].forEach(resetSelect);
      els.search.value = '';
      update();
    }}

    DATA.pointsByCode = Object.fromEntries(DATA.points.map(p => [p.code, p]));

    function colorScale(n) {{
      if (n >= 20000) return '#7dd3fc';
      if (n >= 10000) return '#38bdf8';
      if (n >= 5000) return '#0ea5e9';
      if (n >= 1000) return '#2563eb';
      if (n > 0) return '#1d4ed8';
      return '#172338';
    }}
    function radiusScale(n) {{
      return Math.max(4, Math.min(30, 3 + Math.sqrt(n) / 8));
    }}
    function valuesOf(el) {{
      const values = Array.from(el.selectedOptions).map(option => option.value);
      const specific = values.filter(value => value !== 'all');
      return specific.length ? specific : ['all'];
    }}
    function matchesSelection(value, selectedValues) {{
      return selectedValues.includes('all') || selectedValues.includes(value);
    }}
    function selectedNcmGroups(ncms) {{
      const groups = new Set();
      const divisions = new Set();
      const active = ncms.filter(value => value !== 'all');
      if (!active.length) return {{ active: false, groups, divisions }};
      for (const ncm of active) {{
        for (const item of DATA.ncmMap[ncm]?.groups || []) {{
          groups.add(item.group);
          divisions.add(item.division);
        }}
      }}
      return {{ active: true, groups, divisions }};
    }}
    function selectedCnpjs(cnpjs) {{
      const selected = new Set(cnpjs.filter(value => value !== 'all'));
      return {{ active: selected.size > 0, selected }};
    }}
    function selected() {{
      const ncm = valuesOf(els.ncm);
      const cnpj = valuesOf(els.cnpj);
      return {{
        revenue: valuesOf(els.revenue),
        ncm,
        ncmMatch: selectedNcmGroups(ncm),
        cnpj,
        cnpjMatch: selectedCnpjs(cnpj),
        group: valuesOf(els.group),
        division: valuesOf(els.division),
        uf: valuesOf(els.uf)
      }};
    }}
    function recordMatches(r, s) {{
      if (s.cnpjMatch.active) {{
        let match = false;
        for (const cnpj of s.cnpjMatch.selected) {{
          const company = DATA.cnpjIndex[cnpj];
          if (company && company.code === r.code && company.group === r.group && company.division === r.division && company.revenue === r.revenue) {{
            match = true;
            break;
          }}
        }}
        if (!match) return false;
      }}
      return matchesSelection(r.revenue, s.revenue)
        && (!s.ncmMatch.active || s.ncmMatch.groups.has(r.group))
        && matchesSelection(r.group, s.group)
        && matchesSelection(r.division, s.division);
    }}
    function companyMatches(company, s) {{
      return matchesSelection(company.faturamento, s.revenue)
        && (!s.ncmMatch.active || s.ncmMatch.groups.has(company.grupo))
        && (!s.cnpjMatch.active || s.cnpjMatch.selected.has(String(company.cnpj)))
        && matchesSelection(company.grupo, s.group)
        && matchesSelection(company.divisao, s.division);
    }}
    function revenueRank(value) {{
      const label = normalizeText(value);
      if (label.includes('nao informado') || label.includes('não informado')) return Number.POSITIVE_INFINITY;
      const firstNumber = label.match(/\\d+(?:[\\.,]\\d+)?/);
      if (!firstNumber) return Number.POSITIVE_INFINITY - 1;
      let amount = Number(firstNumber[0].replace(',', '.'));
      if (label.includes('k')) amount *= 1_000;
      if (label.includes('m')) amount *= 1_000_000;
      if (label.includes('b')) amount *= 1_000_000_000;
      return amount;
    }}
    function compareCompaniesByRevenue(a, b) {{
      const revenueDiff = revenueRank(a.faturamento) - revenueRank(b.faturamento);
      if (revenueDiff !== 0) return revenueDiff;
      return String(a.razao).localeCompare(String(b.razao), 'pt-BR');
    }}
    function filteredCompanies() {{
      const s = selected();
      const term = normalizeText(companySearch.value.trim());
      return activeCompanies
        .filter(company => companyMatches(company, s))
        .filter(company => {{
          if (!term) return true;
          const text = normalizeText([
            company.cnpj,
            company.razao,
            company.faturamento,
            company.porte,
            company.cnaePrincipal,
            company.cnaeSecundario,
            company.grupo,
            company.grupoNome,
            company.divisao,
            company.divisaoNome,
            company.techGroup,
            company.techDivision
          ].join(' '));
          return text.includes(term);
        }})
        .sort(compareCompaniesByRevenue);
    }}
    function renderCompanyTable() {{
      const rows = filteredCompanies();
      const pages = activeCompanyManifest?.pages || 1;
      companyRows.innerHTML = rows.map(company => `
        <tr>
          <td>${{escapeHtml(company.cnpj)}}</td>
          <td class="col-razao">${{escapeHtml(company.razao)}}</td>
          <td>${{escapeHtml(company.faturamento)}}</td>
          <td>${{escapeHtml(company.porte)}}</td>
          <td>${{escapeHtml(company.cnaePrincipal)}}</td>
          <td>${{escapeHtml(company.cnaeSecundario)}}</td>
          <td class="col-cnae">${{escapeHtml(company.grupo)}} - ${{escapeHtml(company.grupoNome)}}</td>
          <td class="col-cnae">${{escapeHtml(company.divisao)}} - ${{escapeHtml(company.divisaoNome)}}</td>
          <td>${{escapeHtml(company.techGroup)}}</td>
          <td>${{escapeHtml(company.techDivision)}}</td>
        </tr>
      `).join('') || '<tr><td colspan="10">Nenhuma empresa encontrada para os filtros atuais.</td></tr>';
      const totalRows = activeCompanyManifest?.rows || rows.length;
      companyStatus.textContent = `${{br.format(rows.length)}} empresas exibidas nesta página de ${{br.format(totalRows)}} no município`;
      pageInfo.textContent = `Página ${{currentPage}} de ${{pages}}`;
      prevPage.disabled = currentPage <= 1;
      nextPage.disabled = currentPage >= pages;
    }}
    async function loadCompanyManifest(code) {{
      const key = `${{code}}/manifest`;
      if (!detailCache[key]) {{
        const response = await fetch(`empresas_app_data/municipios/${{code}}/manifest.json`);
        if (!response.ok) throw new Error('Não foi possível carregar os detalhes do município.');
        detailCache[key] = await response.json();
      }}
      return detailCache[key];
    }}
    async function loadCompanyPage(code, page) {{
      const manifest = await loadCompanyManifest(code);
      const safePage = Math.min(Math.max(1, page), manifest.pages);
      const key = `${{code}}/${{safePage}}`;
      if (!detailCache[key]) {{
        const response = await fetch(`empresas_app_data/municipios/${{code}}/${{safePage}}.json`);
        if (!response.ok) throw new Error('Não foi possível carregar a página de empresas do município.');
        const payload = await response.json();
        detailCache[key] = payload.rows.map(row => Object.fromEntries(manifest.columns.map((column, index) => [column, row[index]])));
      }}
      activeCompanyManifest = manifest;
      currentPage = safePage;
      return detailCache[key];
    }}
    async function openCompanyTable(point) {{
      activePoint = point;
      modalTitle.textContent = `${{point.name}} - ${{point.uf}}`;
      modalSubtitle.textContent = 'Carregando empresas do município...';
      companyRows.innerHTML = '<tr><td colspan="10">Carregando...</td></tr>';
      companyStatus.textContent = '';
      companySearch.value = '';
      currentPage = 1;
      companyModal.classList.add('open');
      companyModal.setAttribute('aria-hidden', 'false');
      try {{
        activeCompanies = await loadCompanyPage(point.code, 1);
        modalSubtitle.textContent = 'Tabela paginada por município. A busca atua sobre a página carregada.';
        renderCompanyTable();
      }} catch (error) {{
        activeCompanies = [];
        modalSubtitle.textContent = 'Abra a aplicação com um servidor local para carregar esta tabela.';
        companyRows.innerHTML = `<tr><td colspan="10">${{escapeHtml(error.message)}} Use: python -m http.server 8000</td></tr>`;
      }}
    }}
    function closeCompanyTable() {{
      companyModal.classList.remove('open');
      companyModal.setAttribute('aria-hidden', 'true');
      activeCompanies = [];
      activeCompanyManifest = null;
      activePoint = null;
    }}
    function groupBreakdown(code) {{
      const s = selected();
      const byGroup = {{}};
      for (const r of DATA.records) {{
        if (r.code === code && recordMatches(r, s)) byGroup[r.group] = (byGroup[r.group] || 0) + r.n;
      }}
      const rows = Object.entries(byGroup)
        .map(([group, n]) => {{
          const label = DATA.labels.groups[group]?.cnae_grupo_nome || 'Sem descrição';
          return {{ group, label, n }};
        }})
        .sort((a, b) => b.n - a.n);
      const top = rows.slice(0, 10);
      const rest = rows.slice(10).reduce((sum, row) => sum + row.n, 0);
      if (rest > 0) top.push({{ group: 'Outros', label: `${{rows.length - 10}} grupos restantes`, n: rest }});
      return top;
    }}
    function tooltipHtml(point, total) {{
      const rows = groupBreakdown(point.code);
      const body = rows.length
        ? rows.map(row => `<div class="tip-row" title="${{escapeHtml(row.group + ' - ' + row.label)}}"><span>${{escapeHtml(row.group)}} - ${{escapeHtml(row.label)}}</span><strong>${{br.format(row.n)}}</strong></div>`).join('')
        : '<div class="tip-row"><span>Nenhum CNAE no filtro</span><strong>0</strong></div>';
      return `<div class="tip-box"><div class="tip-title">${{escapeHtml(point.name)}} - ${{point.uf}}<small>${{br.format(total)}} CNPJs no filtro</small></div>${{body}}</div>`;
    }}
    function countsByCity() {{
      const s = selected();
      const counts = {{}};
      if (s.cnpjMatch.active) {{
        for (const cnpj of s.cnpjMatch.selected) {{
          const company = DATA.cnpjIndex[cnpj];
          if (
            company
            && matchesSelection(company.revenue, s.revenue)
            && (!s.ncmMatch.active || s.ncmMatch.groups.has(company.group))
            && matchesSelection(company.group, s.group)
            && matchesSelection(company.division, s.division)
          ) {{
            counts[company.code] = (counts[company.code] || 0) + 1;
          }}
        }}
        return counts;
      }}
      for (const r of DATA.records) {{
        if (recordMatches(r, s)) counts[r.code] = (counts[r.code] || 0) + r.n;
      }}
      return counts;
    }}
    function updateDetails() {{
      const ncms = valuesOf(els.ncm).filter(value => value !== 'all');
      const groups = valuesOf(els.group).filter(value => value !== 'all');
      const divisions = valuesOf(els.division).filter(value => value !== 'all');
      const ncmInfo = ncms.length
        ? `<p><strong>NCM:</strong> ${{ncms.length === 1 ? escapeHtml(ncms[0] + ' - ' + (DATA.ncmMap[ncms[0]]?.name || '')) : `${{ncms.length}} NCMs selecionados`}}</p><p><strong>CNAE relacionados ao NCM:</strong> ${{selectedNcmGroups(ncms).groups.size}} grupos</p>`
        : '';
      if (groups.length === 1) {{
        const group = groups[0];
        const g = DATA.labels.groups[group] || {{}};
        cnaeDetails.innerHTML = `
          ${{ncmInfo}}
          <p><strong>Grupo ${{escapeHtml(group)}}:</strong> ${{escapeHtml(g.cnae_grupo_nome || 'Sem descrição')}}</p>
          <p><strong>Divisão ${{escapeHtml(g.cnae_divisao || '')}}:</strong> ${{escapeHtml(g.cnae_divisao_nome || 'Sem descrição')}}</p>`;
      }} else if (groups.length > 1) {{
        cnaeDetails.innerHTML = `${{ncmInfo}}<p>${{groups.length}} grupos CNAE selecionados.</p>`;
      }} else if (divisions.length === 1) {{
        const division = divisions[0];
        cnaeDetails.innerHTML = `${{ncmInfo}}<p><strong>Divisão ${{escapeHtml(division)}}:</strong> ${{escapeHtml(DATA.labels.divisions[division] || 'Sem descrição')}}</p>`;
      }} else if (divisions.length > 1) {{
        cnaeDetails.innerHTML = `${{ncmInfo}}<p>${{divisions.length}} divisões CNAE selecionadas.</p>`;
      }} else if (ncms.length) {{
        cnaeDetails.innerHTML = ncmInfo;
      }} else {{
        cnaeDetails.innerHTML = '<p>Selecione uma divisão ou grupo CNAE para ver a descrição.</p>';
      }}
    }}

    const polygonLayer = L.geoJSON(DATA.geojson, {{
      style: feature => {{
        const n = DATA.pointsByCode[String(feature.properties.code)]?.total || 0;
        return {{
          color: '#2b5c88',
          weight: n > 0 ? 0.75 : 0.3,
          opacity: 0.58,
          fillColor: colorScale(n),
          fillOpacity: n > 0 ? 0.50 : 0.10
        }};
      }},
      onEachFeature: (feature, layer) => {{
        layer.on('mouseover', event => {{
          const p = feature.properties;
          const point = DATA.pointsByCode[String(p.code)] || {{ code: String(p.code), name: p.name, uf: p.uf }};
          const n = currentCounts[String(p.code)] || 0;
          layer.bindTooltip(tooltipHtml(point, n), {{
            sticky: true,
            direction: 'top',
            opacity: 1,
            className: 'hover-tooltip'
          }}).openTooltip(event.latlng);
        }});
        layer.on('click', () => {{
          const p = feature.properties;
          const n = currentCounts[String(p.code)] || 0;
          layer.bindPopup(`<div class="popup-title">${{escapeHtml(p.name)}} - ${{p.uf}}</div><div class="popup-meta">${{br.format(n)}} CNPJs no filtro</div>`).openPopup();
        }});
      }}
    }}).addTo(map);

    const pointLayer = L.layerGroup().addTo(map);
    let currentCounts = {{}};

    function visibleRows() {{
      const term = els.search.value.trim().toLowerCase();
      const uf = valuesOf(els.uf);
      return DATA.points.map(p => ({{...p, filtered: currentCounts[p.code] || 0}}))
        .filter(p => matchesSelection(p.uf, uf) && (!term || p.name.toLowerCase().includes(term)))
        .filter(p => p.filtered > 0)
        .sort((a, b) => b.filtered - a.filtered);
    }}
    function renderNcmCompanyList(rows, loadingMessage = null) {{
      if (loadingMessage) {{
        ncmCompanyList.innerHTML = `<div class="sub" style="padding:12px">${{escapeHtml(loadingMessage)}}</div>`;
        return;
      }}
      ncmCompanyList.innerHTML = rows.map(company => `
        <div class="ncm-company-row" title="${{escapeHtml(company.razao)}} - ${{escapeHtml(company.municipio)}}">
          <div>
            <strong>${{escapeHtml(company.razao)}}</strong>
            <small>${{escapeHtml(company.cnpj)}} · ${{escapeHtml(company.municipio)}} - ${{escapeHtml(company.uf)}}</small>
            <small>CNAE ${{escapeHtml(company.grupo)}} - ${{escapeHtml(company.grupoNome)}} · Divisão ${{escapeHtml(company.divisao)}}</small>
          </div>
          <em>${{escapeHtml(company.faturamento)}}</em>
        </div>
      `).join('') || '<div class="sub" style="padding:12px">Nenhuma empresa encontrada para os filtros atuais.</div>';
    }}
    async function updateNcmCompanyList() {{
      const s = selected();
      if (!s.ncmMatch.active) {{
        renderNcmCompanyList([], 'Selecione um NCM para listar CNPJs relacionados.');
        return;
      }}
      const rows = visibleRows().slice(0, 8);
      if (!rows.length) {{
        renderNcmCompanyList([], null);
        return;
      }}
      renderNcmCompanyList([], 'Carregando CNPJs relacionados...');
      const companies = [];
      for (const point of rows) {{
        try {{
          const page = await loadCompanyPage(point.code, 1);
          page
            .filter(company => companyMatches(company, s))
            .forEach(company => companies.push({{...company, municipio: point.name, uf: point.uf}}));
        }} catch (error) {{
          console.warn(error);
        }}
      }}
      renderNcmCompanyList(companies.sort(compareCompaniesByRevenue).slice(0, 80));
    }}

    function update() {{
      currentCounts = countsByCity();
      pointLayer.clearLayers();
      const s = selected();
      polygonLayer.setStyle(feature => {{
        const p = feature.properties;
        const ufOk = matchesSelection(p.uf, s.uf);
        const n = ufOk ? currentCounts[String(p.code)] || 0 : 0;
        return {{
          color: n > 0 ? '#48a7df' : '#25415f',
          weight: n > 0 ? 0.8 : 0.25,
          opacity: ufOk ? 0.62 : 0.12,
          fillColor: colorScale(n),
          fillOpacity: n > 0 ? 0.62 : 0.05
        }};
      }});

      const rows = visibleRows();
      let total = 0;
      rows.forEach(p => {{
        total += p.filtered;
        L.circleMarker([p.lat, p.lon], {{
          radius: radiusScale(p.filtered),
          color: '#dff6ff',
          weight: 1,
          fillColor: colorScale(p.filtered),
          fillOpacity: 0.86
        }})
        .bindPopup(`<div class="popup-title">${{escapeHtml(p.name)}} - ${{p.uf}}</div><div class="popup-meta">${{br.format(p.filtered)}} CNPJs no filtro</div>`)
        .bindTooltip(tooltipHtml(p, p.filtered), {{
          sticky: true,
          direction: 'top',
          opacity: 1,
          className: 'hover-tooltip'
        }})
        .on('click', () => openCompanyTable(p))
        .addTo(pointLayer);
      }});

      statEmpresas.textContent = br.format(total);
      statMunicipios.textContent = br.format(rows.length);
      ranking.innerHTML = rows.slice(0, 12).map(p =>
        `<div class="rank-row"><div>${{escapeHtml(p.name)}}<small>${{p.uf}}</small></div><strong>${{br.format(p.filtered)}}</strong></div>`
      ).join('') || '<div class="sub">Nenhum município encontrado para o filtro.</div>';
      updateDetails();
      if (activePoint) renderCompanyTable();
    }}

    Object.values(els).forEach(el => el.addEventListener('input', update));
    filterSearches.forEach(input => input.addEventListener('input', async () => {{
      if (input.dataset.target === 'cnpj') {{
        await updateCnpjOptions(input);
      }} else {{
        filterSelectOptions(input);
      }}
    }}));
    clearButtons.forEach(button => button.addEventListener('click', () => {{
      resetSelect(button.dataset.target);
      update();
    }}));
    clearAllButton.addEventListener('click', resetAllFilters);
    refreshNcmCompanies.addEventListener('click', updateNcmCompanyList);
    closeModal.addEventListener('click', closeCompanyTable);
    companyModal.addEventListener('click', event => {{
      if (event.target === companyModal) closeCompanyTable();
    }});
    companySearch.addEventListener('input', () => {{
      currentPage = 1;
      renderCompanyTable();
    }});
    clearCompanySearch.addEventListener('click', () => {{
      companySearch.value = '';
      currentPage = 1;
      renderCompanyTable();
    }});
    prevPage.addEventListener('click', async () => {{
      currentPage -= 1;
      if (activePoint) activeCompanies = await loadCompanyPage(activePoint.code, currentPage);
      renderCompanyTable();
    }});
    nextPage.addEventListener('click', async () => {{
      currentPage += 1;
      if (activePoint) activeCompanies = await loadCompanyPage(activePoint.code, currentPage);
      renderCompanyTable();
    }});
    update();

    const legend = L.control({{ position: 'bottomleft' }});
    legend.onAdd = () => {{
      const div = L.DomUtil.create('div', 'legend');
      div.innerHTML = '<b>Empresas por município</b><br>tons mais claros indicam maior concentração';
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>"""
    OUTPUT_HTML.write_text(html, encoding="utf-8")


def write_componentes_html(payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = """<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Cadeia GT, NCM e CNPJs</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root { --bg:#06111f; --panel:#0b1728; --ink:#e8f3ff; --muted:#8aa7c4; --line:#1d3958; --blue:#3ba4ff; --green:#5ee48a; --cyan:#63d7ff; }
    * { box-sizing:border-box; }
    body { margin:0; font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }
    .app { min-height:100vh; display:grid; grid-template-columns:minmax(420px,540px) 1fr; }
    aside { max-height:100vh; overflow:auto; padding:24px; background:linear-gradient(180deg,#0b1728,#071321); border-right:1px solid var(--line); }
    h1 { margin:0 0 8px; font-size:26px; line-height:1.08; }
    h2 { margin:0 0 9px; font-size:15px; }
    .sub { color:var(--muted); font-size:14px; line-height:1.45; margin-bottom:18px; }
    .nav,.tabs { display:flex; gap:8px; flex-wrap:wrap; margin:0 0 14px; }
    .nav a,button { border:1px solid #2b6ea3; border-radius:7px; padding:8px 10px; background:rgba(59,164,255,.12); color:var(--ink); font:inherit; font-size:12px; cursor:pointer; text-decoration:none; }
    .nav a:hover,button:hover,.tab.active { background:rgba(59,164,255,.22); border-color:var(--cyan); color:var(--cyan); }
    .stats { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px; }
    .stat { border:1px solid var(--line); border-radius:8px; padding:12px; background:rgba(16,42,70,.72); }
    .stat strong { display:block; color:var(--cyan); font-size:22px; line-height:1; }
    .stat span { display:block; margin-top:6px; color:var(--muted); font-size:12px; }
    .panel,.details,.companies,.ranking { border:1px solid var(--line); border-radius:8px; padding:13px; background:rgba(7,19,33,.72); margin-bottom:14px; }
    label { display:block; margin:0 0 6px; color:#aac9e9; font-size:11px; font-weight:800; text-transform:uppercase; }
    input,select { width:100%; border:1px solid #24486c; border-radius:7px; padding:10px 11px; background:#09182a; color:var(--ink); font:inherit; outline:none; }
    select { min-height:126px; padding:8px; }
    option { padding:6px 7px; border-radius:5px; }
    .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
    .tree { max-height:360px; overflow:auto; padding:8px; border:1px solid #24486c; border-radius:8px; background:#081625; }
    details { border-left:1px solid rgba(138,167,196,.22); margin-left:7px; padding-left:9px; }
    summary { cursor:pointer; padding:5px 0; color:#d8ecff; font-size:13px; }
    .tree-row { display:flex; align-items:flex-start; gap:7px; padding:4px 0; color:#d8ecff; font-size:13px; line-height:1.25; text-transform:none; font-weight:500; }
    .tree-row input { width:auto; margin-top:2px; }
    .tree-row small { display:block; color:var(--muted); }
    .view { display:none; }
    .view.active { display:block; }
    .details p { margin:5px 0; color:var(--muted); font-size:13px; line-height:1.35; }
    .chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:9px; }
    .chip { border:1px solid rgba(99,215,255,.32); border-radius:999px; padding:4px 7px; color:#cfe8ff; font-size:12px; }
    .company-row,.rank-row { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; padding:9px 0; border-bottom:1px solid rgba(138,167,196,.16); font-size:13px; cursor:pointer; }
    .company-row small,.rank-row small { display:block; color:var(--muted); margin-top:2px; }
    .company-row strong,.rank-row strong { color:var(--cyan); }
    .company-row.patented strong:last-child { color:var(--green); }
    .table-wrap { overflow:auto; max-height:360px; border:1px solid rgba(138,167,196,.18); border-radius:8px; }
    table { width:100%; min-width:620px; border-collapse:collapse; font-size:12px; }
    th,td { padding:7px 8px; border-bottom:1px solid rgba(138,167,196,.16); text-align:left; vertical-align:top; }
    th { color:#aac9e9; background:#09182a; position:sticky; top:0; z-index:1; }
    td.num { text-align:right; color:var(--cyan); font-weight:700; }
    .chart { width:100%; height:330px; border:1px solid rgba(138,167,196,.18); border-radius:8px; background:#081522; margin-top:10px; }
    .flow-tools { display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin:4px 0 10px; }
    .flow-tools select { width:auto; min-height:0; padding:8px 10px; }
    #map { width:100%; min-height:100vh; background:#06111f; }
    .leaflet-tile-pane { filter:brightness(.58) saturate(.75) hue-rotate(175deg) contrast(1.15); }
    .leaflet-popup-content-wrapper,.leaflet-popup-tip { background:#071321; color:var(--ink); border:1px solid var(--line); }
    @media (max-width:920px) { .app { grid-template-columns:1fr; } aside { max-height:none; border-right:0; } #map { min-height:72vh; } }
  </style>
</head>
<body>
  <main class="app">
    <aside>
      <h1>Cadeia GT, NCM e CNPJs</h1>
      <div class="sub">Drilldown integrado de GT, subsistema, componente, NCM, municipio e CNPJ.</div>
      <nav class="nav"><a href="index.html">Empresas</a><button id="clearAll" type="button">Limpar filtros</button></nav>
      <section class="stats">
        <div class="stat"><strong id="statEmpresas">0</strong><span>CNPJs únicos na cadeia</span></div>
        <div class="stat"><strong id="statMunicipios">0</strong><span>municipios ativos</span></div>
        <div class="stat"><strong id="statComponentes">0</strong><span>componentes</span></div>
        <div class="stat"><strong id="statNcms">0/0</strong><span>NCMs destacados</span></div>
        <div class="stat"><strong id="statLinks">0</strong><span>vinculos CNPJ-NCM</span></div>
      </section>
      <section class="panel">
        <label for="treeSearch">Arvore GT > Subsistema > Componente > NCM</label>
        <input id="treeSearch" type="search" placeholder="Buscar GT, componente ou NCM" />
        <div id="chainTree" class="tree"></div>
      </section>
      <section class="panel">
        <div class="grid2">
          <div><label for="ufFilter">UF</label><select id="ufFilter" multiple size="4"></select></div>
          <div><label for="revenueFilter">Faturamento</label><select id="revenueFilter" multiple size="4"></select></div>
          <div><label for="groupFilter">CNAE grupo</label><select id="groupFilter" multiple size="4"></select></div>
          <div><label for="citySearch">Municipio</label><input id="citySearch" type="search" placeholder="Buscar municipio" /></div>
        </div>
        <label for="cnpjSearch" style="margin-top:10px">CNPJ ou razao social</label>
        <input id="cnpjSearch" type="search" placeholder="Buscar na lista do municipio selecionado" />
      </section>
      <section class="details"><h2>Recorte atual</h2><div id="selectionDetails"></div></section>
      <div class="tabs">
        <button class="tab active" data-view="rankingView" type="button">Ranking</button>
        <button class="tab" data-view="matrixView" type="button">Matriz</button>
        <button class="tab" data-view="profileView" type="button">Perfil</button>
        <button class="tab" data-view="flowsView" type="button">Fluxos</button>
      </div>
      <section class="ranking">
        <div id="rankingView" class="view active"><h2>Municipios em destaque</h2><div id="ranking"></div></div>
        <div id="matrixView" class="view"><h2>Matriz Municipio x NCM</h2><div id="matrix"></div></div>
        <div id="profileView" class="view"><h2>Perfil do municipio</h2><div id="profile"><p>Selecione um municipio no mapa ou ranking.</p></div></div>
        <div id="flowsView" class="view">
          <h2>Fluxos territoriais e cadeia</h2>
          <div class="flow-tools">
            <label for="flowMode" style="margin:0">Setas</label>
            <select id="flowMode">
              <option value="component">Componente → Subsistema</option>
              <option value="gt">Subsistema → GT</option>
            </select>
          </div>
          <div id="flowMap" class="chart"></div>
          <div id="flowSankey" class="chart"></div>
        </div>
      </section>
      <section class="companies"><h2>CNPJs do municipio selecionado</h2><div id="companyList"><p class="sub">Selecione um municipio para listar CNPJs relacionados.</p></div></section>
    </aside>
    <div id="map"></div>
  </main>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <script>
    const DATA = __DATA__;
    const br = new Intl.NumberFormat('pt-BR');
    const map = L.map('map', { zoomControl:false }).setView([-27.6,-51.2],6);
    L.control.zoom({ position:'bottomright' }).addTo(map);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:18, attribution:'&copy; OpenStreetMap' }).addTo(map);
    const chainTree = document.getElementById('chainTree');
    const treeSearch = document.getElementById('treeSearch');
    const ufFilter = document.getElementById('ufFilter');
    const revenueFilter = document.getElementById('revenueFilter');
    const groupFilter = document.getElementById('groupFilter');
    const citySearch = document.getElementById('citySearch');
    const cnpjSearch = document.getElementById('cnpjSearch');
    const selectionDetails = document.getElementById('selectionDetails');
    const companyList = document.getElementById('companyList');
    const ranking = document.getElementById('ranking');
    const matrix = document.getElementById('matrix');
    const profile = document.getElementById('profile');
    const flowMode = document.getElementById('flowMode');
    const flowMapEl = document.getElementById('flowMap');
    const flowSankeyEl = document.getElementById('flowSankey');
    const statEmpresas = document.getElementById('statEmpresas');
    const statMunicipios = document.getElementById('statMunicipios');
    const statComponentes = document.getElementById('statComponentes');
    const statNcms = document.getElementById('statNcms');
    const statLinks = document.getElementById('statLinks');
    const componentsById = Object.fromEntries(DATA.components.map(item => [item.id, item]));
    const pointsByCode = Object.fromEntries(DATA.points.map(point => [point.code, point]));
    const detailCache = {};
    let activeCityCode = null;
    let currentByCity = {};
    let currentRows = [];
    let currentState = null;
    let updateSeq = 0;
    let lastFlowStats = { componentStats:new Map(), subsystemStats:new Map(), gtStats:new Map(), cityComponent:new Map(), citySubsystem:new Map(), cityGt:new Map() };
    let flowMapChart = null;
    let flowSankeyChart = null;

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
    }
    function normalizeText(value) {
      return String(value || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    }
    function selectedValues(el) {
      return new Set(Array.from(el.selectedOptions).map(option => option.value).filter(value => value !== 'all'));
    }
    function fillSelect(el, rows, allLabel) {
      el.innerHTML = `<option value="all">${escapeHtml(allLabel)}</option>` + rows.map(row => `<option value="${escapeHtml(row.value)}">${escapeHtml(row.label)} (${br.format(row.count || 0)})</option>`).join('');
      el.options[0].selected = true;
    }
    function matchesSet(value, set) {
      return !set.size || set.has(String(value));
    }
    function buildTree() {
      const byGt = new Map();
      DATA.components.forEach(component => {
        if (!byGt.has(component.gt)) byGt.set(component.gt, new Map());
        const bySubsystem = byGt.get(component.gt);
        if (!bySubsystem.has(component.subsystem)) bySubsystem.set(component.subsystem, []);
        bySubsystem.get(component.subsystem).push(component);
      });
      chainTree.innerHTML = [...byGt.entries()].sort().map(([gt, bySubsystem]) => `
        <details open data-search="${escapeHtml(normalizeText(gt))}">
          <summary><label class="tree-row"><input type="checkbox" data-kind="gt" value="${escapeHtml(gt)}"> <span>${escapeHtml(gt)}<small>GT</small></span></label></summary>
          ${[...bySubsystem.entries()].sort().map(([subsystem, components]) => `
            <details open data-search="${escapeHtml(normalizeText(gt + ' ' + subsystem))}">
              <summary><label class="tree-row"><input type="checkbox" data-kind="subsystem" data-gt="${escapeHtml(gt)}" value="${escapeHtml(subsystem)}"> <span>${escapeHtml(subsystem)}<small>Subsistema</small></span></label></summary>
              ${components.sort((a,b) => a.name.localeCompare(b.name)).map(component => `
                <details data-search="${escapeHtml(normalizeText(gt + ' ' + subsystem + ' ' + component.name + ' ' + component.ncms.map(n => n.code).join(' ')))}">
                  <summary><label class="tree-row"><input type="checkbox" data-kind="component" value="${escapeHtml(component.id)}"> <span>${escapeHtml(component.name)}<small>${br.format(component.companies)} CNPJs no componente</small></span></label></summary>
                  ${component.ncms.map(ncm => `<label class="tree-row" data-search="${escapeHtml(normalizeText(component.name + ' ' + ncm.code + ' ' + ncm.name))}"><input type="checkbox" data-kind="ncm" data-component="${escapeHtml(component.id)}" value="${escapeHtml(ncm.code)}"> <span>${escapeHtml(ncm.code)}<small>${br.format(ncm.companies || 0)} CNPJs neste NCM · ${escapeHtml(ncm.name || '')}</small></span></label>`).join('')}
                </details>`).join('')}
            </details>`).join('')}
        </details>`).join('');
    }
    function checked(kind) {
      return new Set(Array.from(chainTree.querySelectorAll(`input[data-kind="${kind}"]:checked`)).map(input => input.value));
    }
    function deriveChainSelection() {
      const gt = checked('gt');
      const subsystem = new Set(Array.from(chainTree.querySelectorAll('input[data-kind="subsystem"]:checked')).map(input => `${input.dataset.gt}||${input.value}`));
      const component = checked('component');
      const selectedNcmPairs = Array.from(chainTree.querySelectorAll('input[data-kind="ncm"]:checked')).map(input => ({ component:input.dataset.component, ncm:input.value }));
      let components = DATA.components.filter(item =>
        (!gt.size || gt.has(item.gt))
        && (!subsystem.size || subsystem.has(`${item.gt}||${item.subsystem}`))
        && (!component.size || component.has(item.id))
      );
      if (selectedNcmPairs.length) {
        const ncmComponents = new Set(selectedNcmPairs.map(item => item.component));
        components = components.filter(item => ncmComponents.has(item.id));
      }
      const componentIds = new Set(components.map(item => item.id));
      const allNcmSet = new Set(components.flatMap(item => item.ncms.map(ncm => String(ncm.code))));
      let ncmPairs = selectedNcmPairs.length
        ? selectedNcmPairs
        : DATA.componentMappings.filter(item => componentIds.has(item.component)).map(item => ({ component:item.component, ncm:String(item.ncm) }));
      ncmPairs = ncmPairs.filter(item => componentIds.has(item.component));
      const ncmSet = new Set(ncmPairs.map(item => String(item.ncm)));
      return { components, componentIds, ncmPairs, ncmSet, allNcmSet };
    }
    function color(value, max) {
      if (!value) return '#17304d';
      const t = Math.min(1, value / Math.max(1, max));
      return `rgb(${28 + Math.round(26*t)}, ${104 + Math.round(112*t)}, ${100 + Math.round(55*t)})`;
    }
    const polygonLayer = L.geoJSON(DATA.geojson, {
      style: () => ({ color:'#24486c', weight:.7, fillColor:'#102a46', fillOpacity:.45 }),
      onEachFeature: (feature, layer) => {
        layer.on('click', () => {
          const code = String(feature.properties.code);
          activeCityCode = code;
          const row = currentByCity[code];
          const point = pointsByCode[code] || feature.properties;
          layer.bindPopup(`<strong>${escapeHtml(point.name || feature.properties.name)}</strong><br>${row ? br.format(row.n) : 0} CNPJs únicos na cadeia`).openPopup();
          renderProfile();
          renderCompanies();
        });
      }
    }).addTo(map);
    function updateMap(rows) {
      currentByCity = Object.fromEntries(rows.map(row => [row.code, row]));
      const max = Math.max(1, ...rows.map(row => row.n));
      polygonLayer.eachLayer(layer => {
        const code = String(layer.feature.properties.code);
        const row = currentByCity[code];
        layer.setStyle({ fillColor: color(row?.n || 0, max), fillOpacity: row ? .78 : .22, weight: row ? 1 : .5 });
      });
    }
    function componentNcmKey(row) {
      return `${row.component}||${row.ncm}`;
    }
    function currentFilterState() {
      const chain = deriveChainSelection();
      const ufs = selectedValues(ufFilter);
      const revenues = selectedValues(revenueFilter);
      const groups = selectedValues(groupFilter);
      const cityTerm = normalizeText(citySearch.value);
      const allowedPairs = new Set(chain.ncmPairs.map(componentNcmKey));
      const activeGroups = new Set(DATA.componentMappings.filter(item => chain.componentIds.has(item.component) && chain.ncmSet.has(String(item.ncm))).map(item => String(item.group)));
      return { chain, ufs, revenues, groups, cityTerm, allowedPairs, activeGroups };
    }
    function computeRows(state) {
      const byCity = new Map();
      const sourceRows = DATA.cityComponentNcmFilter || DATA.cityComponentNcm;
      sourceRows.filter(row => state.allowedPairs.has(componentNcmKey(row))).forEach(row => {
        const point = pointsByCode[row.code];
        if (!point) return;
        if (!matchesSet(point.uf, state.ufs)) return;
        if (row.revenue !== undefined && !matchesSet(row.revenue, state.revenues)) return;
        if (row.group !== undefined && !matchesSet(row.group, state.groups)) return;
        if (state.cityTerm && !normalizeText(point.name).includes(state.cityTerm)) return;
        if (!byCity.has(row.code)) byCity.set(row.code, { code:row.code, n:0, cnpjs:0, patented:0, patents:0, ncms:new Set(), components:new Set() });
        const out = byCity.get(row.code);
        out.n += Number(row.cnpjs || row.n || 0);
        out.cnpjs += Number(row.cnpjs || 0);
        out.patented += Number(row.patented || 0);
        out.patents += Number(row.patents || 0);
        out.ncms.add(String(row.ncm));
        out.components.add(String(row.component));
      });
      return [...byCity.values()].map(row => ({ ...row, n:row.cnpjs, ncms:[...row.ncms], components:[...row.components] })).sort((a,b) => b.n - a.n);
    }
    function renderDetails(state, rows) {
      const total = rows.reduce((sum, row) => sum + row.n, 0);
      const links = rows.reduce((sum, row) => sum + (row.links || 0), 0);
      selectionDetails.innerHTML = `
        <p><strong>${br.format(state.chain.components.length)}</strong> componentes, <strong>${br.format(state.chain.ncmSet.size)}/${br.format(state.chain.allNcmSet.size)}</strong> NCMs destacados e <strong>${br.format(state.activeGroups.size)}</strong> grupos CNAE relacionados.</p>
        <p>${state.chain.components.length === DATA.components.length ? 'Sem seleção: considerando todos os GTs, subsistemas, componentes e NCMs mapeados.' : 'Seleção ativa: considerando apenas os itens marcados na árvore.'}</p>
        <p><strong>${br.format(total)}</strong> CNPJs unicos na cadeia e <strong>${br.format(links)}</strong> vinculos CNPJ-NCM no recorte.</p>
        <p>O vinculo CNPJ-NCM usa o CNAE da empresa e o de-para do NCM destacado.</p>
        <div class="chips">${[...state.chain.ncmSet].slice(0, 14).map(ncm => `<span class="chip">${escapeHtml(ncm)}</span>`).join('') || '<span class="chip">Todos os NCMs da cadeia</span>'}</div>`;
      statEmpresas.textContent = br.format(total);
      statMunicipios.textContent = br.format(rows.length);
      statComponentes.textContent = br.format(state.chain.components.length);
      statNcms.textContent = `${br.format(state.chain.ncmSet.size)}/${br.format(state.chain.allNcmSet.size)}`;
      statLinks.textContent = br.format(links);
    }
    async function loadComponentCompanies(componentId) {
      if (!detailCache[componentId]) {
        const manifest = await fetch(`empresas_app_data/componentes/${componentId}/manifest.json`).then(response => response.json());
        const pages = [];
        for (let page = 1; page <= manifest.pages; page += 1) pages.push(fetch(`empresas_app_data/componentes/${componentId}/${page}.json`).then(response => response.json()));
        const payloads = await Promise.all(pages);
        const rows = payloads.flatMap(payload => payload.rows).map(row => Object.fromEntries(manifest.columns.map((column, index) => [column, row[index]])));
        detailCache[componentId] = { manifest, rows };
      }
      return detailCache[componentId];
    }
    function companyPasses(row, state) {
      if (activeCityCode && String(row.municipioCodigo) !== String(activeCityCode)) return false;
      return companyPassesGlobal(row, state);
    }
    function companyPassesGlobal(row, state) {
      if (!matchesSet(row.uf, state.ufs)) return false;
      if (!matchesSet(row.faturamento, state.revenues)) return false;
      if (!matchesSet(row.grupo, state.groups)) return false;
      if (state.cityTerm && !normalizeText(row.municipio).includes(state.cityTerm)) return false;
      const term = normalizeText(cnpjSearch.value);
      if (term && !normalizeText(`${row.cnpj} ${row.razao}`).includes(term)) return false;
      const rowNcms = Array.isArray(row.ncms) ? row.ncms.map(String).filter(ncm => state.chain.ncmSet.has(ncm)) : [];
      return rowNcms.length > 0 && state.chain.componentIds.has(String(row.componentId));
    }
    async function filteredCompaniesForActiveCity(limit = 300) {
      if (!activeCityCode || !currentState) return [];
      const payloads = await Promise.all([...currentState.chain.componentIds].map(id => loadComponentCompanies(id).catch(() => ({ rows:[] }))));
      const seen = new Map();
      payloads.flatMap(payload => payload.rows).forEach(row => {
        if (!companyPasses(row, currentState)) return;
        const rowNcms = row.ncms.filter(ncm => currentState.chain.ncmSet.has(String(ncm)));
        const key = String(row.cnpj);
        if (!seen.has(key)) {
          seen.set(key, { ...row, ncms:new Set(rowNcms), componentes:new Set([row.componente]) });
        } else {
          const existing = seen.get(key);
          rowNcms.forEach(ncm => existing.ncms.add(ncm));
          existing.componentes.add(row.componente);
          existing.patentes = Math.max(Number(existing.patentes || 0), Number(row.patentes || 0));
        }
      });
      return [...seen.values()].map(row => ({ ...row, ncms:[...row.ncms], componente:[...row.componentes].join('; ') })).sort((a,b) => Number(b.patentes || 0) - Number(a.patentes || 0) || String(a.razao).localeCompare(String(b.razao))).slice(0, limit);
    }
    async function refineRowsByCompanySearch(state) {
      const term = normalizeText(cnpjSearch.value);
      if (!term) return null;
      const payloads = await Promise.all([...state.chain.componentIds].map(id => loadComponentCompanies(id).catch(() => ({ rows:[] }))));
      const byCity = new Map();
      const seen = new Set();
      payloads.flatMap(payload => payload.rows).forEach(row => {
        if (!normalizeText(`${row.cnpj} ${row.razao}`).includes(term)) return;
        if (!matchesSet(row.uf, state.ufs)) return;
        if (!matchesSet(row.faturamento, state.revenues)) return;
        if (!matchesSet(row.grupo, state.groups)) return;
        if (state.cityTerm && !normalizeText(row.municipio).includes(state.cityTerm)) return;
        if (!state.chain.componentIds.has(String(row.componentId))) return;
        const rowNcms = Array.isArray(row.ncms) ? row.ncms.map(String).filter(ncm => state.chain.ncmSet.has(ncm)) : [];
        if (!rowNcms.length) return;
        const key = `${row.cnpj}||${row.componentId}`;
        if (seen.has(key)) return;
        seen.add(key);
        const code = String(row.municipioCodigo);
        if (!byCity.has(code)) byCity.set(code, { code, n:0, cnpjs:0, patented:0, patents:0, ncms:new Set(), components:new Set() });
        const out = byCity.get(code);
        out.n += 1;
        out.cnpjs += 1;
        out.patented += Number(row.patentes || 0) > 0 ? 1 : 0;
        out.patents += Number(row.patentes || 0);
        rowNcms.forEach(ncm => out.ncms.add(ncm));
        out.components.add(String(row.componentId));
      });
      return [...byCity.values()].map(row => ({ ...row, ncms:[...row.ncms], components:[...row.components] })).sort((a,b) => b.n - a.n);
    }
    async function computeExactRowsFromCompanies(state) {
      const payloads = await Promise.all([...state.chain.componentIds].map(id => loadComponentCompanies(id).catch(() => ({ rows:[] }))));
      const byCity = new Map();
      const componentStats = new Map();
      const subsystemStats = new Map();
      const gtStats = new Map();
      const cityComponent = new Map();
      const citySubsystem = new Map();
      const cityGt = new Map();
      function ensureStat(map, key, meta = {}) {
        if (!map.has(key)) map.set(key, { key, cnpjs:new Set(), links:new Set(), ...meta });
        return map.get(key);
      }
      function addToStat(stat, cnpj, linkKey) {
        stat.cnpjs.add(cnpj);
        stat.links.add(linkKey);
      }
      payloads.flatMap(payload => payload.rows).forEach(row => {
        if (!companyPassesGlobal(row, state)) return;
        const rowNcms = Array.isArray(row.ncms) ? row.ncms.map(String).filter(ncm => state.allowedPairs.has(`${row.componentId}||${ncm}`)) : [];
        if (!rowNcms.length) return;
        const component = componentsById[String(row.componentId)];
        if (!component) return;
        const code = String(row.municipioCodigo);
        if (!byCity.has(code)) byCity.set(code, { code, cnpjs:new Set(), links:new Set(), patented:new Set(), patents:0, ncms:new Set(), components:new Set(), ncmCnpjs:new Map() });
        const out = byCity.get(code);
        const cnpj = String(row.cnpj);
        out.cnpjs.add(cnpj);
        if (Number(row.patentes || 0) > 0) out.patented.add(String(row.cnpj));
        out.patents += Number(row.patentes || 0);
        out.components.add(String(row.componentId));
        rowNcms.forEach(ncm => {
          const linkKey = `${cnpj}||${row.componentId}||${ncm}`;
          out.ncms.add(ncm);
          out.links.add(linkKey);
          if (!out.ncmCnpjs.has(ncm)) out.ncmCnpjs.set(ncm, new Set());
          out.ncmCnpjs.get(ncm).add(cnpj);
          addToStat(ensureStat(componentStats, component.id, { component }), cnpj, linkKey);
          addToStat(ensureStat(subsystemStats, `${component.gt}||${component.subsystem}`, { gt:component.gt, subsystem:component.subsystem }), cnpj, linkKey);
          addToStat(ensureStat(gtStats, component.gt, { gt:component.gt }), cnpj, linkKey);
          addToStat(ensureStat(cityComponent, `${code}||${component.id}`, { code, component }), cnpj, linkKey);
          addToStat(ensureStat(citySubsystem, `${code}||${component.gt}||${component.subsystem}`, { code, gt:component.gt, subsystem:component.subsystem }), cnpj, linkKey);
          addToStat(ensureStat(cityGt, `${code}||${component.gt}`, { code, gt:component.gt }), cnpj, linkKey);
        });
      });
      lastFlowStats = { componentStats, subsystemStats, gtStats, cityComponent, citySubsystem, cityGt };
      return [...byCity.values()].map(row => ({
        code:row.code,
        n:row.cnpjs.size,
        cnpjs:row.cnpjs.size,
        links:row.links.size,
        patented:row.patented.size,
        patents:row.patents,
        ncms:[...row.ncms],
        components:[...row.components],
        ncmCounts:Object.fromEntries([...row.ncmCnpjs.entries()].map(([ncm, cnpjs]) => [ncm, cnpjs.size])),
      })).sort((a,b) => b.n - a.n);
    }
    async function renderCompanies() {
      if (!activeCityCode) {
        companyList.innerHTML = '<p class="sub">Selecione um municipio no mapa ou ranking para listar CNPJs.</p>';
        return;
      }
      companyList.innerHTML = '<p class="sub">Carregando CNPJs do municipio...</p>';
      const rows = await filteredCompaniesForActiveCity();
      companyList.innerHTML = rows.length ? rows.map(row => `
        <div class="company-row ${Number(row.patentes || 0) > 0 ? 'patented' : ''}">
          <div><strong>${escapeHtml(row.razao)}</strong><small>${escapeHtml(row.cnpj)} - ${escapeHtml(row.municipio)}/${escapeHtml(row.uf)}</small><small>${escapeHtml(row.gt)} > ${escapeHtml(row.subsistema)} > ${escapeHtml(row.componente)}</small><small>NCM ${escapeHtml(row.ncms.join(', '))} | CNAE ${escapeHtml(row.grupo)} - ${escapeHtml(row.grupoNome)}</small></div>
          <strong>${br.format(Number(row.patentes || 0))}</strong>
        </div>`).join('') : '<p class="sub">Nenhuma empresa para o filtro atual.</p>';
    }
    function renderRanking(rows) {
      ranking.innerHTML = rows.slice(0, 10).map(row => {
        const point = pointsByCode[row.code] || {};
        return `<div class="rank-row" data-code="${escapeHtml(row.code)}"><span>${escapeHtml(point.name || row.code)}<small>${point.uf || ''} | ${br.format(row.ncms.length)} NCMs | ${br.format(row.links || 0)} vinculos</small></span><strong>${br.format(row.n)}</strong></div>`;
      }).join('') || '<p class="sub">Sem municipios relacionados.</p>';
      ranking.querySelectorAll('.rank-row').forEach(row => row.addEventListener('click', () => {
        activeCityCode = row.dataset.code;
        renderProfile();
        renderCompanies();
      }));
    }
    function renderMatrix(rows) {
      const ncms = [...currentState.chain.ncmSet].slice(0, 12);
      const body = rows.slice(0, 12).map(city => {
        const point = pointsByCode[city.code] || {};
        const cells = ncms.map(ncm => {
          const total = city.ncmCounts?.[ncm] || 0;
          return `<td class="num">${total ? br.format(total) : ''}</td>`;
        }).join('');
        return `<tr><td>${escapeHtml(point.name || city.code)}<br><small>${point.uf || ''}</small></td>${cells}</tr>`;
      }).join('');
      matrix.innerHTML = ncms.length ? `<div class="table-wrap"><table><thead><tr><th>Municipio</th>${ncms.map(ncm => `<th>${escapeHtml(ncm)}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></div>` : '<p class="sub">Sem NCMs no recorte.</p>';
    }
    function renderProfile() {
      if (!activeCityCode) {
        profile.innerHTML = '<p>Selecione um municipio no mapa ou ranking.</p>';
        return;
      }
      const city = currentByCity[activeCityCode];
      const point = pointsByCode[activeCityCode] || {};
      if (!city) {
        profile.innerHTML = `<p>${escapeHtml(point.name || activeCityCode)} nao possui CNPJs na cadeia para o recorte atual.</p>`;
        return;
      }
      const components = city.components.map(id => componentsById[id]).filter(Boolean);
      const gtCounts = new Map();
      components.forEach(item => gtCounts.set(item.gt, (gtCounts.get(item.gt) || 0) + 1));
      profile.innerHTML = `<p><strong>${escapeHtml(point.name || activeCityCode)}/${escapeHtml(point.uf || '')}</strong></p><p>${br.format(city.n)} CNPJs unicos, ${br.format(city.links || 0)} vinculos, ${br.format(city.components.length)} componentes e ${br.format(city.ncms.length)} NCMs destacados.</p><div class="chips">${[...gtCounts.entries()].map(([gt,count]) => `<span class="chip">${escapeHtml(gt)}: ${br.format(count)}</span>`).join('')}</div><div class="chips">${city.ncms.slice(0, 18).map(ncm => `<span class="chip">${escapeHtml(ncm)}</span>`).join('')}</div>`;
    }
    function initFlowCharts() {
      if (!window.echarts || flowMapChart) return;
      echarts.registerMap('sulMunicipios', DATA.geojson);
      flowMapChart = echarts.init(flowMapEl);
      flowSankeyChart = echarts.init(flowSankeyEl);
    }
    function statValue(stat) {
      return { cnpjs: stat?.cnpjs?.size || 0, links: stat?.links?.size || 0 };
    }
    function topCityFor(map, predicate) {
      let best = null;
      for (const stat of map.values()) {
        if (!predicate(stat)) continue;
        const value = stat.cnpjs.size;
        if (!best || value > best.value) best = { code:stat.code, value };
      }
      return best;
    }
    function renderSankeyFlow() {
      if (!flowSankeyChart) return;
      const nodes = new Map();
      const links = [];
      function addNode(name, depth, value) {
        if (!nodes.has(name)) nodes.set(name, { name, depth, value });
      }
      for (const stat of lastFlowStats.componentStats.values()) {
        const component = stat.component;
        const componentName = `Componente | ${component.name}`;
        const subsystemName = `Subsistema | ${component.gt} | ${component.subsystem}`;
        const gtName = `GT | ${component.gt}`;
        addNode(componentName, 0, stat.cnpjs.size);
        addNode(subsystemName, 1, statValue(lastFlowStats.subsystemStats.get(`${component.gt}||${component.subsystem}`)).cnpjs);
        addNode(gtName, 2, statValue(lastFlowStats.gtStats.get(component.gt)).cnpjs);
        links.push({ source:componentName, target:subsystemName, value:Math.max(1, stat.cnpjs.size), links:stat.links.size });
      }
      for (const stat of lastFlowStats.subsystemStats.values()) {
        const subsystemName = `Subsistema | ${stat.gt} | ${stat.subsystem}`;
        const gtName = `GT | ${stat.gt}`;
        links.push({ source:subsystemName, target:gtName, value:Math.max(1, stat.cnpjs.size), links:stat.links.size });
      }
      flowSankeyChart.setOption({
        backgroundColor:'#081522',
        tooltip:{
          trigger:'item',
          triggerOn:'mousemove',
          formatter: params => {
            if (params.dataType === 'edge') return `${escapeHtml(params.data.source)}<br>→ ${escapeHtml(params.data.target)}<br>${br.format(params.data.value)} CNPJs únicos<br>${br.format(params.data.links || 0)} vínculos`;
            return `${escapeHtml(params.name)}<br>${br.format(params.data.value || 0)} CNPJs únicos`;
          }
        },
        series:[{
          type:'sankey',
          data:[...nodes.values()],
          links,
          nodeWidth:12,
          nodeGap:8,
          layoutIterations:64,
          emphasis:{ focus:'adjacency' },
          levels:[
            { depth:0, itemStyle:{ color:'#54b6ff' }, lineStyle:{ color:'source', opacity:.45 } },
            { depth:1, itemStyle:{ color:'#7ee19b' }, lineStyle:{ color:'source', opacity:.45 } },
            { depth:2, itemStyle:{ color:'#ffd166' }, lineStyle:{ color:'source', opacity:.45 } },
          ],
          label:{ color:'#e8f3ff', fontSize:10 },
          lineStyle:{ curveness:.5 }
        }]
      }, true);
    }
    function renderMapFlow() {
      if (!flowMapChart) return;
      const mode = flowMode.value;
      const nodes = new Map();
      const edges = [];
      function addNode(code, weight) {
        const point = pointsByCode[code];
        if (!point) return;
        const existing = nodes.get(code);
        nodes.set(code, { name:code, labelName:`${point.name}/${point.uf}`, value:[point.lon, point.lat, (existing?.value?.[2] || 0) + weight] });
      }
      if (mode === 'component') {
        for (const stat of lastFlowStats.cityComponent.values()) {
          const target = topCityFor(lastFlowStats.citySubsystem, item => item.gt === stat.component.gt && item.subsystem === stat.component.subsystem);
          if (!target || target.code === stat.code) continue;
          const value = stat.cnpjs.size;
          addNode(stat.code, value);
          addNode(target.code, value);
          edges.push({
            source:stat.code,
            target:target.code,
            value,
            links:stat.links.size,
            level:'Componente → Subsistema',
            label:`${stat.component.name} → ${stat.component.subsystem}`,
            lineStyle:{ width:Math.max(1, Math.min(7, Math.sqrt(value))), opacity:.72, color:'#74e4ff' }
          });
        }
      } else {
        for (const stat of lastFlowStats.citySubsystem.values()) {
          const target = topCityFor(lastFlowStats.cityGt, item => item.gt === stat.gt);
          if (!target || target.code === stat.code) continue;
          const value = stat.cnpjs.size;
          addNode(stat.code, value);
          addNode(target.code, value);
          edges.push({
            source:stat.code,
            target:target.code,
            value,
            links:stat.links.size,
            level:'Subsistema → GT',
            label:`${stat.subsystem} → ${stat.gt}`,
            lineStyle:{ width:Math.max(1, Math.min(7, Math.sqrt(value))), opacity:.72, color:'#ffd166' }
          });
        }
      }
      flowMapChart.setOption({
        backgroundColor:'#081522',
        tooltip:{
          trigger:'item',
          formatter: params => {
            if (params.dataType === 'edge') return `${escapeHtml(params.data.level)}<br>${escapeHtml(params.data.label)}<br>${escapeHtml(pointsByCode[params.data.source]?.name || params.data.source)} → ${escapeHtml(pointsByCode[params.data.target]?.name || params.data.target)}<br>${br.format(params.data.value)} CNPJs únicos<br>${br.format(params.data.links || 0)} vínculos`;
            return `${escapeHtml(params.data.labelName)}<br>${br.format(params.value?.[2] || 0)} CNPJs nas setas`;
          }
        },
        geo:{
          map:'sulMunicipios',
          roam:true,
          aspectScale:.86,
          itemStyle:{ areaColor:'#102a46', borderColor:'#24486c', borderWidth:.35 },
          emphasis:{ itemStyle:{ areaColor:'#174d75' }, label:{ show:false } }
        },
        series:[{
          type:'graph',
          coordinateSystem:'geo',
          data:[...nodes.values()],
          edges,
          symbolSize: value => Math.max(5, Math.min(18, Math.sqrt(value[2] || 1) + 3)),
          edgeSymbol:['none','arrow'],
          edgeSymbolSize:7,
          lineStyle:{ curveness:.18 },
          itemStyle:{ color:'#7ee19b' },
          label:{ show:false }
        }]
      }, true);
    }
    function renderFlows() {
      initFlowCharts();
      if (!flowMapChart || !flowSankeyChart) return;
      renderMapFlow();
      renderSankeyFlow();
      setTimeout(() => { flowMapChart.resize(); flowSankeyChart.resize(); }, 0);
    }
    async function update() {
      const seq = ++updateSeq;
      currentState = currentFilterState();
      currentRows = computeRows(currentState);
      currentRows = await computeExactRowsFromCompanies(currentState);
      if (seq !== updateSeq) return;
      if (activeCityCode && !currentRows.some(row => row.code === activeCityCode)) activeCityCode = null;
      renderDetails(currentState, currentRows);
      renderRanking(currentRows);
      renderMatrix(currentRows);
      renderProfile();
      updateMap(currentRows);
      renderFlows();
      await renderCompanies();
    }
    function resetAll() {
      chainTree.querySelectorAll('input[type="checkbox"]').forEach(input => input.checked = false);
      [ufFilter, revenueFilter, groupFilter].forEach(el => Array.from(el.options).forEach((option, index) => option.selected = index === 0));
      treeSearch.value = '';
      citySearch.value = '';
      cnpjSearch.value = '';
      activeCityCode = null;
      update();
    }
    treeSearch.addEventListener('input', () => {
      const term = normalizeText(treeSearch.value);
      chainTree.querySelectorAll('[data-search]').forEach(node => {
        node.hidden = term && !node.dataset.search.includes(term);
      });
    });
    chainTree.addEventListener('change', update);
    [ufFilter, revenueFilter, groupFilter].forEach(el => el.addEventListener('change', update));
    [citySearch, cnpjSearch].forEach(el => el.addEventListener('input', update));
    flowMode.addEventListener('change', renderFlows);
    document.getElementById('clearAll').addEventListener('click', resetAll);
    document.querySelectorAll('.tab').forEach(tab => tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(item => item.classList.remove('active'));
      document.querySelectorAll('.view').forEach(item => item.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(tab.dataset.view).classList.add('active');
      if (tab.dataset.view === 'flowsView') renderFlows();
    }));
    buildTree();
    fillSelect(ufFilter, DATA.options.uf, 'Todas UFs');
    fillSelect(revenueFilter, DATA.options.revenue, 'Todos faturamentos');
    fillSelect(groupFilter, DATA.options.group, 'Todos CNAEs');
    update();
  </script>
</body>
</html>""".replace("__DATA__", data)
    OUTPUT_COMPONENTES_HTML.write_text(html, encoding="utf-8")


def main() -> None:
    print("Lendo planilha de empresas e de-para CNAE...")
    empresas = load_empresas()
    print(f"Empresas carregadas: {len(empresas):,}")
    print(f"Com faturamento informado: {count_informative_revenue(empresas):,}")
    print(f"Sem de-para CNAE: {(empresas['cnae_grupo_nome'] == NA_VALUE).sum():,}")
    print("Carregando malha municipal...")
    municipios = load_municipios()
    print("Agregando dados e montando payload...")
    payload = build_payload(empresas, municipios)
    print("Gerando arquivos detalhados por município...")
    write_detail_files(empresas)
    print("Gerando índice de CNPJ...")
    write_cnpj_index(empresas)
    print("Gerando HTML...")
    write_html(payload)
    print("Gerando visualização NCM x Empresas...")
    ncm_payload = build_ncm_payload(empresas, municipios)
    write_ncm_html(ncm_payload)
    print("Carregando componentes e patentes INPI...")
    mapped_componentes = component_cnae_map()
    patentes = load_patentes_inpi_stats()
    empresas_componentes = attach_patent_stats(empresas, patentes)
    print("Gerando arquivos de empresas por componente...")
    write_component_files(empresas_componentes, mapped_componentes)
    print("Gerando visualização Cadeia de Componentes...")
    componentes_payload = build_componentes_payload(empresas_componentes, municipios, mapped_componentes)
    write_componentes_html(componentes_payload)
    print("Criando pacote otimizado para deploy...")
    create_deploy_package()
    print(f"OK: {OUTPUT_HTML}")
    print(f"OK: {OUTPUT_NCM_HTML}")
    print(f"OK: {OUTPUT_COMPONENTES_HTML}")
    print(f"OK: {DIST_DIR}")


if __name__ == "__main__":
    main()

