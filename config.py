"""
Configuração centralizada de caminhos e constantes.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dados"
EMPRESAS_CSV = DATA_DIR / "empresas_rfb_2.csv"
EMPRESAS_FATURAMENTO_XLSX = DATA_DIR / "empresas_rfb_databricks.xlsx"
CNAE_XLSX = DATA_DIR / "CNAE subclasse - classe - grupo - divisão.xlsx"
NCM_XLSX = DATA_DIR / "NCM_SH4 - atualizado 07.04.2026.xlsx"
COMPONENTES_XLSX = DATA_DIR / "GT_NCM_Dados_Brutos.xlsx"
COMPONENTES_XLSX_V2 = DATA_DIR / "GT_NCM_Dados_Brutos_V2.xlsx"
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
