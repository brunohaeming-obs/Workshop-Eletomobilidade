"""
Funções de normalização e utilitários de transformação de dados.
"""

import re
import unicodedata
import pandas as pd
from config import NA_VALUE


def normalize_code(series: pd.Series) -> pd.Series:
    """Normaliza códigos numéricos para formato padronizado."""
    return (
        pd.to_numeric(series, errors="coerce")
        .astype("Int64")
        .astype(str)
        .replace("<NA>", NA_VALUE)
    )


def normalize_ncm_code(series: pd.Series) -> pd.Series:
    """Normaliza códigos NCM para 8 dígitos."""
    digits = series.fillna("").astype(str).str.replace(r"\D", "", regex=True)
    return digits.str.zfill(8).replace({"00000000": NA_VALUE, "": NA_VALUE})


def normalize_cnpj(series: pd.Series) -> pd.Series:
    """Normaliza CNPJ para 14 dígitos."""
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(14)
    )


def slugify(value: str) -> str:
    """Converte string para slug (lowercase, sem acentos, com hífens)."""
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text or "item"


def cnae_group_from_subclass(series: pd.Series) -> pd.Series:
    """Extrai código de grupo CNAE a partir da subclasse."""
    digits = series.fillna("").astype(str).str.replace(r"\D", "", regex=True)
    return digits.str.zfill(7).str[:3].replace({"000": NA_VALUE, "": NA_VALUE})
