from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import NA_VALUE
from loaders import component_cnae_map, load_empresas


ROOT = Path(__file__).resolve().parent
OUT_XLSX = ROOT / "empresas_por_subsistema_e_componente.xlsx"


def build_tables() -> dict[str, pd.DataFrame]:
    empresas = load_empresas()
    mapped = component_cnae_map()

    by_component_rows = []
    by_ncm_rows = []
    by_city_component_rows = []

    for component_id, part in mapped.groupby("component_id", sort=False):
        first = part.iloc[0]
        groups = {str(value) for value in part["cnae_grupo"] if str(value) != NA_VALUE}
        companies = empresas[empresas["cnae_grupo"].astype(str).isin(groups)] if groups else empresas.iloc[0:0]

        by_component_rows.append(
            {
                "GT": first["gt"],
                "Subsistema": first["subsistema"],
                "Componente": first["componente"],
                "Criticidade": first.get("criticidade", NA_VALUE),
                "Complexidade": first.get("complexidade", NA_VALUE),
                "CNPJs únicos": companies["nr_cnpj"].astype(str).nunique(),
                "Municípios": companies["cd_municipio_ibge"].nunique(),
                "UFs": companies["sg_uf"].nunique(),
                "NCMs destacados": part["ncm"].astype(str).nunique(),
                "Grupos CNAE": len(groups),
            }
        )

        for ncm, ncm_part in part.groupby("ncm", sort=False):
            ncm_groups = {str(value) for value in ncm_part["cnae_grupo"] if str(value) != NA_VALUE}
            ncm_companies = empresas[empresas["cnae_grupo"].astype(str).isin(ncm_groups)] if ncm_groups else empresas.iloc[0:0]
            by_ncm_rows.append(
                {
                    "GT": first["gt"],
                    "Subsistema": first["subsistema"],
                    "Componente": first["componente"],
                    "NCM": str(ncm),
                    "Descrição NCM": str(ncm_part["ncm_descricao_gt"].dropna().iloc[0]) if not ncm_part.empty else "",
                    "CNPJs únicos": ncm_companies["nr_cnpj"].astype(str).nunique(),
                    "Municípios": ncm_companies["cd_municipio_ibge"].nunique(),
                    "Grupos CNAE": len(ncm_groups),
                }
            )

        if not companies.empty:
            city = (
                companies.groupby(["sg_uf", "nm_municipio"], dropna=False)
                .agg(**{"CNPJs únicos": ("nr_cnpj", "nunique")})
                .reset_index()
                .rename(columns={"sg_uf": "UF", "nm_municipio": "Município"})
            )
            city["GT"] = first["gt"]
            city["Subsistema"] = first["subsistema"]
            city["Componente"] = first["componente"]
            by_city_component_rows.append(city)

    by_component = pd.DataFrame(by_component_rows).sort_values(
        ["GT", "Subsistema", "CNPJs únicos", "Componente"],
        ascending=[True, True, False, True],
    )
    by_subsystem = (
        by_component.groupby(["GT", "Subsistema"], dropna=False)
        .agg(
            **{
                "Componentes": ("Componente", "nunique"),
                "CNPJs únicos": ("CNPJs únicos", "sum"),
                "Municípios": ("Municípios", "max"),
                "NCMs destacados": ("NCMs destacados", "sum"),
                "Criticidade média": ("Criticidade", "mean"),
                "Complexidade média": ("Complexidade", "mean"),
            }
        )
        .reset_index()
        .sort_values("CNPJs únicos", ascending=False)
    )
    by_ncm = pd.DataFrame(by_ncm_rows).sort_values(
        ["GT", "Subsistema", "Componente", "CNPJs únicos"],
        ascending=[True, True, True, False],
    )
    by_city_component = (
        pd.concat(by_city_component_rows, ignore_index=True)
        if by_city_component_rows
        else pd.DataFrame(columns=["UF", "Município", "CNPJs únicos", "GT", "Subsistema", "Componente"])
    ).sort_values(["GT", "Subsistema", "Componente", "CNPJs únicos"], ascending=[True, True, True, False])

    return {
        "Resumo subsistemas": by_subsystem,
        "Componentes": by_component,
        "NCM por componente": by_ncm,
        "Municipio componente": by_city_component,
    }


def write_excel(tables: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        for sheet, df in tables.items():
            df.to_excel(writer, sheet_name=sheet[:31], index=False)
            ws = writer.book[sheet[:31]]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for column_cells in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in column_cells)
                ws.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 48)


def main() -> None:
    tables = build_tables()
    write_excel(tables)
    print(f"OK: {OUT_XLSX}")
    for name, df in tables.items():
        print(f"{name}: {len(df)} linhas")


if __name__ == "__main__":
    main()
