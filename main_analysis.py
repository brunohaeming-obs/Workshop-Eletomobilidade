"""
Script principal que orquestra o pipeline de análise de dados.

Uso:
    python main_analysis.py [--load-empresas] [--write-indices] [--write-html] [--create-deploy]
    
Exemplos:
    # Carregar apenas dados de empresas
    python main_analysis.py --load-empresas
    
    # Executar todo o pipeline
    python main_analysis.py
"""

import argparse
import json
import shutil
from pathlib import Path

from config import (
    DIST_DIR, OUTPUT_HTML, OUTPUT_NCM_HTML, OUTPUT_COMPONENTES_HTML,
    DETAIL_DIR, CNPJ_INDEX_DIR, COMPONENT_INDEX_DIR, LEGACY_CNPJ_INDEX
)
from loaders import load_empresas, load_municipios, component_cnae_map, load_ncm_depara
from enrichers import enrich_empresas_with_patents
from writers import write_detail_files, write_cnpj_index, write_component_files
from htmlgen import (
    build_payload, build_ncm_payload, build_componentes_payload,
    write_html, write_ncm_html, write_componentes_html,
    externalize_payload, create_deploy_package
)


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de análise de dados de empresas, NCM e componentes"
    )
    parser.add_argument(
        "--load-empresas",
        action="store_true",
        help="Apenas carregar e exibir estatísticas de empresas"
    )
    parser.add_argument(
        "--write-indices",
        action="store_true",
        help="Escrever índices de detalhes, CNPJ e componentes"
    )
    parser.add_argument(
        "--write-html",
        action="store_true",
        help="Gerar arquivos HTML de visualização"
    )
    parser.add_argument(
        "--create-deploy",
        action="store_true",
        help="Criar pacote de deploy"
    )

    args = parser.parse_args()

    # Se nenhuma flag específica, executar pipeline completo
    if not any(vars(args).values()):
        print("Executando pipeline completo...")
        load_and_process_all()
    else:
        if args.load_empresas:
            print("Carregando dados de empresas...")
            load_and_display_empresas()
        
        if args.write_indices:
            print("Escrevendo índices...")
            write_all_indices()
        
        if args.write_html:
            print("Gerando HTML...")
            write_all_html()
        
        if args.create_deploy:
            print("Criando pacote de deploy...")
            create_deploy_package()


def load_and_display_empresas():
    """Carrega e exibe estatísticas de empresas."""
    print("  - Carregando empresas...")
    empresas = load_empresas()
    print(f"    OK {len(empresas)} empresas carregadas")
    print(f"    OK {empresas['nr_cnpj'].nunique()} CNPJs únicos")
    print(f"    OK {empresas['cd_municipio_ibge'].nunique()} municípios")
    
    print("  - Enriquecendo com patentes...")
    empresas = enrich_empresas_with_patents(empresas)
    print(f"    OK {empresas['tem_patente'].sum()} CNPJs com patentes")
    
    return empresas


def write_all_indices():
    """Escreve todos os índices."""
    print("  - Carregando dados...")
    empresas = load_and_display_empresas()
    mapped = component_cnae_map()
    
    print("  - Limpando diretórios antigos...")
    if DETAIL_DIR.exists():
        shutil.rmtree(DETAIL_DIR)
    if CNPJ_INDEX_DIR.exists():
        shutil.rmtree(CNPJ_INDEX_DIR)
    if COMPONENT_INDEX_DIR.exists():
        shutil.rmtree(COMPONENT_INDEX_DIR)
    if LEGACY_CNPJ_INDEX.exists():
        LEGACY_CNPJ_INDEX.unlink()
    
    print("  - Escrevendo detalhes por município...")
    write_detail_files(empresas)
    print(f"    OK Arquivos escritos em {DETAIL_DIR}")
    
    print("  - Escrevendo índice CNPJ...")
    write_cnpj_index(empresas)
    print(f"    OK Arquivos escritos em {CNPJ_INDEX_DIR}")
    
    print("  - Escrevendo índice de componentes...")
    write_component_files(empresas, mapped)
    print(f"    OK Arquivos escritos em {COMPONENT_INDEX_DIR}")


def write_all_html():
    """Gera todos os arquivos HTML."""
    print("  - Carregando dados...")
    empresas = load_and_display_empresas()
    municipios = load_municipios()
    mapped = component_cnae_map()
    
    print("  - Construindo payloads...")
    print("    - payload principal...")
    payload = build_payload(empresas, municipios)
    
    print("    - payload NCM...")
    ncm_payload = build_ncm_payload(empresas, municipios)
    
    print("    - payload componentes...")
    componentes_payload = build_componentes_payload(empresas, municipios, mapped)
    
    print("  - Gerando HTML...")
    print(f"    - {OUTPUT_HTML.name}...")
    write_html(payload)
    
    print(f"    - {OUTPUT_NCM_HTML.name}...")
    write_ncm_html(ncm_payload)
    
    print(f"    - {OUTPUT_COMPONENTES_HTML.name}...")
    write_componentes_html(componentes_payload)
    
    print("  OK Arquivos HTML gerados com sucesso")


def load_and_process_all():
    """Executa o pipeline completo."""
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETO DE ANÁLISE")
    print("=" * 60 + "\n")
    
    print("1. CARREGAMENTO E ENRIQUECIMENTO")
    print("-" * 60)
    write_all_indices()
    
    print("\n2. GERAÇÃO DE VISUALIZAÇÕES")
    print("-" * 60)
    write_all_html()
    
    print("\n3. CRIAÇÃO DE PACOTE DE DEPLOY")
    print("-" * 60)
    create_deploy_package()
    
    print("\n" + "=" * 60)
    print("OK PIPELINE CONCLUÍDO COM SUCESSO")
    print("=" * 60 + "\n")
    
    print("Próximos passos:")
    print(f"  1. Verificar arquivos HTML em: {Path.cwd()}")
    print(f"  2. Verificar índices em: empresas_app_data/")
    print(f"  3. Fazer deploy de: {DIST_DIR}/")
    print()


if __name__ == "__main__":
    main()

