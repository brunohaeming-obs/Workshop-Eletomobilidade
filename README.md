# Workshop Eletromobilidade

Projeto de analise e visualizacao de empresas, NCMs, componentes e grupos tecnologicos da cadeia de eletromobilidade no Sul do Brasil.

O repositorio combina um pipeline em Python, dados de entrada em `dados/`, indices JSON para consulta sob demanda e visualizacoes HTML/SVG para uso em aplicacao web e apresentacoes.

## Estrutura

```text
.
|-- analysis.py                         # modulo principal legado: carga, tratamento, payloads e templates HTML
|-- main_analysis.py                    # orquestrador do pipeline atual
|-- config.py                           # caminhos e constantes globais
|-- loaders.py                          # leitura de bases de empresas, municipios, NCM e componentes
|-- enrichers.py                        # enriquecimentos, como vinculo com patentes
|-- writers.py                          # escrita de indices JSON
|-- htmlgen.py                          # wrappers para geracao de HTML/payloads
|-- formatters.py / utils.py            # utilitarios de apoio
|-- dados/                              # bases brutas e planilhas de referencia
|-- empresas_app_data/                  # indices JSON gerados para carregamento sob demanda
|-- dist/                               # pacote pronto para publicacao
|-- analise_descritiva/                 # relatorios, slides e scripts de visualizacao executiva
|-- visualizacao_empresas_rfb.html      # visualizacao local principal de empresas
|-- visualizacao_ncm_empresas.html      # visualizacao local NCM x empresas
|-- visualizacao_componentes.html       # visualizacao local da cadeia GT/NCM/componentes/CNPJs
`-- DEPLOY.md                           # instrucoes de deploy
```

## Dados

As principais entradas ficam em `dados/`:

- `empresas_rfb_databricks.xlsx`: base empresarial usada no pipeline.
- `GT_NCM_Dados_Brutos.xlsx` e `GT_NCM_Dados_Brutos_V2.xlsx`: mapeamento de GT, componentes, NCMs, criticidade e complexidade.
- `INPI - patentes_depositantes_ipc.xlsx`: vinculo de CNPJs com patentes.
- `municipios_sul_geobr_2020.geojson`: malha municipal do Sul.
- arquivos auxiliares de CNAE, NCM e comercio exterior.

O arquivo `dados/empresas_rfb_2.csv` fica ignorado por ser grande/local.

## Pipeline

Uso principal:

```powershell
python main_analysis.py
```

Flags disponiveis:

```powershell
python main_analysis.py --load-empresas
python main_analysis.py --write-indices
python main_analysis.py --write-html
python main_analysis.py --create-deploy
```

O pipeline:

1. carrega e enriquece empresas;
2. gera indices em `empresas_app_data/`;
3. gera HTMLs locais na raiz;
4. cria o pacote publicavel em `dist/`.

## Aplicacao

Visualizacoes locais:

- `visualizacao_empresas_rfb.html`: mapa e filtros de empresas.
- `visualizacao_ncm_empresas.html`: exploracao por NCM.
- `visualizacao_componentes.html`: cadeia GT, NCM, componentes, municipios e CNPJs.

Pacote de deploy:

- `dist/index.html`
- `dist/ncm.html`
- `dist/componentes.html`
- `dist/assets/data/*.json`
- `dist/empresas_app_data/`

Para testar o pacote:

```powershell
cd dist
python -m http.server 8000
```

Acesse `http://localhost:8000/`.

## Analise Descritiva

A pasta `analise_descritiva/` concentra artefatos executivos:

- `dados_resumo/`: CSVs agregados usados nos slides e relatorios.
- `gerar_slides_adensamento_cadeia.py`: slides de base industrial e adensamento por GT.
- `gerar_sankey_cadeia_gt.py`: sankeys da cadeia por GT, incluindo versoes ponderadas.
- `gerar_slide_componentes_empresas.py`: slides por GT com componentes e empresas destaque.
- `gerar_matriz_criticidade_complexidade.py`: matriz de criticidade x complexidade por subcomponente.
- `*.svg` e `*.html`: imagens e previews para apresentacao.

## Deploy

Veja [DEPLOY.md](DEPLOY.md). Em resumo, o deploy deve apontar para `dist/`, nao para a raiz do projeto, pois a raiz contem dados brutos e arquivos intermediarios.

```powershell
vercel dist --prod
```

## Versionamento

O `.gitignore` cobre ambientes locais, caches Python, logs, temporarios, o CSV bruto grande e backups experimentais de visualizacoes. Alguns artefatos gerados, como `dist/` e `empresas_app_data/`, podem aparecer no Git porque fazem parte do pacote de publicacao e consulta da aplicacao.
