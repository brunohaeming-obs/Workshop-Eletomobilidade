# Deploy da ferramenta

O pacote pronto para publicação fica na pasta `dist`.

## Teste local

```powershell
cd "C:\Users\bruno.haeming\Desktop\Workshop Eletomobilidade\dist"
python -m http.server 8000
```

Acesse:

```text
http://localhost:8000/
```

## Deploy no Vercel

Execute o deploy apontando diretamente para `dist`:

```powershell
cd "C:\Users\bruno.haeming\Desktop\Workshop Eletomobilidade"
vercel dist
```

Para produção:

```powershell
vercel dist --prod
```

## Arquivos publicados

- `dist/index.html`: visualização principal Empresas, com filtro NCM integrado.
- `dist/ncm.html`: visualização auxiliar NCM x Empresas.
- `dist/assets/data/*.json`: dados agregados carregados pela aplicação.
- `dist/empresas_app_data/municipios/*.json`: detalhes por município carregados sob demanda.

Não faça deploy da raiz do projeto, pois ela contém as planilhas originais em `dados/`.
