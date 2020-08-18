# Calcula Qualis CC (Conferências e Periódicos)

## Script para as Conferências (scriptPythonConfs-cp.py)
- Script python utilizado para automatização (busca dos h5 no Google Scholar, via `web scrapping`) e que calcula o valor do Novo Qualis das Conferências conforme regras do Documento de Área (Ciência da Computação) da CAPES.

- O Script foi configurado para executar todos os dias (de segunda a domingo) à meia-noite (horário de Brasília (3am EUA)) e está hospedado no Heroku.

- Este script é utilizado para apresentar a tabela do Qualis CC em relação às Conferências, que consta no site `Discentes PPGCC/PUCRS` e que pode ser visualizada por: [Qualis CC](https://ppgcc.github.io/discentesPPGCC/pt-BR/qualis/)

## Script para os Periódicos (scriptPythonPeri-cp.py)
- Script python utilizado para automatização (busca dos percentis na Scopus, via API da própria Scopus) e que calcula o valor do Novo Qualis para os Periódicos conforme regras do Documento de Área (Ciência da Computação) da CAPES.

- O Script foi configurado para executar todos os dias (de segunda a domingo) à meia-noite (horário de Brasília (3am EUA)) e está hospedado no Heroku.

- Este script é utilizado para apresentar a tabela do Qualis CC em relação aos Periódicos, que consta no site `Discentes PPGCC/PUCRS` e que pode ser visualizada por: [Qualis CC](https://ppgcc.github.io/discentesPPGCC/pt-BR/qualis/)

- **OBS:** _Informações relacionadas às credenciais foram omitidas neste repositório, por questões de segurança relacionadas à conta do Google. No entanto, para fazer uso deste script, basta apenas gerar suas próprias credenciais._

### *Implementado por [Olimar Teixeira Borges](https://github.com/olimarborges).
