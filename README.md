# PyImport

Projeto em python para importar dois arquivos em txt distintos, porém ambos irão para a mesma tabela.
A idéia é criar um daemon que irá verificar a cada N minutos se chegaram arquivos.

## Algoritmo

O processo vai verificar se tem arquivos para processar, se houver ele não processa na hora, pois pode ser que os arquivos ainda estejam sendo copiadas.
Se houver arquivos cria um arquivo com a lista dos nomes que estão disponíveis, e no próximo passo passa a importar apenas os nomes dentro da lista.
Depois de importar, tem que compactar e guardar em um histórico separado por ano, mes e dia.
As informações de data estarão disponíveis nos nomes dos arquivos.

## Status
	[Stable] testado com aproximadamente 1.000.000 de registros com diversos arquivos txt e zip.
	Levou menos de 110s para realizar as importações e os demais processos.
	Inidicadores dentro do esperado

## Pré requisitos
	[Python 2.7](http://www.python.org.br/) ou mais recente(não testei em versões anteriores)
	[MySQL](https://www.mysql.com/) ou outro banco
	[MySQLdb](http://sourceforge.net/projects/mysql-python/files/) no caso de usar MySQL

## Como Rodar
	Configure as linhas 16 até 30 acordo com seus dados	
	Crie as tabelas e o banco de dados conforme o arquivo sql.txt

## Licença(CC)

	You are free to:
	    Share — copy and redistribute the material in any medium or format
	    Adapt — remix, transform, and build upon the material
	    for any purpose, even commercially.
	    The licensor cannot revoke these freedoms as long as you follow the license terms.

## Autor
	Cleber Silva
	cleber_pesilva@yahoo.com.br

## Ano
	2015