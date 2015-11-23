# PyImport
Projeto em python para importar dois arquivos em txt distintos, porém ambos irão para a mesma tabela.
A idéia é criar um daemon que irá verificar a cada N minutos se chegaram arquivos.

## Algoritmo

O processo vai verificar se tem arquivos para processar, se houver ele não processa na hora, pois pode ser que os arquivos ainda estejam sendo copiadas.
Se houver arquivos cria um arquivo com a lista dos nomes que estão disponíveis, e no próximo passo passa a importar apenas os nomes dentro da lista.
Depois de importar, tem que compactar e guardar em um histórico separado por ano, mes e dia.
As informações de data estarão disponíveis nos nomes dos arquivos.

## Pré requisitos
	[Python 2.7](http://www.python.org.br/) ou mais recente(não testei em versões anteriores)
	[MySQL](https://www.mysql.com/) ou outro banco
	[MySQLdb](http://sourceforge.net/projects/mysql-python/files/) no caso de usar MySQL

## Como Rodar
	Configure as linhas 15 e 16 de acordo com seu path
	ex:
		origem = "C:\\01Digital\\aes\\Import"
		destino_historico = "C:\\01Digital\\aes\\Import\\Historico"
	Configure os dados do banco de dados nas linhas 35 até 39
	Coloque para rodar no agendador de tarefas do Windows

## Licença(CC)

You are free to:
    Share — copy and redistribute the material in any medium or format
    Adapt — remix, transform, and build upon the material
    for any purpose, even commercially.
    The licensor cannot revoke these freedoms as long as you follow the license terms.




