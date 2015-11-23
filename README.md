# PyImport
Projeto em python para importar dois arquivos em txt distintos, porém ambos irão para a mesma tabela.
A idéia é criar um daemon que irá verificar a cada N minutos se chegaram arquivos.

## Algoritmo

O processo vai verificar se tem arquivos para processar, se houver ele não processa na hora, pois pode ser que os arquivos ainda estejam sendo copiadas.
Se houver arquivos cria um arquivo com a lista dos nomes que estão disponíveis, e no próximo passo passa a importar apenas os nomes dentro da lista.
Depois de importar, tem que compactar e guardar em um histórico separado por ano, mes e dia.
As informações de data estarão disponíveis nos nomes dos arquivos.

## Pré requisitos
	[Python 2.7](http://wiki.python.org.br/) ou mais recente(não testei em versões anteriores pode ser que funcione)
	[MySQL](https://www.mysql.com/) ou outro banco
	[MySQLdb](http://sourceforge.net/projects/mysql-python/files/) no caso de usar MySQL


