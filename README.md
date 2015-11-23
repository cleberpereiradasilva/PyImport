# PyImport
Projeto em python para importar dois arquivos em txt distintos, porém ambos irão para a mesma tabela.
A idéia é criar um daemon que ira verificar a cada N minutyos se chegaram arquivos.

#Algoritmo

O processo vai verificar se tem arquivos para processar, se houver ele não processa na hora, pois pode ser que os arquivos ainda estejam sendo copiadas.
Se houver arquivos cria um arquivo com a lista dos nomes que estão disponíveis, e no próximo passo passa a importar apenas os nomes dentro da lista.
Depois de importar, tem que compactar e guardar em um histórico separado por ano, mes e dia.
As informações de data estarão disponíveis nos nomes dos arquivos.


