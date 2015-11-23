# encoding=iso-8859-1  
import sys  
reload(sys)  
sys.setdefaultencoding('iso-8859-1')

import os 
import time
import zipfile

#MySQLdb http://sourceforge.net/projects/mysql-python/files/
import MySQLdb
from datetime import date


origem = "C:\\01Digital\\aes\\Import"
destino_historico = "C:\\01Digital\\aes\\Import\\Historico"

#como o proceso pode demorar entao vou criar um controlador
#caso exista o arquivo running entao nao faz nada...
running = origem + "\\running.dat"

#arquivo onde tera a lista dos files para importar
job_list = origem + "\\job.list"


#numero de inserts por vez
buffer_insert = 0

dia_semana = ['Dom', 'Seg', 'Ter',  'Qua',  'Qui',  'Sex',  'Sáb']

#prefixo usado nas querys de insert
prefixo_sql = "insert into temp_table_nova(origem,ds_0800,destino,situacao,OK, "
prefixo_sql += "NR, LO, CO, CO2, CO3, DSC, OU, data,ano, mes,dia, hora,duracao,ddd, semana) values"

def conectar():
	db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="root", # your username
                      passwd="", # your password
                      db="importador") # name of the data base
	return db
def executa_query(query):
	db = conectar()	
	cursor = db.cursor()
	try:
		cursor.execute(query)
		db.commit()
	except: 
		db.rollback()
	finally:
		db.close()

	
def importar_compato(arquivo):
	
	with open(origem + "\\" + arquivo, 'r') as linhas:
		sql_string=""
		n=0;
		virgula=""
		for linha in linhas.readlines():
			n +=1
			colunas = linha.split(";")
			sql_string += virgula + "("
			#origem da chamada
			sql_string += "'" + colunas[0] +"',"

			#0800 destino
			sql_string += "'" + colunas[1] +"',"

			#destino do 0800
			sql_string += "'" + colunas[2] +"',"

			#situacao/status da chamada
			sql_string += "'" + colunas[4] +"',"
			sql_string += "'" + ('1' if colunas[4]=="OK" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="NR" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="LO" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="CO" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="CO2" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="CO3" else '0') +"',"
			sql_string += "'" + ('1' if colunas[4]=="DSC" else '0') +"',"

			#o resto sera considerado como OU
			if not colunas[4] in ["OK", "NR", "LO", "CO", "CO2", "CO3", "DSC"] :
				sql_string += "'1',"
			else:
				sql_string += "'0',"
			
			#data da chamada
			sql_string += "'20" + colunas[5][6:8] + "-" +colunas[5][3:5] + "-" + colunas[5][0:2] + "',"

			#campo ano, mes e dia
			sql_string += "'20" + colunas[5][6:8] + "',"
			sql_string += "'" +colunas[5][3:5]  + "',"
			sql_string += "'" +colunas[5][0:2] + "',"

			#hora com intervalo de 30 em 30
			if colunas[5][13:15] > 30:
				intervalo = '30'
			else:
				intervalo = '00'
			sql_string += "'" + colunas[5][9:12]  + intervalo + "',"

			#tempo da ligacao
			sql_string += "'" + colunas[6] +"',"

			#dd chamante
			sql_string += "'" + colunas[8] +"',"

			#dia da semana
			sql_string += "'" + dia_semana[date(int("20" + colunas[5][6:8]) , int(colunas[5][3:5]) , int(colunas[5][0:2])).weekday()] +"'"
			
			sql_string += ")"
			virgula=","
			if n >= buffer_insert:				
				executa_query(prefixo_sql+" "+sql_string)				
				sql_string=""
				n=0
				virgula=""

		#caso tenha sobrado alguma coisa para processar
		if n >=0:					
			executa_query(prefixo_sql+" "+sql_string)



def importar_completo(arquivo):
	print "Importando arquivo completo..."


def descompactar(arquivo):
	with zipfile.ZipFile(origem + "\\" + arquivo, "r") as z:
		z.extractall(origem)
	os.remove(origem + "\\" + arquivo)
	
def compactar(arquivo):
	prefixo = time.strftime("%Y%m%d-%H%M%S") + "_" + arquivo + '.zip'
	zout = zipfile.ZipFile(origem + "\\" + prefixo, "w", zipfile.ZIP_DEFLATED) # <--- this is the change you need to make
	zout.write(origem + "\\" + arquivo,os.path.basename(origem + "\\" + arquivo))
	zout.close()
	return prefixo


def historico(arquivo):
	nome_zip = compactar(arquivo)
	partes_nome = arquivo.split('_')
	parte_data = 0
	if partes_nome[0].isdigit():
		parte_data = 1
	else:
		parte_data = 4
	salvar_em = destino_historico+"\\" + partes_nome[parte_data][0:4]+"\\"+partes_nome[parte_data][4:6]+"\\"+partes_nome[parte_data][6:8]
	if not os.path.exists(salvar_em):
		os.makedirs(salvar_em)
	#os.rename(origem + "\\" + nome_zip, salvar_em + "\\" + nome_zip)
	#os.remove(origem + "\\" + arquivo)
	

if not os.path.exists(running):
	try:
		file_running = open(running,'w')  
		file_running.close()

		#verificar se tem uma lista com os arquivos para ler
		if os.path.exists(job_list):	
			
			with open(job_list, 'r') as linhas:
				for linha in linhas.readlines():
					linha=linha.replace("\n","")
					#se houver zip, apenas vou descompactar
					#como nao sei oq vai sair de dentro
					#no proximo passo procura os txt
					if linha.endswith('.zip'):
						descompactar(linha)					

					#se tiver txt ja importa....
					if linha.endswith('.txt'):
						inicio_name = (linha + "_").split(' ', 1 )[0].split("_",1)[0]
						if inicio_name.isdigit():
							importar_compato(linha)
						else:
							importar_completo(linha)
						#historico(linha)
			

			os.remove(job_list)
	finally:		
		#remove o running e fica pronto para executar de novo
		os.remove(running)

	#por fim verifica se tem novos arquivos para o proximo step
	if not os.path.exists(job_list):
		get_txt_list = [f for f in os.listdir(origem) if f.endswith('.txt')]
		get_zip_list = [f for f in os.listdir(origem) if f.endswith('.zip')]
		if len(get_txt_list) + len(get_zip_list) > 0:
			text_file = open(job_list, "w")		
	  		text_file.write("\n".join(get_txt_list) + "\n")
	  		text_file.write("\n".join(get_zip_list) + "\n")
			text_file.close()