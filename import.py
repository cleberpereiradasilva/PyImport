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


#			settings
################################################################
origem = "C:\\01Digital\\aes\\Import"
destino_historico = "C:\\01Digital\\aes\\Import\\Historico"
destino_logs = "C:\\01Digital\\aes\\Import\\logs"
host_database = "localhost"
user_database = "root"
pass_database =""
banco_database ="importador"
#prefixo usado nas querys de insert
prefixo_sql = "insert into temp_table_nova(origem,ds_0800,destino,situacao,OK, "
prefixo_sql += "NR, LO, CO, CO2, CO3, DSC, OU, data, data_banco,ano, mes,dia, hora,duracao,ddd, semana) values"
#numero de inserts por vez
buffer_insert = 100





################################################################



#como o proceso pode demorar entao vou criar um controlador
#caso exista o arquivo running entao nao faz nada...
running = origem + "\\running.dat"
#arquivo onde tera a lista dos files para importar
job_list = origem + "\\job.list"
dia_semana = ['Dom', 'Seg', 'Ter',  'Qua',  'Qui',  'Sex',  'Sáb']

def conectar():
	db = MySQLdb.connect(host=host_database, # your host, usually localhost
                     user=user_database, # your username
                      passwd=pass_database, # your password
                      db=banco_database) # name of the data base
	return db
def grava_log(mensagem):
	if not os.path.exists(destino_logs):
		os.makedirs(destino_logs)
	log_name = time.strftime("%Y_%m_%d_") +"log.txt"
	text_log_file = open(destino_logs+"\\" + log_name, "a")		
	text_log_file.write("[" + str(time.strftime("%Y-%m-%d %H:%M:%S")) + "]" + mensagem)	
	text_log_file.close()
	

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

def get_query(query):	
	db = conectar()	
	cursor = db.cursor()
	data=[]
	try:
		cursor.execute(query)
		data=cursor.fetchall()	
	finally:
		db.close()
	return data	
	
#os tres metodos abaixo estão de acordo com minha regra de negocio
#caso queira usasr o programa, acredito que seria aqui o lugar
#para aplicar sua regra...
#nao repara se parecer estranha... mas é um sistema legado que 
#irá receber esses dados	
def importar_compacto(arquivo):	
	with open(origem + "\\" + arquivo, 'r') as linhas:
		#prepara as variaveis
		sql_string=""
		n=0;
		total_registros = 0
		virgula=""
		erro_colunas = False
		for linha in linhas.readlines():			
			colunas = linha.split(";")
			total_registros += 1
			if len(colunas) == 15:				
				n +=1
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
				
				#data, apenas para exibir em um grafico... não sera computado como data
				sql_string += "'" +colunas[5][0:2] + "/" + colunas[5][3:5] + "/" + "20" + colunas[5][6:8]  + "',"

				#data_banco
				sql_string += "'20" + colunas[5][6:8] + "-" +colunas[5][3:5] + "-" + colunas[5][0:2] + "',"


				#campo ano, mes e dia
				sql_string += "'20" + colunas[5][6:8] + "',"
				sql_string += "'" +colunas[5][3:5]  + "',"
				sql_string += "'" +colunas[5][0:2] + "',"

				#hora com intervalo de 30 em 30
				
				if colunas[5][12:15] > '30':
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

				#aquela virguala basica so para ajeitar a query =D
				virgula=","

				#opa, da uma parada e faz o insert de acordo com 
				#o tamanho do buffer
				if n >= buffer_insert:		
					#print prefixo_sql+" "+sql_string
					executa_query(prefixo_sql+" "+sql_string)				
					sql_string=""
					n=0
					virgula=""
			else:
				erro_colunas=True


		if erro_colunas:
			grava_log("Arquivo " + arquivo + " com colunas incorretas. Eram esperadas 15 colunas.\n")

		#caso tenha sobrado alguma coisa para processar
		if sql_string != "":		
			executa_query(prefixo_sql+" "+sql_string)
		return total_registros

def importar_completo(arquivo):
	with open(origem + "\\" + arquivo, 'r') as linhas:
		#prepara as variaveis
		sql_string=""
		n=0;
		virgula=""
		total_registros = 0
		erro_colunas = False
		#tabela que tem os 0800 cadastrados... nao vamos querer pegar tudo...
		permitidos = get_query("select ds_0800 from banco_bp.tbl_grupo_0800 group by 1")
		for linha in linhas.readlines():
			colunas = linha.split("|")	
			total_registros += 1		
			if len(colunas) == 10:				
				n +=1							
				if any("0800" + colunas[1].split("0800")[1] in t_0800 for t_0800 in permitidos):

					sql_string += virgula + "("
					#origem da chamada
					sql_string += "'" + colunas[0] +"',"

					#0800 destino
					#este arquivo vem com operadoras antes do 0800 ex 015, 021....
					sql_string += "'0800" + colunas[1].split("0800")[1] +"',"

					#destino do 0800
					sql_string += "'" + colunas[2] +"',"

					#situacao/status da chamada
					sql_string += "'" + colunas[7] +"',"
					sql_string += "'" + ('1' if colunas[7]=="OK" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="NR" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="LO" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="CO" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="CO2" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="CO3" else '0') +"',"
					sql_string += "'" + ('1' if colunas[7]=="DSC" else '0') +"',"

					#o resto sera considerado como OU
					if not colunas[7] in ["OK", "NR", "LO", "CO", "CO2", "CO3", "DSC"] :
						sql_string += "'1',"
					else:
						sql_string += "'0',"				

					#data, apenas para exibir em um grafico... não sera computado como data
					sql_string += "'" +colunas[3][8:10] + "/" + colunas[3][5:7] + "/" + colunas[3][0:4] + "',"

					#data_banco
					sql_string += "'" +colunas[3] + "',"

					#campo ano, mes e dia
					sql_string += "'" + colunas[3][0:4] + "',"
					sql_string += "'" +colunas[3][5:7]  + "',"
					sql_string += "'" +colunas[3][8:10] + "',"

					#hora com intervalo de 30 em 30
					if colunas[4][3:5] > '30':
						intervalo = '30'
					else:
						intervalo = '00'
					
					sql_string += "'" + colunas[4][0:3]  + intervalo + "',"

					#tempo da ligacao
					sql_string += "'" + colunas[5] +"',"

					#dd chamante
					sql_string += "'" + colunas[6] +"',"

					#dia da semana
					sql_string += "'" + dia_semana[date(int(colunas[3][0:4]) , int(colunas[3][5:7]) , int(colunas[3][8:10])).weekday()] +"'"
					
					sql_string += ")"

					#aquela virguala basica so para ajeitar a query =D
					virgula=","

					#opa, da uma parada e faz o insert de acordo com 
					#o tamanho do buffer
					if n >= buffer_insert:		
							
						executa_query(prefixo_sql+" "+sql_string)
						#reseta as variaveis							
						sql_string=""
						n=0
						virgula=""
			else:
				erro_colunas=True

		if erro_colunas:
			grava_log("Arquivo " + arquivo + " com colunas incorretas. Eram esperadas 10 colunas.\n")

		#caso tenha sobrado alguma coisa para processar
		if sql_string != "":						
			executa_query(prefixo_sql+" "+sql_string)
		return total_registros

def post_processar():
	#para evitar alguma catastrofe eu movo os dados para uma tabela
	#pego os dados que serao substituidos e os guardo
	#esses dados sao os 0800xxxx e o dia dele, ja que os arquivos sao diarios

	#query para pegar os 0800 e dias q serao afetados
	aux = get_query("select ds_0800, data_banco from temp_table_nova group by 1")

	#query que irá mover para a tabela segura
	prefixo_sql_post =  "insert into banco_bp.tbl_0800_calculado_removido(DS_0800,ANO,MES,SEMANA,DATA,DIA,"
	prefixo_sql_post += "HORA,DATA_HORA,TOTAL,OK,NR,LO,DSC,CO,OU,DURACAO,ATIVO,origem,CIDADE,DDD) "
	prefixo_sql_post += " select A.* FROM banco_bp.tbl_0800_calculado A where concat(ds_0800,'_',data_hora) in "

	#query que ira remover os dados para serem substituidos, depois de serem salvos...
	prefixo_sql_delete =  "delete from banco_bp.tbl_0800_calculado where concat(ds_0800,'_',data_hora) in "

	#query q ira dar carga em producao
	query_post_processo = "insert into banco_bp.TBL_0800_CALCULADO (DS_0800,DATA,data_hora,ANO,MES,DIA,HORA,DDD,SEMANA,TOTAL,OK,NR,LO,DSC,CO,OU,DURACAO) "
	query_post_processo += "select ds_0800, data, data_banco, ano, mes, dia, hora, ddd, semana, SUM(1) AS TOTAL, SUM(OK) AS OK, SUM(NR) AS NR, "
	query_post_processo += "SUM(LO) AS LO, SUM(DSC) as DSC, (SUM(CO2)+SUM(CO3)) as CO, SUM(OU) as OU,SUM(DURACAO) as DURACAO "
	query_post_processo += "from temp_table_nova group by 1,2,3,4,5,6,7,8,9 "
	
	sql_string=""
	n=0;
	virgula=""
	for item in aux:
		n +=1
		sql_string += virgula + "'" + item[0]+"_"+item[1] +"'"
		virgula=","
		if n >= 10:
			executa_query(prefixo_sql_post + "(" + sql_string +")")
			executa_query(prefixo_sql_delete + "(" + sql_string +")")
			sql_string=""
			n=0
			virgula=""
	if sql_string != "":
		executa_query(prefixo_sql_post + "(" + sql_string +")")
		executa_query(prefixo_sql_delete + "(" + sql_string +")")
	executa_query(query_post_processo)		

	#limpar a tabela de importacao
	executa_query("truncate table temp_table_nova")

def descompactar(arquivo):
	with zipfile.ZipFile(origem + "\\" + arquivo, "r") as z:
		z.extractall(origem)
	os.remove(origem + "\\" + arquivo)
	
def compactar(arquivo):
	#cria o nome do zip
	#gosto de deixar o timestamp no comeco pq pode ser
	#que o cliente envie duas vezes o mesmo arquivo
	#na pratica um vai anular o outro, mas vao ficar os dois no
	#historico...
	prefixo = arquivo.lower().replace(".txt","_") + time.strftime("%Y_%m_%d_%H_%M_%S") + '.zip'
	zout = zipfile.ZipFile(origem + "\\" + prefixo, "w", zipfile.ZIP_DEFLATED) # <--- this is the change you need to make
	zout.write(origem + "\\" + arquivo,os.path.basename(origem + "\\" + arquivo))
	zout.close()

	#devolve o nome do zip
	return prefixo

def historico(arquivo):
	#compacta e recebe o nome do arquivo.zip 
	#exemplo 20151123-161924_0800809494_20151114_sac.txt.zip
	nome_zip = compactar(arquivo)

	#os nomes podem estar assim:
	#Consulta_AES_CDRONE_SP01_20150905230000.txt
	#0800809494_20151115_sac.txt
	#entao separo por _
	partes_nome = arquivo.split('_')
	parte_data = 0

	#cada um tem a data em um lugar diferente
	if partes_nome[0].isdigit():
		#0800809494_20151115_sac.txt
		parte_data = 1
	else:
		#Consulta_AES_CDRONE_SP01_20150905230000.txt
		parte_data = 4

	#faz o caminho para guardar organizado em ano/mes/dia
	salvar_em = destino_historico+"\\" + partes_nome[parte_data][0:4]+""+partes_nome[parte_data][4:6]+""+partes_nome[parte_data][6:8]
	
	#cria o diretorio caso nao exista
	if not os.path.exists(salvar_em):
		os.makedirs(salvar_em)

	#move o zipado e remove o original
	os.rename(origem + "\\" + nome_zip, salvar_em + "\\" + nome_zip)
	os.remove(origem + "\\" + arquivo)




#primeiro verifica se já esta rodando...
#pode ser que demore o processo
if not os.path.exists(running):
	try:		
		executa_query("truncate table temp_table_nova")				
		file_running = open(running,'w')  
		file_running.close()

		#verificar se tem uma lista com os arquivos para ler
		if os.path.exists(job_list):			
			with open(job_list, 'r') as linhas:
				for linha in linhas.readlines():
					linha=linha.replace("\n","")
					if len(linha) > 5:
						grava_log("Inicio do processamento do arquivo " + linha +".\n")					
						#se houver zip, apenas vou descompactar
						#como nao sei oq vai sair de dentro
						#no proximo passo procura os txt
						retorno_acao=""

						if linha.upper().endswith('.ZIP'):
							retorno_acao="[Descompactado]"
							descompactar(linha)					

						#se tiver txt ja importa....
						
						if linha.upper().endswith('.TXT'):
							inicio_name = (linha + "_").split(' ', 1 )[0].split("_",1)[0]									
							if inicio_name.isdigit():
								#0800809494_20151115_sac.txt
								retorno_acao = "[Linhas " + str(importar_compacto(linha)) + "]"
							else:
								#Consulta_AES_CDRONE_SP01_20150905230000.txt
								retorno_acao = "[Linhas " + str(importar_completo(linha)) + "]"
							historico(linha)
						#faz carga em producao
						#limpa as tabelas usadas no processo
						#roda arquivo por arquivo para ficar mais leve de trabalhar com 
						#tabelas menores....
						post_processar()
						grava_log("Fim do processamento do arquivo " + linha +". "+retorno_acao+"\n\n")	
			
			
			
	finally:	
		#limpa a lista de jobs, tudo feito!
		if os.path.exists(job_list):
			os.remove(job_list)

		#remove o running e fica pronto para executar de novo
		if os.path.exists(running):
			os.remove(running)

	#por fim verifica se tem novos arquivos para o proximo step
	if not os.path.exists(job_list):
		#recebe a lista de txts
		get_txt_list = [f for f in os.listdir(origem) if f.upper().endswith('.TXT')]
		#recebe a lista de zips
		get_zip_list = [f for f in os.listdir(origem) if f.upper().endswith('.ZIP')]
		#pode ser que tenhamos zips e txt ao mesmo tempo.


		if len(get_txt_list) + len(get_zip_list) > 0:
			grava_log("Arquivos encontrados para processamento.\n")	
			text_file = open(job_list, "w")		
	  		text_file.write("\n".join(get_txt_list) + "\n")
	  		text_file.write("\n".join(get_zip_list) + "\n")
			text_file.close()
	print "Fim "