import os 
import time
import gzip

origem = "C:\\01Digital\\aes\\Import"
destino_historico = "C:\\01Digital\\aes\\Import\\Historico"

#como o proceso pode demorar entao vou criar um controlador
#caso exista o arquivo running entao nao faz nada...
running = origem + "\\running.dat"

#arquivo onde tera a lista dos files para importar
job_list = origem + "\\job.list"
	
def importar_compato(arquivo):
	print "Importando arquivo compacto..."
def importar_completo(arquivo):
	print "Importando arquivo completo..."
def descompactar(arquivo):
	print "Descompactando arquivo..."
def compactar(arquivo):
	prefixo = time.strftime("%Y%m%d-%H%M%S") + "_" + arquivo + '.gz'
	f_in = open(origem + "\\" + arquivo, 'rb')
	f_out = gzip.open(origem + "\\" + prefixo, 'wb')
	f_out.writelines(f_in)
	f_out.close()
	f_in.close()
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
	os.rename(origem + "\\" + nome_zip, salvar_em + "\\" + nome_zip)
	
	

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
						historico(linha)
			

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