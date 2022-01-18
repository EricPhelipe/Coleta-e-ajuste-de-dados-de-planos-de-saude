# -*- coding: utf-8 -*-
"""
Created on Sat Jan 15 20:15:19 2022

@author: ericp
"""
import pandas as pd
import os 
import zipfile
import requests, io
from bs4 import BeautifulSoup
import seaborn as sns
import matplotlib.pyplot as plt
import time
pd.set_option('display.max_columns', 31)
pd.set_option('display.max_rows', 50)

# Escolhendo um diretório específico para salvar todos os arquivos
os.chdir('seu diretório')
os.listdir()
url = 'http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/2019/'

# Criando uma lista com as iniciais de todos os estados
lista1 = []
soup = BeautifulSoup(requests.get(url).text, 'html.parser')
for i in soup.findAll('a', href=True):
    j = i['href']
    if '/' in j[-3:]:
        lista1.append(j[-3:])
lista1 = lista1[1:]   

# Criano uma lista com as urls de todos os estados
urls = []
def fun(arg):
    x = 'http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/2019/'
    for i in lista1:
        xy = x + str(i)
        urls.append(xy)
fun(lista1)

# Criando duas lista, uma com os links de cada arquivo zip e outra com 
# o nome dos arquivos zip
lista2 = []
lista3 = []
for url in urls:
    soup1 = BeautifulSoup(requests.get(url).text, 'html.parser')
    for i in soup1.findAll('a', href=True):
        s = i['href']
        print(s)
        if '.zip' in s:
            lista2.append(url + s)
            lista3.append(s)

# Extraindo todos os arquivos zip para a pasta escolhida inicialmente          
for url in lista2:
    ext = zipfile.ZipFile(io.BytesIO(requests.get(url).content))
    ext.extractall()    

# Existem dois tipos de arquivos para cada mês em cada estado, 
# aqui esses arquivos foram separados em duas listas por cada tipo 
lista4 = []
lista5 = []
for i in os.listdir():
    if 'CONS' in i:
        table = pd.read_csv(i, sep=';')
        time.sleep(2)
        lista4.append(table)
    elif 'DET' in i:
        table1 = pd.read_csv(i, sep=';')
        time.sleep(2)
        lista5.append(table1)

# Concatenando todos os arquivos de cada tipo em duas tabelas diferentes
tabela1 = pd.concat(lista4[0:], axis=0)
tabela2 = pd.concat(lista5[0:], axis=0)

# Salvando as duas tabelas em pickle (opcional e não recomendado) 
#tabela1.to_pickle('tabelaCONS.pkl')
#tabela2.to_pickle('tabelaDET.pkl')

# Juntando as duas tabelas
tabela = pd.merge(tabela2, tabela1, how='left', on=['ID_EVENTO_ATENCAO_SAUDE'])

# Removendo colunas duplicadas pelo merge
tabela.drop(['UF_PRESTADOR_y', 'TEMPO_DE_PERMANENCIA_y', 'ANO_MES_EVENTO_y'], axis=1, inplace=True)

# Verificando a presença de linhas duplicadas na tabela principal
# (foram retiradas 275858 linhas duplicadas)
dropdup = tabela.columns
tabela = tabela.drop_duplicates(subset = dropdup, keep = 'first')

# Separando a colunas de ano-mês em duas e ajustando nomes modificados pelo merge
tabela['Ano'], tabela['Mes'] = tabela['ANO_MES_EVENTO_x'].str.split('-', 1).str
tabela['Mes'] =tabela['Mes'].astype(int)
tabela.columns = ['ID_EVENTO_ATENCAO_SAUDE', 'UF_PRESTADOR', 'TEMPO_DE_PERMANENCIA',
       'ANO_MES_EVENTO', 'CD_PROCEDIMENTO', 'CD_TABELA_REFERENCIA',
       'QT_ITEM_EVENTO_INFORMADO', 'VL_ITEM_EVENTO_INFORMADO',
       'VL_ITEM_PAGO_FORNECEDOR', 'IND_PACOTE', 'IND_TABELA_PROPRIA',
       'ID_PLANO', 'FAIXA_ETARIA', 'SEXO', 'CD_MUNICIPIO_BENEFICIARIO',
       'PORTE', 'CD_MODALIDADE', 'NM_MODALIDADE', 'CD_MUNICIPIO_PRESTADOR',
       'CD_CARATER_ATENDIMENTO', 'CD_TIPO_INTERNACAO', 'CD_REGIME_INTERNACAO',
       'CD_MOTIVO_SAIDA', 'CID_1', 'CID_2', 'CID_3', 'CID_4',
       'QT_DIARIA_ACOMPANHANTE', 'QT_DIARIA_UTI', 'IND_ACIDENTE_DOENCA',
       'LG_VALOR_PREESTABELECIDO', 'ANO', 'MES']

# Ajustando colunas que tiveram números registrados com a separação por vírgula,
# para que possam ser lidos como números 
tabela['VL_ITEM_EVENTO_INFORMADO'] = tabela['VL_ITEM_EVENTO_INFORMADO'].astype(str)
tabela['VL_ITEM_EVENTO_INFORMADO'] = tabela['VL_ITEM_EVENTO_INFORMADO'].str.replace(',', '.', regex=True)
tabela['VL_ITEM_EVENTO_INFORMADO'] = tabela['VL_ITEM_EVENTO_INFORMADO'].astype(float)

tabela['QT_ITEM_EVENTO_INFORMADO'] = tabela['QT_ITEM_EVENTO_INFORMADO'].astype(str)
tabela['QT_ITEM_EVENTO_INFORMADO'] = tabela['QT_ITEM_EVENTO_INFORMADO'].str.replace(',', '.', regex=True)
tabela['QT_ITEM_EVENTO_INFORMADO'] = tabela['QT_ITEM_EVENTO_INFORMADO'].astype(float)

tabela['VL_ITEM_PAGO_FORNECEDOR'] = tabela['VL_ITEM_PAGO_FORNECEDOR'].astype(str)
tabela['VL_ITEM_PAGO_FORNECEDOR'] = tabela['VL_ITEM_PAGO_FORNECEDOR'].str.replace(',', '.', regex=True)
tabela['VL_ITEM_PAGO_FORNECEDOR'] = tabela['VL_ITEM_PAGO_FORNECEDOR'].astype(float)
tabela['VL_ITEM_PAGO_FORNECEDOR'].describe()
tabela.columns
# Retirando a coluna "ano", já que todas as observações são para 2019
tabela.drop(['ANO'], axis=1, inplace=True)

# Criando duas features de negócio 
# A primeira consiste em calcular a diferença entre entre o 
# código do município de residência do beneficiário e o código
# do município do prestador executante, obtendo zero caso o beneficiário
# seja atendido em seu município de residência e transformando todos os outros 
# números em 1, para caso ele seja atendido fora de seu município de residência   
tabela['DIFERENCA_LOCAL'] = tabela['CD_MUNICIPIO_BENEFICIARIO'] - tabela['CD_MUNICIPIO_PRESTADOR']
def func1(arg1):
    if '1' in arg1:
        return 1
    elif '2' in arg1:
        return 1
    elif '3' in arg1:
        return 1
    elif '4' in arg1:
        return 1
    elif '5' in arg1:
        return 1
    elif '6' in arg1:
        return 1
    elif '7' in arg1:
        return 1
    elif '8' in arg1:
        return 1
    elif '9' in arg1:
        return 1
    else:
        return arg1
tabela['DIFERENCA_LOCAL'] = tabela['DIFERENCA_LOCAL'].astype(str)
tabela['DIFERENCA_LOCAL'] = tabela['DIFERENCA_LOCAL'].map(func1)                                
tabela['DIFERENCA_LOCAL'] = tabela['DIFERENCA_LOCAL'].astype(float)      
tabela['DIFERENCA_LOCAL'].describe()
tabela['DIFERENCA_LOCAL'].value_counts()
tabela['DIFERENCA_LOCAL'].isna().sum()

# A segunda feature de negócio consiste em criar variáveis dummy 
# (variáveis binárias, 0 ou 1) para cada intervalo de faixa etária
dummies = pd.get_dummies(tabela['FAIXA_ETARIA'])
dummies.columns
f = dummies.columns.tolist()
lista10 = []
for i in f:
    i = 'IDADE_' + i.replace(' ', '_')
    lista10.append(i)
dummies.columns = lista10
tabela = pd.concat([tabela, dummies], axis=1)

# Salvamento da base completa (opcional e não recomendado)
#tabela.to_pickle('tabela.pkl')

# Criando lista para fazer tabelas
lista6 = []
lista7 = []
b = tabela.columns
for i in b:
    # Preenchendo lista com o somatório de valores faltantes
    x = int(tabela[i].isna().sum())
    lista6.append(x)
    # Preenchendo lista com estatísticas das colunas
    a = tabela[i].describe()
    lista7.append(a)
    # Verificando  estatísticas das colunas
    print(tabela[i].describe())
    # Verificando a contagem de valores de cada coluna
    print(tabela[i].value_counts().sort_values( ascending=False))
    # Verificando os valores faltantes em cada coluna
    print( 'Valores faltantes de ' + str(i) + ' = '   +  str(int(tabela[i].isna().sum())))

# Criando DataFrame para fazer tabela de valores faltantes
aed = pd.DataFrame(lista6).reset_index()
aed.columns
aed['index'] = b
aed.columns = ['Variaveis','Valores faltantes']
aed['Percentual de Valores faltantes'] = (aed['Valores faltantes'] / len(tabela))*100
aed.to_excel('tabelaemissingv.xlsx')

# Criando DataFrame para fazer tabela com estatísticas das colunas
c = pd.concat(lista7[0:], axis=1)
c.columns
d = c[['TEMPO_DE_PERMANENCIA','CD_TABELA_REFERENCIA',
      'QT_ITEM_EVENTO_INFORMADO', 'VL_ITEM_EVENTO_INFORMADO', 'VL_ITEM_PAGO_FORNECEDOR', 'IND_PACOTE', 'IND_TABELA_PROPRIA',
      'ID_PLANO','CD_MUNICIPIO_BENEFICIARIO', 'CD_MODALIDADE', 'CD_MUNICIPIO_PRESTADOR',
      'CD_CARATER_ATENDIMENTO','CD_TIPO_INTERNACAO', 'CD_REGIME_INTERNACAO',
      'CD_MOTIVO_SAIDA','QT_DIARIA_ACOMPANHANTE', 'QT_DIARIA_UTI', 'IND_ACIDENTE_DOENCA',
      'LG_VALOR_PREESTABELECIDO']][1:8]
#d.to_excel('tabelaestatist.xlsx')

# Criando tabelas de contagem para verificar possíveis outliers
lista8 = []
dici = {}
dici['Maior ou igual a 1 milhão'] = tabela['VL_ITEM_EVENTO_INFORMADO'].loc[tabela['VL_ITEM_EVENTO_INFORMADO'] >= 1000000].count()
dici['Maior ou igual a 5 milhões'] = tabela['VL_ITEM_EVENTO_INFORMADO'].loc[tabela['VL_ITEM_EVENTO_INFORMADO'] >= 5000000].count()
dici['Maior ou igual a 10 milhões'] = tabela['VL_ITEM_EVENTO_INFORMADO'].loc[tabela['VL_ITEM_EVENTO_INFORMADO'] >= 10000000].count()
dici['Maior ou igual a 15 milhões'] = tabela['VL_ITEM_EVENTO_INFORMADO'].loc[tabela['VL_ITEM_EVENTO_INFORMADO'] >= 15000000].count()
lista8.append(dici)
valor = pd.DataFrame(lista8)
valor = valor.transpose().reset_index()
valor.columns = ['Valores', 'Contagem']
#valor.to_excel('tabelavalor.xlsx')

lista9 = []
dici1 = {}
dici1['Maior ou igual a 100 mil'] = tabela['QT_ITEM_EVENTO_INFORMADO'].loc[tabela['QT_ITEM_EVENTO_INFORMADO'] >= 100000].count()
dici1['Maior ou igual a 500 mil'] = tabela['QT_ITEM_EVENTO_INFORMADO'].loc[tabela['QT_ITEM_EVENTO_INFORMADO'] >= 500000].count()
dici1['Maior ou igual a 1 milhão'] = tabela['QT_ITEM_EVENTO_INFORMADO'].loc[tabela['QT_ITEM_EVENTO_INFORMADO'] >= 1000000].count()
dici1['Maior ou igual a 5 milhões'] = tabela['QT_ITEM_EVENTO_INFORMADO'].loc[tabela['QT_ITEM_EVENTO_INFORMADO'] >= 5000000].count()
lista9.append(dici1)
quant = pd.DataFrame(lista9)
quant = quant.transpose().reset_index()
quant.columns = ['Quantidades', 'Contagem']
#quant.to_excel('tabelaquant.xlsx')

# Gráficos de algumas variáveis
# Faixa etária e sexo
plt.figure(figsize=(15,8))
plt.subplot(121)
tabela['FAIXA_ETARIA'].value_counts().plot.pie(autopct = '%1.0f%%',
                                        colors = sns.color_palette('seismic', 14), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.title('Faixa etária', fontsize=20)
plt.subplots_adjust(hspace=.4)
plt.subplot(122)
tabela['SEXO'].value_counts().plot.pie(autopct = '%1.0f%%',
                                        colors = sns.color_palette('seismic', 2), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.title('Sexo do beneficiário', fontsize=20) 
plt.subplots_adjust(hspace=.4)

# Tipo de atendimento e tipo de internação
plt.figure(figsize=(15,8))
plt.subplot(121)
tabela['CD_CARATER_ATENDIMENTO'].value_counts().plot.pie(autopct = '%1.0f%%', 
                                        colors = sns.color_palette('seismic', 2), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.legend(['2.0 = Emergência', '1.0  = Eletivo'], loc='best',prop={'size': 10})
plt.title('Caráter do atendimento', fontsize=20)
plt.subplots_adjust(hspace=.4)
plt.subplot(122)
tabela['CD_TIPO_INTERNACAO'].value_counts().plot.pie(autopct = '%1.0f%%',
                                        colors = sns.color_palette('seismic', 6), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.legend(['1.0  = Clínica', '2.0 = Cirúrgica', '3.0 = Obstétrica', '4.0 = Pediátrica', '5.0 = Psiquiátrica' ], loc='best',prop={'size': 10})
plt.title('Tipo de internação', fontsize=20)
plt.subplots_adjust(hspace=.4)

# Modalidade e porte da empresa
plt.figure(figsize=(15,8))
plt.subplot(121)
tabela['NM_MODALIDADE'].value_counts().plot.pie(autopct = '%1.0f%%',
                                        colors = sns.color_palette('seismic', 6), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.title('Modalidade', fontsize=20) 
plt.subplots_adjust(hspace=.4)
plt.subplot(122)
tabela['PORTE'].value_counts().plot.pie(autopct = '%1.0f%%',
                                        colors = sns.color_palette('seismic', 4), wedgeprops = {'linewidth':2, 'edgecolor':'white'})
circulo = plt.Circle((0,0),.7, color = 'white')
plt.gca().add_artist(circulo)
plt.ylabel('')
plt.title('Porte das empresas', fontsize=20)
plt.subplots_adjust(hspace=.4)

