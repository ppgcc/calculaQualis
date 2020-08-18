from __future__ import print_function
import pickle
import os.path
import re
import pandas as pd
import json

import requests
from requests.exceptions import HTTPError
import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import proxyscrape
from proxyscrape import create_collector, get_collector

from datetime import datetime
import urllib3
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from progress.bar import Bar

from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
# horário dos EUA (+3 horas) do horário brasileiro
# -> 3 horas da manhã nos EUA é a meia-noite no Brasil
@sched.scheduled_job('cron', day_of_week='mon-sun', hour=3)
def scheduled_job():
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # The ID and range of a sample spreadsheet.
    #SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
    #SAMPLE_RANGE_NAME = 'Class Data!A2:E'
    SAMPLE_SPREADSHEET_ID = '1EBJ8OXGPHU58ukZAUfF9N7Cy8A8mbl-jjUjZL5Cg9xM'
    SAMPLE_RANGE_NAME = 'Qualis!A1:I1849'

    SERVICE_ACCOUNT_FILE = 'credencialContaServico.json'
    API_KEY_SCOPUS = '[CHAVE_OMITIDA]'

    credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    def main():
        print('1/5. Iniciando o processo de configuração e autenticação com o Google Sheets, para buscar TODOS os dados da planilha.')
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        #creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        '''
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        '''
        '''
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials_v7.json', SCOPES)
                creds = flow.run_local_server(port=8080)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        '''
        delegated_credentials = credentials.with_subject('[CREDENCIAL_OMITIDA]')
        service = build('sheets', 'v4', credentials=delegated_credentials)
        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()

        header = result.get('values', [])[0]   # Assumes first line is header!
        values = result.get('values', [])[1:]  # Everything else is data.

        if not values:
            print('No data found.')
        else:
            all_data = []
            for col_id, col_name in enumerate(header):
                column_data = []
                for row in values:
                    column_data.append(row[col_id])
                ds = pd.Series(data=column_data, name=col_name)
                all_data.append(ds)
            df = pd.concat(all_data, axis=1)
        print('Finalizou: 1/5')

        terminou = False
        row = df.shape[0] #1850
        val_init = 0
        val_fin = 99
        itr = 1
        print('###### Realiza o processo a cada 100 periódicos, dividido em iterações:')
        while terminou == False:
            if val_fin > row:
                val_fin = row
                terminou = True
            print('2/5. Iniciando o processo de busca do Percentil... - Iteração: ', itr)
            print('Iteração: ============ ', itr)

            df_new = df.iloc[val_init:val_fin]
            print('Prim.Peri ', df_new.iloc[[0], 0])
            print('Últ.Peri ', df_new.iloc[[-1], 0])

            valores = realizaParanaue(df_new)

            print('Finalizou: 2/5 - Iteração: ', itr)

            #print('3/6. Iniciando o processo de configuração e autenticação novamente com o Google Sheets, para buscar ALGUNS dados da planilha para fazer o UPDATE.')
            # Call the Sheets API
            #sheet = service.spreadsheets()
            #result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
            #                            range=SAMPLE_RANGE_NAME).execute()
            #print('Finalizou: 3/6')

            print('3/5. Iniciando a configuração para realizar o Update na planilha. - Iteração: ', itr)
            #print(valores)

            if val_init == 0:
                linhas_init = val_init+2
                linhas_fin = val_fin+1
                val_init+=99
            else:

                linhas_init = val_init+2
                linhas_fin = val_fin+1
                val_init+=100

            val_fin+=100

            # The A1 notation of the values to update. 'Qualis!F2:G2'
            range_update = 'Qualis!D'+ str(linhas_init) +':I'+str(linhas_fin)  # TODO: Update placeholder value.

            # How the input data should be interpreted.
            value_input_option = 'USER_ENTERED'  # TODO: Update placeholder value.

            value_range_body = {
                "values": valores
            }

            #print(value_range_body)
            print('Finalizou: 3/5 - Iteração: ', itr)
            print('4/5. Iniciando ação para efetivar o update na planilha.', itr)

            request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                            range=range_update,
                                            valueInputOption=value_input_option,
                                            body=value_range_body).execute()

            print('Finalizou: 4/5 - Iteração: ', itr)
            itr+=1

        print('5/5. Planilha atualizada com sucesso!', itr)
        ##print('{0} cells updated.'.format(request.get('updatedCells')))


    def realizaParanaue(df):
        qualis = None
        estratoBase = None
        valores = []

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        for index, row in df.iterrows():
            linha = []
            #toda vez que o script executar, ele vai registrar a data desta tentativa de atualização
            linha.insert(4, dt_string)

            ajusteSBC = df.loc[index, 'Ajuste_SBC']
            issn = df.loc[index, 'issn']

            if issn != 'nulo':
                #print(df.loc[index, 'issn'])
                percentil, link_scopus, log = buscaPercentil(issn)

                if log != '': #deu erro ao buscar o periódico na scopus
                    if link_scopus != '':
                        linha.insert(0, link_scopus)
                    else:
                        linha.insert(0, df.loc[index, 'link_scopus'])

                    linha.insert(1, df.loc[index, 'percentil'])
                    linha.insert(2, df.loc[index, 'Qualis_Final'])
                    linha.insert(3, df.loc[index, 'data-atualizacao'])
                    linha.insert(5, log)
                elif percentil != None:
                    linha.insert(0, link_scopus)
                    linha.insert(1, percentil)
                    linha.insert(3, dt_string)
                    if percentil != 0 :
                        estratoBase = aplicaRegra(percentil)
                        if ajusteSBC == 'sim':
                            qualis = sobeNivel(estratoBase, 2)
                            linha.insert(2, qualis)
                            linha.insert(5, 'atualizado com sucesso')
                        else:
                            linha.insert(2, estratoBase)
                            linha.insert(5, 'atualizado com sucesso')
                    else:
                        linha.insert(2, df.loc[index, 'Qualis_Final'])
                        linha.insert(5, 'percentil está zerado')
                else:
                    linha.insert(0, df.loc[index, 'link_scopus'])
                    linha.insert(1, df.loc[index, 'percentil'])
                    linha.insert(2, df.loc[index, 'Qualis_Final'])
                    linha.insert(3, df.loc[index, 'data-atualizacao'])
                    linha.insert(5, 'percentil inválido')
            else:
                linha.insert(0, df.loc[index, 'link_scopus'])
                linha.insert(1, df.loc[index, 'percentil'])
                linha.insert(2, df.loc[index, 'Qualis_Final'])
                linha.insert(3, df.loc[index, 'data-atualizacao'])
                linha.insert(5, 'sem informação do ISSN')

            valores.insert(index, linha)
            #bar.next()

        return valores

    # executa busca no google
    def buscaPercentil(issn):
        issn = issn.replace('-', '')
        percentil = None
        log = ''
        link_scopus = ''
        try:
            uri = "https://api.elsevier.com/content/serial/title?issn=" + issn + "&view=citescore&apiKey=" + API_KEY_SCOPUS
            response = requests.get(uri)
            json_data = json.loads(response.text)

            link_scopus = json_data['serial-metadata-response']['entry'][0]['link'][0]['@href']
            try:
                percentil = json_data['serial-metadata-response']['entry'][0]['citeScoreYearInfoList']['citeScoreYearInfo'][1]['citeScoreInformationList'][0]['citeScoreInfo'][0]['citeScoreSubjectRank'][0]['percentile']
            except:
                log = 'sem valor de percentil'

        except HTTPError as http_err:
            log += 'HTTP error occurred: ' + str(http_err) + ' - Status_code: ' + str(response.status_code)
        except Exception as err:
            log += 'Other error occurred: ' + str(err) + ' - Status_code: ' + str(response.status_code)

        if percentil == '':
            percentil = None

        return percentil, link_scopus, str(log)

    def aplicaRegra(percentil):
        percentil = float(percentil)
        estratoBase = None

        if percentil >= 87.5:
            estratoBase = "A1"
        elif percentil >= 75:
            estratoBase = "A2"
        elif percentil >= 62.5:
            estratoBase = "A3"
        elif percentil >= 50:
            estratoBase = "A4"
        elif percentil >= 37.5:
            estratoBase = "B1"
        elif percentil >= 25:
            estratoBase = "B2"
        elif percentil >= 12.5:
            estratoBase = "B3"
        elif percentil < 12.5:
            estratoBase = "B4"

        return estratoBase

    def sobeNivel(estratoBase, nivel):
        qualis = None
        if nivel == 1:
            if estratoBase == "A4":
                qualis = "A3"
            elif estratoBase == "B1":
                qualis = "A4"
            elif estratoBase == "B2":
                qualis = "B1"
            elif estratoBase == "B3":
                qualis = "B2"
            elif estratoBase == "B4":
                qualis = "B3"
        elif nivel == 2:
            if estratoBase == "A4":
                qualis = "A3"
            elif estratoBase == "B1":
                qualis = "A3"
            elif estratoBase == "B2":
                qualis = "A4"
            elif estratoBase == "B3":
                qualis = "B1"
            elif estratoBase == "B4":
                qualis = "B2"

        return qualis

    if __name__ == '__main__':
        main()

sched.start()
