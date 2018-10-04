import pandas as pd
from datetime import datetime
import json
import logging
from random import choice
import urllib.request


class BacenScraper:

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger(__name__)

    def build_url(self, cod_modalidade, data):
        self.log.info("-----------------------------")
        self.log.info("Modalidade: {}".format(cod_modalidade))
        self.log.info("Data: {}".format(data))

        url = "https://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/" \
              "TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=modalidade:" \
              + cod_modalidade + ";periodoInicial:" + data

        self.log.info(url)
        self.log.info("-----------------------------")
        return url

    def scrap_date_list(self):
        url = 'http://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=undefined'

        header = self.random_headers()
        req = urllib.request.Request(
            url,
            headers=header
        )

        response_text = urllib.request.urlopen(req).read()
        response = json.loads(response_text)
        date_list = []
        try:
            for i in response['parametros'][3]['ValidValues']:
                date_object = datetime.strptime(i['Value'], '%m/%d/%Y %I:%M:%S %p')
                date_list.append(date_object)
        except Exception as err:
            self.log.error(err)
        return date_list

    @staticmethod
    def get_last_date(date_list):
        return max(date_list)

    @staticmethod
    def parse_date(date_object, format):
        return date_object.strftime(format)

    @staticmethod
    def generate_date_range(start_date, end_date):
        return pd.bdate_range(start=start_date, end=end_date).tolist()

    def get_periodo_semana(self, dfraw):
        try:
            return dfraw[4][0].to_string()[1:].strip()
        except Exception as err:
            self.log.error(err)


    def get_tipo_encargo(self, dfraw):
        try:
            return dfraw[7][0].to_string()[1:].strip()
        except Exception as err:
            self.log.error(err)

    @staticmethod
    def random_headers():

        desktop_agents = [
               #Chrome
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
                #Firefox
                'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
                'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
                'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
                'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
                'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
                'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
        ]

        return {'User-Agent': choice(desktop_agents)}


    def scrap_bacen(self, cod_modalidade, daterange, temp_file):
        """Scraps data from www.bcb.gov.br"""

        try:

            columns = ['posicao', 'instituicao', 'taxa_am', 'taxa_aa', 'codigo_modalidade',
                       'periodo_inicial', 'periodo_semana', 'modalidade', 'tipo_encargo']

            dataset = pd.DataFrame(columns=columns)
            for i in daterange:

                data = self.parse_date(i, '%m/%d/%Y')
                url = self.build_url(cod_modalidade, data)

                try:
                    header = self.random_headers()
                    req = urllib.request.Request(
                        url,
                        headers=header
                    )

                    print("User-Agent Sent: {}".format(header))

                    response_text = urllib.request.urlopen(req).read()
                    response = json.loads(response_text)
                    df_raw = pd.read_html(response['conteudo'], thousands=' ')

                    df = df_raw[9]
                    dataset = self.clean_dataset(cod_modalidade, columns, dataset, df, df_raw, i)
                    dataset.to_csv(temp_file, sep=';', index=False)
                except Exception as err:
                    self.log.error(err)
                    continue
            dataset = pd.read_csv(temp_file, sep=';')
            return dataset
        except Exception as e:
            self.log.error(e)

    def clean_dataset(self, cod_modalidade, columns, dataset, df, df_raw, i):
        df = df.drop(df.index[[0, 1, 2]])
        df['codigo_modalidade'] = cod_modalidade
        df['periodo_inicial'] = i
        df['periodo_semana'] = self.get_periodo_semana(df_raw)
        if cod_modalidade == '220':
            df['modalidade'] = 'Crédito pessoal consignado público'
        else:
            df['modalidade'] = 'Crédito consignado INSS'
        df['tipo_encargo'] = self.get_tipo_encargo(df_raw)
        df.columns = columns
        dataset = dataset.append(df)
        dataset['taxa_am'] = dataset['taxa_am'].replace(',', '.', regex=True).astype(float)
        dataset['taxa_aa'] = dataset['taxa_aa'].replace(',', '.', regex=True).astype(float)
        return dataset

    def run_scrap(self, path, cdmodalidade,temp_filepath, dt_base = None, scrap_type="FULL"):

        datelist = self.scrap_date_list()

        if dt_base is not None and scrap_type == "FULL":
            ## pega datas retroativas

            datelist = [date for date in datelist if date < datetime.strptime(dt_base, '%Y-%m-%d')]

            dataset = self.scrap_bacen(cdmodalidade, datelist, temp_filepath)
        elif dt_base is None and scrap_type == "DELTA":
            max_date = self.get_last_date(datelist)
            datelist = list()
            datelist.append(max_date)
            dataset = self.scrap_bacen(cdmodalidade, datelist, temp_filepath)
        else:
            dataset = self.scrap_bacen(cdmodalidade, datelist, temp_filepath)

        filedate = self.parse_date(max_date, '%Y-%m-%d')

        self.persist_dataset(cdmodalidade, dataset, filedate, path)

    @staticmethod
    def persist_dataset(cdmodalidade, dataset, filedate, path):

        filename = filedate + "-" + cdmodalidade + '.csv'
        filepath = path + "/" + filename
        dataset.to_csv(filepath, index=False)



# scrap.run_scrap(".", "220", "csv", '220.csv','DELTA')  # cdmodalidade = "220"  # Crédito consignado Público
# scrap.run_scrap("C://workspace", "218", "csv",'218.csv','DELTA')  # cdmodalidade = "218"  # Crédito consignado INSS
