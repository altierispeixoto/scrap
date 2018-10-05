import pandas as pd
from datetime import datetime
import logging
from proxy import ScrapBase
from dateutil import parser


class BacenScraper():

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger(__name__)

    def build_url(self, cod_modalidade, _date):
        self.log.info("-----------------------------")
        self.log.info("Modalidade: {}".format(cod_modalidade))
        self.log.info("Data: {}".format(_date))

        url = "https://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/" \
              "TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=modalidade:" \
              + cod_modalidade + ";periodoInicial:" + _date

        print(url)
        self.log.info(url)
        self.log.info("-----------------------------")
        return url

    def scrap_date_list(self):
        url = 'https://www.bcb.gov.br/api/relatorio/pt-br/txjuros?path=conteudo/txcred/Reports/TaxasCredito-Consolidadas-porTaxasAnuais-Historico.rdl&parametros=undefined'

        scrap = ScrapBase(url)
        response_call = scrap.url_call()
        response = response_call.json()
        date_list = []
        try:
            for i in response['parametros'][3]['ValidValues']:
                date_object = parser.parse(i['Value'])
                date_list.append(date_object)
        except Exception as err:
            self.log.error(err)
        return date_list

    def scrap_bacen(self, codes, daterange, filepath):
        """Scraps data from www.bcb.gov.br"""

        try:
            for i in daterange:

                _date = self.parse_date(i, '%m/%d/%Y')

                for code in codes:
                    url = self.build_url(code, _date)

                    try:
                        scrap = ScrapBase(url)
                        response_call = scrap.url_call()
                        response = response_call.json()
                        df_raw = pd.read_html(response['conteudo'], thousands=' ')
                        dataset = self.clean_dataset(code, df_raw ,i )

                        self.persist_dataset(dataset, filepath, self.parse_date(i, '%Y-%m-%d') , code)

                    except Exception as err:
                        self.log.error(err)
                        continue
        except Exception as e:
            self.log.error(e)


    def run_scrap(self, codes, filepath, dt_base = None):

        datelist = self.scrap_date_list()

        if dt_base is not None:
            ## pega datas retroativas
            datelist = [date for date in datelist if date < datetime.strptime(dt_base, '%Y-%m-%d')]
        # elif dt_base is None:
        #     datelist = self.get_last_date(datelist)

        self.scrap_bacen(codes, datelist, filepath)


    def clean_dataset(self, cod_modalidade,df_raw, periodo_inicial):

        columns = ['posicao', 'instituicao', 'taxa_am', 'taxa_aa', 'codigo_modalidade',
                   'periodo_inicial', 'periodo_semana', 'modalidade', 'tipo_encargo']

        dataset = pd.DataFrame(columns=columns)

        df = df_raw[9]
        df = df.drop(df.index[[0, 1, 2]])
        df['codigo_modalidade'] = cod_modalidade
        df['periodo_inicial'] = periodo_inicial
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

    def persist_dataset(self, dataset, filepath, date, code):
        filename = filepath + '/' + date + '-' + code + '.csv'
        dataset.to_csv(filename, index=False)



# scrap.run_scrap(".", "220", "csv", '220.csv','DELTA')  # cdmodalidade = "220"  # Crédito consignado Público
# scrap.run_scrap("C://workspace", "218", "csv",'218.csv','DELTA')  # cdmodalidade = "218"  # Crédito consignado INSS
