from pathlib import Path
import os
import re
from ast import arg
from sys import argv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

CURRENT_DIR = os.path.abspath(os.getcwd())
#dataset_dengue = f'{CURRENT_DIR}/datalake/bronze/sample_casos_dengue.txt'
#dataset_chuvas = f'{CURRENT_DIR}/datalake/bronze/sample_chuvas.csv'
dataset_dengue = f'{CURRENT_DIR}/datalake/bronze/casos_dengue.txt'
dataset_chuvas = f'{CURRENT_DIR}/datalake/bronze/chuvas.csv'

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)
dengue_columns = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]


def create_hash(element):
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    
    return element


def uf_key(element):
    uf = element['uf']
    
    return (uf, element)
    
    
def dengue_data(element):
    uf, data = element
    for value in data:
        if bool(re.search(r'\d', value['casos'])):
            yield (f"{uf}-{value['ano_mes']}", float(value['casos']))               
        yield (f"{uf}-{value['ano_mes']}", 0.0)
        
        
# Aqui são os métodos para média de chuvas
def key_uf_year_month(element):
    date, rain_mm, uf = element
    key = f"{uf}-{'-'.join(date.split('-')[:2])}"
    
    return (key, float(rain_mm) if float(rain_mm) > 0 else 0.0)


def rounding(element):
    key, rain_mm = element
    return (key, round(rain_mm, 1))


def empty_data_filter(element):
    key, data = element
    if all([data['chuvas'], data['dengue']]):
        return True
    return False


def ungroup_elements(element):
    key, data = element
    rain = data['chuvas']
    dengue = data['dengue']
    uf, ano, mes = key.split('-')
    
    return (uf, ano, mes, str(rain[0]), str(dengue[0]))


def prepare_csv(element, delimiter=';'):
    return f"{delimiter}".join(element)
    


dengue = (
    pipeline
    | "Leitura do dataset de dengue" >>         
        ReadFromText(dataset_dengue, skip_header_lines=1)
    | "Texto > lista" >> beam.Map(lambda x: x.split('|'))
    | "Lista > dicionario" >> beam.Map(lambda x: dict(zip(dengue_columns, x)))
    | "Criar campo ano_mes" >> beam.Map(create_hash)    
    | "Criar chave pelo estado" >> beam.Map(uf_key)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(dengue_data)
    | "Soma dos casos pelo estado" >> beam.CombinePerKey(sum)    
)


chuvas = (
    pipeline 
    | "Leitura do dataset de chuvas" >> 
        ReadFromText(dataset_chuvas, skip_header_lines=1)
    | "De texto para lista chuvas" >> beam.Map(lambda x: x.split(','))
    | "Criando chave UF-ANO-MES" >> beam.Map(key_uf_year_month)
    | "Soma dos totais de chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arredondamento de precipitação" >> beam.Map(rounding)    
)

result = (
    ({'chuvas': chuvas, 'dengue': dengue})
    | 'Mesclar pcols' >> beam.CoGroupByKey()
    | 'Filtrar dados vazios' >> beam.Filter(empty_data_filter)
    | 'Descompactar elementos' >> beam.Map(ungroup_elements)
    | 'Preparar csv' >> beam.Map(prepare_csv)    
)

header = 'UF;ANO;MES;VOL.CHUVA;CASOS DENGUE'
data_set_result = f'{CURRENT_DIR}/datalake/silver/resultado'
result | 'Criar arquivo CSV' >> WriteToText(
    data_set_result,
    file_name_suffix='.csv',
    header=header
    )

pipeline.run()

