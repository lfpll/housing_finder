import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import re

p = beam.Pipeline(options=PipelineOptions())
float_regex = re.compile('^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$')
int_regex = re.compile('^[+-]?[0-9]$')


def loadJson(json_str):
    def classify_val(str_value):
        str_value = str_value.strip()
        if int_regex.search(str_value):
            return int
        elif float_regex.search(str_value):
            return float
        return str
    load = json.loads(json_str).items()
    return [(key, str(classify_val(val))) if isinstance(val, str) else (key, str(type(val))) for key, val in load if val]


class AverageFn(beam.CombineFn):

    def create_accumulator(self):
        return set()

    def add_input(self, list_acc, input):
        list_acc.add(input)
        return list_acc

    def merge_accumulators(self, accumulators):
        return_list = []
        for val in accumulators:
            if isinstance(val, list) or isinstance(val, set):
                return_list.extend(val)
            else:
                return_list.append(val)
        return return_list

    def extract_output(self, sum_count):
        val = sum_count[0]
        if val == str(str):
            return 'STRING'
        elif val == str(int):
            return 'INTEGER'
        elif val == str(float):
            return 'FLOAT'
        elif val == str(list):
            return 'RECORDS'

def combine_schema(tup_values):
    if 'iterables' in dir(tup_values):
        return tup_values.iterables[0][0]
    if len(tup_values) >0 and isinstance(tup_values[0],dict):
        for key,val in tup_values[1:]:
            tup_values[0][key] = val
        return tup_values[0]
    else:
        return {key:val for key,val in tup_values }



lines = p | 'ReadMyFile' >> beam.io.ReadFromText(r'/home/luizfernandolobo/PycharmProjects/parse_imoveis/data/bigtable-data/*.json') | 'ReturnTypes' >> beam.FlatMap(
    loadJson) | beam.CombinePerKey(AverageFn()) |  beam.CombineGlobally(combine_schema) | beam.Map(json.dumps) | 'WriteFile' >> beam.io.WriteToText(r'/home/luizfernandolobo/PycharmProjects/parse_imoveis/data.json',file_name_suffix='.json')
p.run()
