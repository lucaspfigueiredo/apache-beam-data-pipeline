import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options = PipelineOptions() 
pipeline = beam.Pipeline(options=pipeline_options)

dengue_cols = [
    "id",
    "data_iniSE",
    "casos",
    "ibge_code",
    "cidade",
    "uf",
    "cep",
    "latitude",
    "longitude",
]


""" Dengue Pipeline """

def list_to_dict(element, col):
    return dict(zip(col, element))

def string_to_list(element, sep="|"):
    return element.split(sep)

def date_format(element):
    element["yy_mm"] = "-".join(element["data_iniSE"].split("-")[:2])
    return element

def uf_key(element):
    key = element["uf"]
    return (key, element)

def dengue_cases(element):
    uf, records = element
    for record in records:
        if bool(re.search(r"\d", record["casos"])):
            yield (f"{uf}-{record['yy_mm']}", float(record["casos"]))
        else:
            yield (f"{uf}-{record['yy_mm']}", 0.0)


""" Precipitation Pipeline """

def key_rain(element):
    """
    Return key and rain value (mm) 
    ('UF-year-month', 1.3)
    """
    date, rain_mm, uf = element
    yy_mm = "-".join(date.split("-")[:2])
    key = f"{uf}-{yy_mm}"
    if float(rain_mm) < 0:
        rain_mm = 0.0
    else:
        rain_mm = float(rain_mm)
    return key, rain_mm

def round_rain(element):
    key, rain_mm = element
    return (key, round(rain_mm, 1))


""" Output Pipeline """

def remove_empty_fields(element):
    key, data = element
    if all([
        data["precipitation"],
        data["dengue"]
    ]):
        return True
    return False

def unzip_elements(element):
    key, data = element
    rain = data["precipitation"][0]
    dengue = data["dengue"][0]
    uf, year, month = key.split("-")
    return uf, year, month, str(rain), str(dengue)

def prepare_csv(element, sep=";"):
    return f"{sep}".join(element)


dengue = (
    pipeline
    | "Dengue Dataset" >>
        ReadFromText("dataset/sample_casos_dengue.txt", skip_header_lines=1)
    | "String to list" >> beam.Map(string_to_list)
    | "List to dict" >> beam.Map(list_to_dict, dengue_cols)
    | "Create [year_month] field" >> beam.Map(date_format)
    | "Create key by state" >> beam.Map(uf_key)
    | "Group by state" >> beam.GroupByKey()
    | "Split dengue cases" >> beam.FlatMap(dengue_cases)
    | "Sum of cases by key" >> beam.CombinePerKey(sum)
)

precipitation = (
    pipeline
    | "Precipitation Dataset" >>
        ReadFromText("dataset/sample_chuvas.csv", skip_header_lines=1)
    | "String text to list" >> beam.Map(string_to_list, sep=",")
    | "Create key [state-year-month]" >> beam.Map(key_rain)
    | "Total precipitation by key" >> beam.CombinePerKey(sum)
    | "Round off rainfall results" >> beam.Map(round_rain)
)

output = (
    ({"precipitation": precipitation, "dengue": dengue})
    | "Merge pcols" >> beam.CoGroupByKey()
    | "Filter empty field" >> beam.Filter(remove_empty_fields)
    | "Unpack elements" >> beam.Map(unzip_elements)
    | "Prepare csv" >> beam.Map(prepare_csv)
)


header = "STATE;YEAR;MONTH;PRECIPITATION;DENGUE"

output | "Create output file" >> WriteToText("output/output_file", file_name_suffix=".csv", header=header)

pipeline.run()