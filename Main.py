#Scructure to work paper-id:::author1::author2::…. ::authorN:::title

import mincemeat
import glob
import csv
from unidecode import unidecode


def file_contents(file_name):
    f = open(file_name, encoding="UTF-8")
    try:
        return f.read()
    finally:
        f.close

def mapfn(key, value):
    print("... Maping: {}".format(key))

    for line in value.splitlines():
        line = unidecode(line)
        line = line.split(":::")

        tile = line[2]
        authors = line[1].split("::")

        for author in authors:
            yield author, title

def reducefn(key, value):
    print("... Reducing {}".format(key))

text_files = glob.glob("files\\*")

source = dict((file_name, file_contents(file_name))for file_name in text_files)

mapfn("files\\c0001", source["files\\c0001"])



s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfnr
s.reducefn = reducefn

print("Starting server...")
results = s.run_server(password="changeme")
print("Server Started!")

w = csv.writer(open("Resultado.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])
