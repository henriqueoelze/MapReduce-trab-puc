import mincemeat
import glob
import csv

text_files = glob.glob("C:\\Temp\\oelze\\python\\arquivos\\2.2\\*")

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(k, v):
    print "map " + k
    for line in v.splitlines():
        if k == "C:\\Temp\\oelze\\python\\arquivos\\2.2\\2.2-vendas.csv":
            yield line.split(";")[0], "Vendas:" + line.split(";")[5]
        else:
            yield line.split(";")[0], "Filial:" + line.split(";")[1]


def reducefn(k, v):
    print "reduce" + k

    total = 0
    nomeFilial = ""
    for index, value in enumerate(v):
        if value.split(":")[0] == "Vendas":
            total = total + int(value.split(":")[1])
        else:
            nomeFilial = value.split(":")[1]

    reducedList = []
    reducedList.append(nomeFilial + ", " + str(total))

    return reducedList

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("C:\\Temp\\oelze\\python\\Resultado.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])
