#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Scructure to work paper-id:::author1::author2::…. ::authorN:::title
import mincemeat
import glob
import csv
import operator

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close

def mapfn(key, value):
    from stopwords import allStopWords
    #print("... Maping: {}".format(key))

    for line in value.splitlines():
        line = line.split(":::")

        title = line[2]
        authors = line[1].split("::")

        newTitle = ""
        for word in title.split():
            word = word.replace(",", "").replace(".", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace(";", "").replace("{", "").replace("}", "")

            if word not in allStopWords:
                newTitle = "{} {}".format(newTitle, word)

        for author in authors:
            #print("Mapped: {}-{}".format(author, newTitle))
            yield author, newTitle

def reducefn(key, value):
    #print("Reducing {}...".format(key))
    words = dict()

    for title in value:
        for word in title.split():
            if word in words:
                words[word] = words[word] + 1
            else:
                words[word] = 1

    return words

text_files = glob.glob("files\\*")

source = dict((file_name, file_contents(file_name))for file_name in text_files)

mapfn("files\\c0001", source["files\\c0001"])

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

print("Starting server...")
results = s.run_server(password="changeme")
print("... done!")

print("Writing CSV in project folder...")
w = csv.writer(open("C:\\Users\\Henrique Oelze\\Development\\MapReduce-trab-puc\\Resultado.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])
print("... done")

print("Analizing 'Grzegorz Rozenberg'...")
toAnalize = results["Grzegorz Rozenberg"]
analizedWords = dict(sorted(toAnalize.iteritems(), key=operator.itemgetter(1), reverse=True)[:2])
print(analizedWords)
print("... done")

print("Analizing 'Philip S. Yu'...")
toAnalize = results["Philip S. Yu"]
analizedWords = dict(sorted(toAnalize.iteritems(), key=operator.itemgetter(1), reverse=True)[:2])
print(analizedWords)
print("... done")
