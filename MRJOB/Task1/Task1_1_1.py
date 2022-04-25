# Task1-1-1
from unidecode import unidecode 
import re


WORD_RE = re.compile(r"[\w']+")
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    def mapper1(self,_,line): 
        line = unidecode(line)     
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
    def combiner(self, key,value):
        yield key,sum(value)
    def reducer1(self,key,value):
        total_count = sum(value)
        yield None,(total_count,key)
    def reducer2(self,_,value):
        N = 20
        value = sorted(list(value),reverse=True)
        return value[:N]
    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),MRStep(reducer=self.reducer2)]
if __name__ == '__main__':
    MRmyjob.run()