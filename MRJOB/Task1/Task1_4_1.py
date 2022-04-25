# Task1-4-1
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    def mapper1(self,_,line):
        line = line.replace(","," ")
        data = line.split(' ')
        sender = data[0]
        reciever = data[1]
        yield (sender, 1)
   
    def reducer1(self,key,value):
        total_count = sum(value)
        yield None,(total_count,key)
    def reducer2(self,_,value):
        N = 10
        value = sorted(list(value),reverse=True)
        return value[:N]
    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),MRStep(reducer=self.reducer2)]
if __name__ == '__main__':
    MRmyjob.run()