# Task1-4-3
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    def mapper1(self,_,line):
        line = line.replace(","," ")
        data = line.split(' ')
        sender = data[0]
        reciever = data[1]
        yield sender, (1,0)
        yield reciever, (0,1)
      
        
    def reducer1(self,key,value):
       
        s = 0
        r = 0
        for x in value:
            s += x[0] 
            r += x[1] 

        yield key , (s,r)

    def steps(self):
       return [MRStep(mapper=self.mapper1,reducer=self.reducer1)]
   
if __name__ == '__main__':
    MRmyjob.run()