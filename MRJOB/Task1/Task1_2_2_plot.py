# Task1-2-2 for plotting
import re
WORD_RE = re.compile(r"[\w']+")
from mrjob.step import MRStep
from mrjob.job import MRJob
class MRmyjob (MRJob):
    
    def mapper1(self,_,line):
            data = line.split() 
            if len(data)> 9 :    
                #parse line
                ip = data[0].strip()
                date = data[3].strip()
                url = data[6].strip()
                if  len(url) > 1 :
                    #Extract year from data
                    year = date[8:12]
                    month = date[4:7]
                    if year.isdigit() and len(year) > 3:                
                            yield (year,month,ip),1
                    
    def reducer1(self,key,list_of_values):       
        yield (key[0],key[1]),(sum(list_of_values))

    def reducer2(self,key,list_of_values):
         yield (key[0]),(sum(list_of_values),key[1])
   
    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    MRmyjob.run()
