from mrjob.job import MRJob
from mrjob.step import MRStep

# Executing the map reduce jobs Creating the class for generating the Inverted Index
class Inverted_Index(MRJob):

# Defining the mapper function to be used. The mapper initially splits the line by #words and within a for loop we capture the number and the line_number. The final #value that is passed to the reducer is 
# Key - Number 
# Value - line_number
 def mapper(self,_,line):
  data=line.split();

  for i in range(1,len(data)):
   number_index=data[i]
   number_line_loc=data[0]
   yield number_index,data[0]

# In the reducer, we combine all the values of the line_number for each number into # a list. In order to remove the duplicates line numbers, we first convert to tuple #and then to list.
# Then we sort the line numbers list 
# Output :Key - number
#         Value - A list of the line_numbers which contains the number
 def reducer(self,key,value):
  a=list(set([int(i) for i in value]))
  a=sorted(a)
  yield key,a

# We are giving the order in which the map reduce jobs needs to be executed. This #step is followed as best practise for all the programs implemented
 def steps(self):
  return [MRStep(mapper=self.mapper,reducer=self.reducer)]

# Executing the map reduce jobs
if __name__=='__main__':
 Inverted_Index.run();

  
	
