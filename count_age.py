from mrjob.job import MRJob
from mrjob.step import MRStep

#  Creating the class for the counting the frequency of the age
class count_freq(MRJob):

#  The mapper definition is to split the line by ', ' into words and stores it into an array and then it outputs  the first field of the array as the key with  1 as value to each of the keys
# Output of the mapper : Key - Age
#                        Value - 1
 def mapper(self,_,line):
  data=line.split(", ");
  yield(data[0],1)

#  We are using a combiner to reduce the load on the reducer, so that it can sum the age frequencies by age age and then pass it to the reducers
# Output of the combiner: Key - Age
#                         Value - sum of frequencies of age
 def combiner(self,age,age_freq):
  yield age,sum(age_freq)

# The reducer_sum -  sums the value of the frequency combing from different mappers #and then pass None as the key along with a combination of sum(age frequencies) and #the age.
# Output of the reducer_sum: Key - None
#                            Value: combination of sum of age frequencies and age
 def reducer_sum(self,age,age_freq):
  yield None,(sum(age_freq),age)

# The reducer_sort: Sorts the outputs from the previous reducer in descending order and then send out only the first 10 values of age_frequency and the age as the key value pair.
# Output of the reducer_sort:Key - Age_Frequency
#                            Value: Age
 def reducer_sort(self,_,age):
  k=[]
  c_counter=1
  for count,key in sorted(age, reverse=True):
    if c_counter<=10:
     yield count,key
     c_counter+=1

#  We are using the steps to define the order in which the map reduce needs to be #executed. First, we need to sum of frequencies of the age and only after than we are #sorting and printing out the top 10 values in the final reducer step.
 def steps(self):
  return [MRStep(mapper=self.mapper,combiner=self.combiner, reducer=self.reducer_sum),MRStep(reducer=self.reducer_sort)]

# Executing the map reduce jobs
if __name__=='__main__':
 count_freq.run();

  
	
