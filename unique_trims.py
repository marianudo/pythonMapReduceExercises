import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
  seq_id = record[0]
  seq = record[1]
  trimmed_sec = record[1][0:len(record[1])-10]  
  mr.emit_intermediate(1, trimmed_sec)

def reducer(key, list_of_values):
    no_duplicates = set(list_of_values)
    for next in no_duplicates:
      mr.emit(next)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
