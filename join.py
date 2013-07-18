import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
  mr.emit_intermediate(record[1], record)

def reducer(key, list_of_values):
  order = list_of_values[0]
  for next_item in list_of_values[1:]:
    result = order + next_item
    mr.emit(result)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
