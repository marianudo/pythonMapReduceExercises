import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

aux = {}

def mapper(record):
    name = record[0]
    friend = record[1]
    ordered = [name, friend]
    ordered.sort()
    mr.emit_intermediate((ordered[0], ordered[1]), 1)

def reducer(key, list_of_values):
    #print key, list_of_values
    list_size = len(list_of_values)
    if list_size == 1:
        mr.emit(key)
        mr.emit((key[1], key[0]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
