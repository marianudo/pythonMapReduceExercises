import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
  x = int(record[1])
  y = int(record[2])
  matrix = str(record[0])
  value = int(record[3])

  for i in range(5):
    if matrix =='a':
      mr.emit_intermediate((x,i), (matrix, x, y, value))
    elif matrix == 'b':
      mr.emit_intermediate((i,y), (matrix, x, y, value))

def reducer(key, list_of_values):
  #print key, list_of_values
  #Let's create a data structure suitable for calculations
  #print key
  data = create_reducer_data(list_of_values)
  a_data = data['a']
  b_data = data['b']
  a_x = key[0]
  b_y = key[1]
  sumandos = []
  for i in range(5):
    a_value = 0
    if (a_x, i) in a_data.keys():
      a_value = a_data[(a_x, i)]
    b_value = 0
    if (i, b_y) in b_data.keys():
      b_value = b_data[i, b_y]
    sumandos.append(a_value * b_value)
  result = (key[0], key[1], sum(sumandos))
  mr.emit(result)
  #print key
  #print data
  #mr.emit(data)

def create_reducer_data(list_of_values):
  data = {}
  #print list_of_values
  for next_value in list_of_values:
    #print next_value
    matrix = next_value[0]
    x = next_value[1]
    y = next_value[2]
    value = next_value[3]
    matrix_data = {}
    if matrix in data.keys():
      matrix_data = data[matrix]
    else:
      data[matrix] = matrix_data
    matrix_data[(x,y)]=value
  return data

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
