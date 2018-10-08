from pyspark import SparkContext
from pyspark import SparkConf

def count_lines_lambdaExpression():
  '''
  The function is to compute the number of line in a text file.
  The function is implemented with python lambda expression.
  '''
  # configuration
  APP_NAME = 'count lines'
  conf = SparkConf().setAppName(APP_NAME)
  conf = conf.setMaster('spark://ec2-18-206-64-64.compute-1.amazonaws.com:7077')
  sc = SparkContext(conf=conf)

  # actuall lambda
  lines = sc.textFile('/input/README.txt')
  lineLength = lines.map(lambda s: len(s))
  totalLength = lineLength.reduce(lambda a,b: a+b)
  print ("The total length of the document is {}".format(totalLength))
  return totalLength
  pass

if __name__ == '__main__':
    print count_lines_lambdaExpression()