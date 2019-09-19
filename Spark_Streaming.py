#Shobhit Khandelwal

#Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01
from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam
import numpy as np
from pprint import pprint
from scipy import sparse
from bitarray import bitarray
from  numpy import median, mean
import mmh3, re
import mmap
import csv
import math

isSmall = False
#PART II A: Map Reduce implementationj in Spark
def myMap(x):
	k = x[0]
	v = x[1]
	mapped_kv = []
	(label, row, col)  = k
	mat, sA, sB = label.split(":")
	m1, n1 = sA.split(",")
	m2, n2 = sB.split(",")
	
	if mat == 'A':
		for k in range(0,int(n2)):
			mapped_kv.append(((row,k),(mat,col,v)))
	elif mat == 'B':
		for i in range(0,int(m1)):
			mapped_kv.append(((i,col),(mat,row,v)))
	return mapped_kv

def myReduce(rec):
	#<COMPLETE>
	(k, vs) = rec
	listA = [mat for mat in vs if mat[0] == 'A']
	listB = [mat for mat in vs if mat[0] == 'B']
	listA = sorted(listA , key=lambda val: val[1])
	listB = sorted(listB , key=lambda val: val[1])
	
	j = 0
	listR = [] 
	for elem in listA:
		while j < len(listB) and listB[j][1] <= elem[1]:
			if elem[1] == listB[j][1]:
				listR.append(elem[2]*listB[j][2])
			#We can just fall through without break as it will be handled in while loop cond
			j += 1
		
		if j >= len(listB):
			break
				
	res = np.sum(listR)
	return (k, res)


def sparkMatrixMultiply(rdd):
	#rdd where records are of the form:
	#  ((“A:nA,mA:nB,mB”, row, col),value)
	#returns an rdd with the resulting matrix
	# rstring = rdd[0][0].split(":")
	# print (rstring)
	rdd1 = rdd.flatMap(myMap).groupByKey()
	#rdd2 = rdd1.aggregateByKey(zero_val, seq_func, comb_func)
	rdd2 = rdd1.map(myReduce)
	resultRdd = rdd2
	return resultRdd

'''
 NOTE: 
 Basic implementation of bloom filter with 128 bits
 User Objects can't be broadcasted hence had to figure out 
 am alternative for this
'''
'''
class BloomFilter():
	
	def __init__(self, size, n):
		self.size = size
		self.nHash = n
		self.bitMap = bitarray(self.size)
		self.bitMap.setall(0)

	def add_key(self, key):
		for seed in range(0,self.nHash):
			hkey = mmh3.hash(key, seed*5)%(self.size)
			self.bitMap[hkey] = True


	def check_key(self, key):

		for seed in range(0, self.nHash):
			hkey = mmh3.hash(key, seed*5)%(self.size)
			if self.bitMap[hkey] == False:
				return False

		return True
'''

# Alterbate implementation of BLOOM FILTER
def generate_hash_idx(key):

	size = 1000000
	bitMap = []
	for seed in range(0, 5):
		hkey = mmh3.hash(key, seed*5)%(size)
		bitMap.append(hkey)
	return bitMap

def check_key(key,filt):
	for idx in generate_hash_idx(key[0].rstrip().lower()):
		if filt.value[idx] == False:
			return False
	return True


def check_for_nf(post, nf):
	mapped_kv = []
	post = post.lower()
	words = post.split(" ")

	for i, w in enumerate(words):
		for k, v in nf.value.items():
			for reg in v:
				pat = re.compile(reg)
				if pat.fullmatch(w) != None and i < len(words)-1:
					e = i+4 if i < len(words)-3 else -1
					mapped_kv.append((k,  ' '.join(words[i+1:e])))

	return mapped_kv
		
'''
Implementing custom accumulator for keeping track of maximum
'''
class MaxAccumulator(AccumulatorParam):
	
	# Start with an initial value of 0
	def zero(self, value):
		return 0

	# Modified accumulator functionality to keep track of maximum at each node
	def addInPlace(self, val1, val2):
		return max(val1, val2)

'''
Implementing Flajolet Martin Algorithm as specified in lecture slides
'''
def flajolet_martin_algo(k,v, accum,seed,n):
	key = ['MM','OH','SIGH','UM']
	idx = key.index(k)
	hkey = mmh3.hash128(v.lower(), seed)%n
	hkey = format(hkey, 'b')
	trailing_zeros = len(hkey) - len(hkey.rstrip('0'))
	accum.add(pow(2,trailing_zeros))
	return

def umbler(sc, rdd):
	#	sc: the current spark context
	#   		(useful for creating broadcast or accumulator variables)
	#	rdd: an RDD which contains location, post data.
	#
	#returns a *dictionary* (not an rdd) of distinct phrases per um category

	#SETUP for streaming algorithms

	# Reading valid locations into a list and then add each entry to the 
	# bloom filter with #bits = 1000000 in order to prevent storing this entire list in memory
	global isSmall
	validLocFile = 'umbler_locations.csv'
	size = 1000000
	bf = bitarray(size)
	bf.setall(0)

	with open(validLocFile) as file:
		for loc in file:
			loc = loc.rstrip().lower()
			bitMap = generate_hash_idx(loc)
			for i in bitMap:
				bf[i] = True

	# Regex patterns to match non fluencies
	nonfluencies = {'MM': ['mm+[.,!?]*'],
					'OH': ['oh+[.,!?]*', 'ah+[.,!?]*'],
					'SIGH': ['sigh[.,!?]*', 'sighed[.,!?]*', 'sighing[.,!?]*', 'sighs[.,!?]*', 'ugh[.,!?]*', 'uh[.,!?]*'],
					'UM': ['um+[.,!?]*', 'hm+[.,!?]*', 'huh[.,!?]*'] }
	
	distinctPhraseCounts = {'MM': 0,
							'OH': 0,
							'SIGH': 0,
							'UM': 0	}

	accum = dict()
	for k in distinctPhraseCounts.keys():
		maxCount = 0
		accum[k] = sc.accumulator(maxCount, MaxAccumulator())
	
	bloom_filter = sc.broadcast(bf)
	nf = sc.broadcast(nonfluencies)
	#print (rdd.take(5)) 

	#PROCESS the records in RDD (i.e. as if processing a stream:
	# a foreach transformation
	rdd1 = rdd.map(lambda x : (x[0], check_for_nf(x[1], nf))) \
				.filter(lambda tup: check_key(tup, bloom_filter) and len(tup[1]) != 0) \
				.map(lambda data: (data[1][0][0], data[1][0][1]))
	
	#rdd1.saveAsTextFile("test")
	#rdd1.saveAsTextFile("test")
	#At this point all (nonfluency Category, phrases) are present as key-value pair in the RDD
	#print(rdd1.saveAsTextFile("abc.txt"))
	# Applying FM Algorithm to each record to get the number of distinct phrases for each 
	# non-Fluency
	countList = {'MM': [],
				 'OH': [],
				 'SIGH': [],
				 'UM': [] }

	n  = 64 if isSmall else 1024
	for hash in range(0,5):
		filteredAndCountedRdd = rdd1.foreach(lambda x : flajolet_martin_algo(x[0],x[1],accum[x[0]],hash*2,n))
		for key in countList:
			countList[key].append(accum[key].value)
			accum[key].value = 0

	for key in countList:
		distinctPhraseCounts[key] = median(countList[key])

	return distinctPhraseCounts


################################################
## Testing Code (subject to change for testing)

def createSparseMatrix(X, label):
	sparseX = sparse.coo_matrix(X)
	list = []
	for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
		list.append(((label, i, j), v))
	return list                                         

def runTests(sc):    
	#runs MM and Umbler Tests for the given sparkContext

	#MM Tests:
	print("\n*************************\n MatrixMult Tests\n*************************")
	test1 = [ (('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 1, 0), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 0, 1), 3) ]
	test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
	test3 = createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')
	
	mmResults = sparkMatrixMultiply(sc.parallelize(test1))
	pprint(mmResults.collect())
	mmResults = sparkMatrixMultiply(sc.parallelize(test2))
	pprint(mmResults.collect())
	mmResults = sparkMatrixMultiply(sc.parallelize(test3))
	pprint(mmResults.collect())
	

	#Umbler Tests:
	print("\n*************************\n Umbler Tests\n*************************")
	testFileSmall = 'publicSampleLocationTweet_small.csv'
	testFileLarge = 'publicSampleLocationTweet_large.csv'
	global isSmall
	isSmall = True
	#setup rdd

	smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
	#pprint(smallTestRdd.take(5))  #uncomment to see data
	pprint(umbler(sc, smallTestRdd))

	isSmall = False
	largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
	#pprint(largeTestRdd.take(5))  #uncomment to see data
	pprint(umbler(sc, largeTestRdd))
	
	return 

if __name__ == "__main__":
	sc = SparkContext("local", "MyApp")
	runTests(sc)



