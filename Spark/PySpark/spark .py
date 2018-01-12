#get orders table
orders = sc.textFile("/user/ushapotturu/data/retail_db/orders")
#split and extract 4 th column

ordersMap3 = orders.map(lambda order: order.split(",")[3])
for orderStatus in ordersMap3.collect():print(orderStatus)

#Check for 20 records in orders
ordersMap = orders.map(lambda order: order.split(","))
for orderStatus in ordersMap.take(20):print(orderStatus)


help(orders.filter)
# Extract status is either "COMPLETE" or "CLOSED"
ordersFiltered  = orders.filter(lambda order:order.split(",")[3] == "COMPLETE"  or order.split(",")[3] == "CLOSED")
ordersFiltered.count()
for i in ordersFiltered.take(100):print(i)

#User Defined Functions

ordersCompletedCount = 0
def isComplete(order,ordersCompletedCount):
  isCompleted = order.split(",")[3] == "COMPLETE"  	or \
  order.split(",")[3] == "CLOSED"
  
  if(isCompleted) : ordersCompletedCount + 1
  return isCompleted
  
ordersFiltered =  orders.filter(lambda order: isComplete(order, ordersCompletedCount))
ordersFiltered.count()

 
# ordersFiltered is not matching with ordersCompletedCount
 
ordersCompletedCount = sc.accumulator(0)
#accumulator
type(ordersCompletedCount)  
#help(ordersCompletedCount)
#to get the value in the accumulator
ordersCompletedCount.value
 
def isComplete(order,ordersCompletedCount):
  isCompleted = order.split(",")[3] == "COMPLETE"  	or \
  order.split(",")[3] == "CLOSED"
  
  if(isCompleted) : ordersCompletedCount.add(1)
  return isCompleted
 
ordersFiltered = orders.filter(lambda order: isComplete(order, ordersCompletedCount))
ordersFiltered.count()

# now add another accumulator
ordersNonCompletedCount = sc.accumulator(0)
#accumulator
type(ordersNonCompletedCount)  
#help(ordersNonCompletedCount)
#to get the value in the accumulator
ordersNonCompletedCount.value
 
def isComplete(order,ordersCompletedCount,ordersNonCompletedCount):
  isCompleted = order.split(",")[3] == "COMPLETE"  	or \
  order.split(",")[3] == "CLOSED"
  
  if(isCompleted) : ordersCompletedCount.add(1)
  else: ordersNonCompletedCount.add(1)
  return ordersCompletedCount
 
ordersFiltered = orders.filter(lambda order: isComplete(order, ordersCompletedCount,ordersNonCompletedCount))
ordersFiltered.count()



#Converting Key value pairs for orders(order_id,order_date) and orderItems()

ordersMap = ordersFiltered.map(lambda order: (order.split("|")[0]), (order.split("|")[1]))
ordersMap.first()

# for orderItems
orderItems = sc.textFile("/user/ushapotturu/retail_db/order_items")
orderItemsMap = orderItems.map(lambda x: (int(x.split(",")[1]),(int(x.split(",")[2])), (float(x.split(",")[4]))))
orderItemsMap.first()

#Joining the data sets
ordersJoin = ordersMap.join(orderItemsMap)
#.collect()
ordersJoin.count()
for i in ordersJoin.take(10): print(i)
orderItemsMap.count() #172198
ordersMap.count()  #68883

#leftjoin
ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)
for i in ordersLeftOuterJoin.take(100): print(i)
ordersLeftOuterJoin.count() #68883

#rightjoin
ordersRightOuterJoin = ordersMap.rightOuterJoin(orderItemsMap)
for i in ordersRightOuterJoin.take(100): print(i)
ordersRightOuterJoin.count() #

r = (5476,(u'2013-08-27 00:00:00.0',None))
r[0]    # 5476
r[1][0]  #u'2013-08-27 00:00:00.0'
r[1][1]  #None	
type(r[1][1])  #None type
r[1][1] == None  #true

# order with none 

ordersWithNoOrderItems = ordersLeftOuterJoin.filter(lambda order: order[1][1] == None )
for i in ordersWithNoOrderItems.take(100): print(i)
ordersWithNoOrderItems.count()

# for right Outer Join
ordersRightOuterJoin = ordersMap.rightOuterJoin(orderItemsMap)
ordersWithNoOrderItems = ordersRighttOuterJoin.filter(lambda order: order[1][1] == None )
for i in ordersWithNoOrderItems.take(100): print(i)
ordersWithNoOrderItems.count()

#Aggregating data by reduceByKey
#reduceByKey - used when function on the subsets and function on the intermediate values are same then use reduceByKey
# Eg:1,2,3 ...100. get the sum of this series.One way is dividing into mutually exclusive subsets and add them after getting intermediate values ((1to25),(25to50),(51to75),(76to100))

r = (5476,(u'2013-08-27 00:00:00.0',(1004,399.98)))
r[1][0]  # u'2013-08-27 00:00:00.0'
r[1][1] #(1004,399.98)
r[1][1][0] #1004
r[1][1][1] #399.98
# get the data like this ((u'2013-08-27 00:00:00.0',1004),399.98)
((r[1][0] ,r[1][1][0]),r[1][1][1])

# Computing daily revenue per Product
ordersJoinMap = ordersJoin.map(lambda r :((r[1][0] ,r[1][1][0]),r[1][1][1]))

ordersJoinMap.first()
ordersJoinMap.count()

dailyRevenuePerProductId = ordersJoinMap.\
reduceByKey(lambda total,revenue: total + revenue)
for i in dailyRevenuePerProductId.take(100): print(i)

#sanity check
dailyRevenuePerProductId.count()

total =0 
for i in range(1,100): total = total+i
total

for i in ordersMap.take(10):print(i)
#Aggregating data - if logic is different for intermediate values and final values then use this aggregateByKey
# computing daily revenue and daily count per product 
# revenue =0.0 
dailyRevenueAndCountPerProductId = ordersJoinMap.\
aggregateByKey((0.0,0),
lambda inter, revenue: (inter[0]+revenue, inter[1] + 1 ),
lambda final, inter: (inter[0]+final[0], inter[1]+final[1])
) 

for i in dailyRevenueAndPerProductId.take(100): print(i)

# get daily revenue per product using join
products = open("usha/data/retail_db/products/part-0000").read().splitlines()
#products is list not RDD
products[0]
# convert into RDD
productsRDD = sc.parallelize(products)
productsMap = productsRDD.map(lambda product: (int(product.split(",")[0]), product.split(",")[2]))
productsMap.first()

dailyRevenuePerProductId.first()
dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda rec: (rec[0][1], (rec[0][0], rec[1])))

dailyRevenuePerProductsMap = dailyRevenuePerProductIdMap.join(productsMap)
dailyRevenuePerProductName = dailyRevenuePerProductJoinProductsMap.map(lambda rec: 

elections = sc.textFile("data/retail_db/electionresults")
elections.take(5)




