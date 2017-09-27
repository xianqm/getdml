#coding=utf-8
import re    
import os
import pymysql
import time

def getDatabase(host,port,user,passwd):
	'''
	获取数据库列表，结果返回数据库元组
	'''
	conn = pymysql.connect(host=host,port=port,user=user,passwd=passwd)
	cur = conn.cursor()
	cur.execute("show databases")
	databases = cur.fetchall()
	databases_tup = ()
	for i in databases:
		databases_tup = databases_tup + (i[0],)
	cur.close()
	conn.close()
	return databases_tup
def getTablesName(host,port,user,passwd,db):
	'''
	获取某一数据库中的数据表，结果返回数据表元组
	'''
	tablesNameTuple = ()
	#忽略信息数据库
	if db != "information_schema":
		conn = pymysql.connect(host = host,port = port,user = user,passwd = passwd,db = db)
		cur = conn.cursor()
		cur.execute("show tables")
		tablesName = cur.fetchall()
		for i in tablesName:
			tablesNameTuple = tablesNameTuple + (i[0],)
		cur.close()
		conn.close()
	else:
		pass
	return tablesNameTuple
def getColName(host,port,user,passwd,db,tableName):
	'''
	获取数据表中的字段名，结果返回字段名的元组
	'''
	conn = pymysql.connect(host = host,port = port,user = user,passwd = passwd,db = db)
	cur = conn.cursor()
	cur.execute("desc " + str(tableName))
	ColName = cur.fetchall();
	#print(ColName)
	field = ()
	for i in ColName:
		field = field + (i[0],)
	cur.close()
	conn.close()
	return field
def getFileName(path): 
	'''
	获取path中的mysql-bin文件，忽略mysql-bin.index文件，结果返回文件列表
	'''
	fileName = os.popen('ls ' + path + '| grep mysql-bin.0').read()
	fileName = fileName.split()
	return fileName
def printDML(path,fileName,whole_dic):
	'''
	提取并打印binlog文件中的DML语句
	'''
	string = ''
	fp = os.popen('mysqlbinlog --base64-output=DECODE-ROWS -v ' + path + '/' + fileName)
	f = open("/home/xianqi/python_example/getdml/error.txt",'a+')
	#记录是否已经提取到###语句
	flag = 0
	#记录是否已经发生错误
	error_flag = 0

	data_byte = fp.buffer.readline()
	try:
		data = data_byte.decode(encoding='gbk')
	except:
		try:
			#'gbk'无法解码，尝试用'utf-8'解码
			data = data_byte.decode(encoding='utf-8')
		except UnicodeError as e:
			print("error:" + str(e),file = f)
			print("error!")
			error_flag = 1
	while data:
		#print(data)
		if re.match('###',data):
			string = string + data            
			#print(data)
			flag = 1
		elif re.match('# at',data) and flag == 1:
			string = string + data             
			#print(data)
			flag = 0
			if error_flag == 0:
				pattern = re.compile('###(.*?)# at',re.S)
				findResult = pattern.findall(string)
				result = ()
       	 
				for i in findResult:
					i = i.replace('###','')
					#i = re.compile('(\s+)').sub(' ',i)
					result = result + (i,)
				s = printStandardDML(result)
				#s = dmlreplace(whole_dic,s)
				print(s)
			
			string = ''
			error_flag = 0			
		else:
			pass

		data_byte = fp.buffer.readline()
		try:
			data = data_byte.decode(encoding='gbk')
		except UnicodeError as e:
			print("error:"+str(e)+",trying to use 'utf-8'",file = f)
			try:
				data = data_byte.decode(encoding='utf-8')
			except UnicodeError as e:	
				print("error:" + str(e)+",had tryed 'gbk' and 'utf-8'",file = f)
				print("error!")
				error_flag = 1
	f.close()

def getColDict(host,port,user,passwd,db,table):
	'''
	获取某一数据库某一表单的字段名，返回字典
	'''
	dic = {}
	field = getColName(host,port,user,passwd,db,table)
	#print(field)
	if field:
		dic[db+'.'+table] = list(tuple(field))
	return dic

def printStandardDML(lastResult):
	'''
	把提取到的dml语句变为标准的SQL语句
	'''
	for i in lastResult:
		#print(i)
		firstItem = i.split()[0]
		s = ''
		if firstItem == 'INSERT':
			#print('this is an insert sentence!')
			#print(i)
			dbandtable = re.compile("`(\w*?)`.`(\w*?)`").findall(i)
			db = dbandtable[0][0]
			table = dbandtable[0][1]
			key = re.compile("([\w@]*?)=.*?\n").findall(i)
			#计算字段的个数
			l =len(set(key))
			val = re.compile("[\w@]*?=(.*?)\n").findall(i)
			#print("%s,%s" %(key,val))
			i = re.compile("\n",re.S).sub(" ",i)
			#print("INSERT INTO %s.%s (" %(db,table),end='')
			s = s + "INSERT INTO "+db+"."+table+" ("
			for i in range(0,l):
				#print(key[i],end='')
				s = s + key[i]
				if i != l-1:
					#print(',',end='')
					s = s + ','
				else:
					#print(') ',end='')
					s = s + ') '
			#print("VALUES (",end='')
			s = s + "VALUES ("
			for i in range(1,len(val)+1):
				#print(val[i-1],end='')
				s = s + val[i-1]
				if i%l != 0:
					#print(',',end='')
					s = s+ ','
				elif i%l == 0 and i != len(val):
					#print('),(',end='')
					s = s + '),('
				else:
					#print(')')
					s = s + ')'
				
		elif firstItem == 'UPDATE':
			#print('this is an update sentence!')
			#print(i)
			dbandtable = re.compile("`(\w*?)`.`(\w*?)`").findall(i)
			db = dbandtable[0][0]
			table = dbandtable[0][1]
			val = re.compile("([\w@]*?=.*?)\n").findall(i)
			key = re.compile("([\w@]*?)=.*?\n").findall(i)
			i = re.compile("\n").sub(" ",i)
			#计算字段的个数
			l = len(set(key))
			#print("UPDATE %s.%s SET " %(db,table),end='')
			s = s + "UPDATE "+db+'.'+table+" SET "
			for i in range(0,l):
				#print(val[i+l],end='')
				s = s + val[i+l]
				if i != l-1:
					#print(",",end='')
					s = s + ","
				else:
					#print(' WHERE ',end='')
					s = s + ' WHERE '
			for i in range(0,l):
				#print(val[i],end='')
				s = s + val[i]
				if i != l-1:
					#print(",",end='')
					s = s + ","
				else:
					#print('')
					s = s + ''
		elif firstItem == 'DELETE':
			#print('this is a delete sentence!')
			#print(i)
			dbandtable = re.compile("`(\w*?)`.`(\w*?)`").findall(i)
			db = dbandtable[0][0]
			table = dbandtable[0][1]
			val = re.compile("([\w@]*?=.*?)\n").findall(i)
			i = re.compile("\n").sub(" ",i)
			#print(val)
			#print("DELETE FROM %s.%s WHERE " %(db,table),end='')
			s = s + "DELETE FROM "+db+'.'+table+" WHERE "
			for i in range(0,len(val)):
				#print(val[i],end='')
				s = s + val[i]
				if i != len(val)-1:
					#print(",",end='')
					s = s + ","
				else:
					#print('')
					s = s + ''
		else:
			pass

		#print(s)
	return s

def dmlreplace(whole_dic,string):
	host = '127.0.0.1'
	port = 3306
	user = "xianqi"
	passwd = "Xianqi123!"
	dbName = re.compile("\s(\w*)\.\w*\s").findall(string)
	tableName = re.compile("\s\w*\.(\w*)\s").findall(string)
	name = dbName[0] + '.' +tableName[0]
	#print("%s,%s,%s" %(dbName[0],tableName[0],name))
	
	if name in whole_dic:
		for j in range(0,len(whole_dic[name])):
			string = string.replace('@'+str(j+1),whole_dic[name][j])
	else:
		try:
			dic = getColDict(host,port,user,passwd,dbName[0],tableName[0])
			if dic:
				whole_dic = dict(whole_dic,**dic)
			else:
				return string
		except pymysql.err.OperationalError as e:
			print("error:"+str(e))
			return string
		for j in range(0,len(whole_dic[name])):
			string = string.replace('@'+str(j+1),whole_dic[name][j])
	
	return string
def main():
	whole_dic = {}
	path = "/home/xianqi/python_example"
	#path = "/home/mysql/mysql/data3306"
	#path = "/home/xianqi/python_example/mysqlbinlog"

	ftime = open('/home/xianqi/python_example/getdml/time.txt','r+')
	fileName = getFileName(path)
	print("path:"+path,file=ftime)
	start_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	print("start time:"+start_time,file=ftime)
	for i in range(0,len(fileName)):
        	print("------------------------------------------------------------%s------------------------------------------------------------------------" %(fileName[i]))
        	result = printDML(path,fileName[i],whole_dic)
        	print("----------------------------------------------------------------------------------------------------------------------------------------------------")
	end_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	print("end   time:"+end_time,file=ftime)
	ftime.close()

if __name__ == "__main__":
	main()
