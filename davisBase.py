import struct
import re
import os.path
#######global_variables######
prompt = 'davisql>'
currentSchema='information_schema'
#######define_functions######
def version():
    print 'DavisBaseLite v1.0'
    
def splashScreen():
    print '*'*80
    print 'Welcome to DavisBaseLite'
    version()
    print 'Type "help;" to display supported commands.'
    print '*'*80

def displayAllSchemas():
    schemataTableFile = open("information_schema.schemata.tbl","r+b")
    record=schemataTableFile.read()
    i=0
    while i<len(record):
        b=struct.unpack('>B',record[i])
        x=struct.unpack('>'+str(b[0])+"s",record[i+1:i+b[0]+1])
        print x[0]
        i=i+b[0]+1

def displayAllTables(currentSchema):
    #TABLE tables
    tablesTableFile = open("information_schema.tables.tbl","r+b")
    record=tablesTableFile.read()
    i=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        ind=struct.unpack('>l',record[i:i+8])
        i=i+8
        if x1[0]==currentSchema:
            print x2[0]

def checkSchema(x):
    schemataTableFile = open("information_schema.schemata.tbl","r+b")
    record=schemataTableFile.read()
    i=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        if x1[0]==x:
            return 1
        i=i+b1[0]+1
    return 0

def pack(dataType,f,x):
    if dataType=='byte':
        pack_byte(f,x)
    elif dataType=='short' or dataType=='short int':
        pack_short(f,x)
    elif dataType=='int':
        pack_int(f,x)
    elif dataType=='long' or dataType=='long int':
        pack_long(f,x)
    elif dataType[0]=='c':
        re1='.*?'	# Non-greedy match on filler
        re2='(\\d+)'	# Integer Number 1

        rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
        m = rg.search(dataType)
        if m:
            n=m.group(1)
        pack_char(f,x,n)
    elif dataType[0]=='v':
        if x[0]=="'":
            x=x[1:-1]
        pack_varchar(f,x)
    elif dataType=='float':
        pack_float(f,x)
    elif dataType=='double':
        pack_double(f,x)
    elif dataType=='datetime':
        date=long(x[0:4])*10000000000+long(x[5:7])*100000000+long(x[8:10])*1000000+long(x[11:13])*10000+long(x[14:16])*100+long(x[17:19])
        pack_long(f,date)
    elif dataType=='date':
        date=long(x[0:4])*10000+long(x[5:7])*100+long(x[8:10])
        pack_long(f,date)
    
def pack_varchar(f,s):
    f.write(struct.pack('>B',len(s)))
    for i in range(len(s)):
        f.write(struct.pack('>c',s[i]))

def pack_int(f,i):
    f.write(struct.pack('>i',int(i)))

def pack_byte(f,x):
    f.write(struct.pack('>B',x))

def pack_short(f,x):
    f.write(struct.pack('>h',int(x)))

def pack_long(f,x):
    f.write(struct.pack('>q',x))
 
def pack_char(f,x,n):
    i=0
    while i<n:
        if i>=len(x):
            f.write(struct.pack('>c','\0'))
        else:
            f.write(struct.pack('>c',x[i]))
        i+=1

def pack_float(f,x):
    f.write(struct.pack('>f',x))

def pack_double(f,x):
    f.write(struct.pack('>d',x))

def unpack(dataType,record,i):
    if dataType=='byte':
        return unpack_byte(record,i)
    elif dataType=='short' or dataType=='short int':
        return unpack_short(record,i)
    elif dataType=='int':
        return unpack_int(record,i)
    elif dataType=='long' or dataType=='long int':
        return unpack_long(record,i)
    elif dataType[0]=='c':
        re1='.*?'	# Non-greedy match on filler
        re2='(\\d+)'	# Integer Number 1

        rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
        m = rg.search(dataType)
        if m:
            n=m.group(1)
        return unpack_char(record,i,n)
    elif dataType[0]=='v':
        return unpack_varchar(record,i)
    elif dataType=='float':
        return unpack_float(record,i)
    elif dataType=='double':
        return unpack_double(record,i)
    elif dataType=='datetime':
        x,i = unpack_long(record,i)
        date=str(x)[0:4]+'-'+str(x)[4:6]+'-'+str(x)[6:8]+'_'+str(x)[8:10]+':'+str(x)[10:12]+':'+str(x)[12:14]
        return date,i
    elif dataType=='date':
        x,i = unpack_long(record,i)
        date=str(x)[0:4]+':'+str(x)[4:6]+':'+str(x)[6:8]
        return date,i
    
def unpack_varchar(record,i):
    b1=struct.unpack('>B',record[i])
    x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
    i=i+b1[0]+1
    return str(x1[0]),i

def unpack_int(record,i):
    x=struct.unpack('>i',record[i:i+4])
    i=i+4
    return int(x[0]),i

def unpack_byte(record,i):
    x=struct.unpack('>B',record[i])
    i=i+1
    return x[0],i

def unpack_short(record,i):
    x=struct.unpack('>h',record[i:i+2])
    i=i+2
    return int(x[0]),i

def unpack_long(record,i):
    x=struct.unpack('>q',record[i:i+8])
    i=i+8
    return long(x[0]),i

def unpack_char(record,j,n):
    x=''
    i=j
    while i<j+n:
        y=struct.unpack('>c',record[j]);
        if y[0]!='\0':
            x=x+y[0]
        i+=1
    return str(x),i-1

def unpack_float(record,i):
    x=struct.unpack('>f',record[i:i+4])
    i=i+4
    return float(x[0]),i

def unpack_double(record,i):
    x=struct.unpack('>d',record[i:i+8])
    i=i+8
    return float(x[0]),i

def createSchema(x):
    schemataTableFile = open("information_schema.schemata.tbl","a+b")
    pack_varchar(schemataTableFile,x)
    schemataTableFile.close()
    updateTableFile('schemata')

def createTable(txt):
    re1='.*?'	# Non-greedy match on filler
    re2='(?:[a-z][a-z0-9_]*)'	# Uninteresting: var
    re3='.*?'	# Non-greedy match on filler
    re4='(?:[a-z][a-z0-9_]*)'	# Uninteresting: var
    re5='.*?'	# Non-greedy match on filler
    re6='((?:[a-z][a-z0-9_]*))'	# Variable Name 1
    re7='.*?'	# Non-greedy match on filler
    re8='(\\(.*\\))'	# Round Braces 1

    rg = re.compile(re1+re2+re3+re4+re5+re6+re7+re8,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    if m:
        var1=m.group(1)
        rbraces1=m.group(2)

    columns=rbraces1[1:-1].split(',')
    tableName=var1

    tablefilestr=currentSchema+'.'+tableName+'.'+'tbl'
    createTableFile = open(tablefilestr,"a+b")
    createTableFile.close()
    
    tablesTableFile = open("information_schema.tables.tbl","a+b")
    pack_varchar(tablesTableFile,currentSchema)
    pack_varchar(tablesTableFile,tableName)
    pack_long(tablesTableFile,0)
    tablesTableFile.close()

    columnTableFile = open("information_schema.columns.tbl","a+b")
    i=0
    for col in columns:
        i=i+1
        flag=0
        columnName=col.split()[0]
        pack_varchar(columnTableFile,currentSchema)
        pack_varchar(columnTableFile,tableName)
        pack_varchar(columnTableFile,columnName)
        indexFile = open(currentSchema+'.'+tableName+'.'+columnName+'.ndx',"a+b")
        indexFile.close()
        pack_int(columnTableFile,i)
        columnType=col.split()[1]
        if columnType=='short':
            if len(col.split())>2:
                if col.split()[2]=='int':
                    pack_varchar(columnTableFile,'short int')
                    flag=1
                else:
                    pack_varchar(columnTableFile,'short')
            else:
                pack_varchar(columnTableFile,'short')
        elif columnType=='long':
            if len(col.split())>2:
                if col.split()[2]=='int':
                    pack_varchar(columnTableFile,'long int')
                    flag=1
                else:
                    pack_varchar(columnTableFile,'long')
            else:
                pack_varchar(columnTableFile,'long')
        else:
            pack_varchar(columnTableFile,columnType)

        if len(col.split())>2:
            if flag==0:
                if col.split()[2]=='not':
                    pack_varchar(columnTableFile,'NO')
                    pack_varchar(columnTableFile,'')
                elif col.split()[2]=='primary':
                    pack_varchar(columnTableFile,'NO')
                    pack_varchar(columnTableFile,'PRI')
                else:
                    pack_varchar(columnTableFile,'YES')
                    pack_varchar(columnTableFile,'')
            elif flag==1:
                if len(col.split())>3:
                    if col.split()[3]=='not':
                        pack_varchar(columnTableFile,'NO')
                        pack_varchar(columnTableFile,'')
                    elif col.split()[3]=='primary':
                        pack_varchar(columnTableFile,'NO')
                        pack_varchar(columnTableFile,'PRI')
                    else:
                        pack_varchar(columnTableFile,'YES')
                        pack_varchar(columnTableFile,'')
                else:
                    pack_varchar(columnTableFile,'YES')
                    pack_varchar(columnTableFile,'')
            else:
                pack_varchar(columnTableFile,'YES')
                pack_varchar(columnTableFile,'')
        else:
            pack_varchar(columnTableFile,'YES')
            pack_varchar(columnTableFile,'')
        updateTableFile('columns')
    columnTableFile.close()
    updateTableFile('tables')
    
def checkTable(tableName):
    tablesTableFile = open("information_schema.tables.tbl","r+b")
    record=tablesTableFile.read()
    i=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        ind=struct.unpack('>q',record[i:i+8])
        i=i+8
        if x1[0].lower()==currentSchema.lower() and x2[0].lower()==tableName.lower():
            return 1
    return 0

def typeCasted(value,columnType):
    if columnType=='int' or columnType=='short' or columnType=='short int':
        return int(value)
    elif columnType=='long' or columnType=='long int':
        return long(value)
    elif columnType=='float' or columnType=='double':
        return float(value)
    elif columnType[0]=='c' or columnType[0]=='v':
        return str(value)[1:-1]
    elif columnType=='datetime' or columnType=='date':
        return str(value)[1:-1]
    
def updatendxFile(tableName,columnName,value,offset,columnType):
    
    indexFile = open(currentSchema+'.'+tableName+'.'+columnName+'.ndx',"a+b")
    indexFile.close()
    indexFile = open(currentSchema+'.'+tableName+'.'+columnName+'.ndx',"r+b")
    record = indexFile.read()
    indexFile.seek(0)
    i=0
    flag=0
    treemap={}
    key=''
    while i<len(record):
        key,i = unpack(columnType,record,i)
        num,i = unpack('int',record,i)
        off,i = unpack('int',record,i)
        treemap[key]=[num,off]
        keys=treemap.keys()
        keys.sort()
        
    
    if len(record)!=0:
        for x in keys:
            if x==typeCasted(value,columnType):
                tup = treemap[x]
                tup[0]+=1
                tup.append(offset)
                treemap[x]=tup
                flag=1

    if flag==0:
        treemap[typeCasted(value,columnType)]=[1,offset]

    keys=treemap.keys()
    keys.sort()
    for x in keys:
        pack(columnType,indexFile,x)
        tup=treemap[x]
        for y in tup:
            pack('int',indexFile,y)

    indexFile.close()

def checkPrimaryKey(tableName,columnName,columnType,checkValue):
    if os.path.isfile(currentSchema+'.'+tableName+'.'+columnName+'.ndx')==False:
        return True
    ndxFile=open(currentSchema+'.'+tableName+'.'+columnName+'.ndx','r+b')
    record=ndxFile.read()
    i=0
    while i<len(record):
        value,i=unpack(columnType,record,i)
        num,i=unpack('int',record,i)
        loc=[]
        j=0
        while j<num:
            temp,i=unpack('int',record,i)
            loc.append(temp)
            j=j+1
        if value==checkValue:
            return False
    return True

def updateTableFile(tableName):
    tablesTableFile = open("information_schema.tables.tbl","r+b")
    record=tablesTableFile.read()
    i=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        ind=struct.unpack('>q',record[i:i+8])
        i=i+8
        if x1[0].lower()==currentSchema.lower() and x2[0].lower()==tableName.lower():
            i=i-8
            tablesTableFile.seek(i);
            ind=long(ind[0])+1;
            pack('long int',tablesTableFile,ind)
            break;
    tablesTableFile.close()

def insertInto(txt):

    re1='.*?'	# Non-greedy match on filler
    re2='(?:[a-z][a-z0-9_]*)'	# Uninteresting: var
    re3='.*?'	# Non-greedy match on filler
    re4='(?:[a-z][a-z0-9_]*)'	# Uninteresting: var
    re5='.*?'	# Non-greedy match on filler
    re6='((?:[a-z][a-z0-9_]*))'	# Variable Name 1

    rg = re.compile(re1+re2+re3+re4+re5+re6,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    if m:
        tableName=m.group(1)

    if checkTable(tableName)==0:
        print 'table not found in the currentschema'
        return
    
    re1='.*?'	# Non-greedy match on filler
    re2='(\\(.*\\))'	# Round Braces 1

    rg = re.compile(re1+re2,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    if m:
        rbraces1=m.group(1)

    values=rbraces1[1:-1].split(',')
    columnTableFile = open("information_schema.columns.tbl","r+b")
    record=columnTableFile.read()
    i=0
    j=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        b3=struct.unpack('>B',record[i])
        x3=struct.unpack('>'+str(b3[0])+"s",record[i+1:i+b3[0]+1])
        i=i+b3[0]+1
        ind=struct.unpack('>i',record[i:i+4])
        i=i+4
        b4=struct.unpack('>B',record[i])
        x4=struct.unpack('>'+str(b4[0])+"s",record[i+1:i+b4[0]+1])
        i=i+b4[0]+1
        b5=struct.unpack('>B',record[i])
        x5=struct.unpack('>'+str(b5[0])+"s",record[i+1:i+b5[0]+1])
        i=i+b5[0]+1
        b6=struct.unpack('>B',record[i])
        x6=struct.unpack('>'+str(b6[0])+"s",record[i+1:i+b6[0]+1])
        i=i+b6[0]+1
        if x1[0]==currentSchema and x2[0]==tableName:
            if j==0:
                tblFile = open(currentSchema+'.'+tableName+'.tbl',"a+b")
                temprecord=tblFile.read()
                offset=tblFile.tell()
                tblFile.close()
            tblFile = open(currentSchema+'.'+tableName+'.tbl',"a+b")
            if x6[0]=='PRI':
                if checkPrimaryKey(tableName,x3[0],x4[0],typeCasted(values[j],x4[0]))==False:
                    print 'Primary Key Error'
                    return
            if x5[0]=='NO' and values[j]=='':
                print 'Null error'
                return
            if x5[0]=='Yes' and values[j]=='':
                if columnType=='int' or columnType=='short' or columnType=='short int':
                    values[j]=0
                elif columnType=='long' or columnType=='long int':
                    values[j]=0
                elif columnType=='float' or columnType=='double':
                    values[j]=0.0
                elif columnType[0]=='c' or columnType[0]=='v':
                    values[j]='0'
                elif columnType=='datetime' or columnType=='date':
                    values[j]='0000-00-00'
            if x4[0]=='date':
                if len(values[j])!=12:
                    print x4[0],values[j],len(values[j])
                    print 'date format incorrect'
                    return
            elif x4[0]=='datetime':
                if len(values[j])!=21:
                    print values[j]
                    print 'datetime format incorrect'
                    return
            j=j+1
    columnTableFile.close()            

    columnTableFile = open("information_schema.columns.tbl","r+b")
    record=columnTableFile.read()
    i=0
    j=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        b3=struct.unpack('>B',record[i])
        x3=struct.unpack('>'+str(b3[0])+"s",record[i+1:i+b3[0]+1])
        i=i+b3[0]+1
        ind=struct.unpack('>i',record[i:i+4])
        i=i+4
        b4=struct.unpack('>B',record[i])
        x4=struct.unpack('>'+str(b4[0])+"s",record[i+1:i+b4[0]+1])
        i=i+b4[0]+1
        b5=struct.unpack('>B',record[i])
        x5=struct.unpack('>'+str(b5[0])+"s",record[i+1:i+b5[0]+1])
        i=i+b5[0]+1
        b6=struct.unpack('>B',record[i])
        x6=struct.unpack('>'+str(b6[0])+"s",record[i+1:i+b6[0]+1])
        i=i+b6[0]+1
        if x1[0]==currentSchema and x2[0]==tableName:
            if j==0:
                tblFile = open(currentSchema+'.'+tableName+'.tbl',"a+b")
                temprecord=tblFile.read()
                offset=tblFile.tell()
                tblFile.close()
            tblFile = open(currentSchema+'.'+tableName+'.tbl',"a+b")
            pack(x4[0],tblFile,typeCasted(values[j],x4[0]))
            tblFile.close()
            updatendxFile(tableName, x3[0],values[j], offset, x4[0])
            j=j+1
            
    columnTableFile.close()
    

def getTuple(tableFile,columnInfo,loc):
    tableFile.seek(0)
    tableFile.seek(loc)
    record=tableFile.read()
    keys=columnInfo.keys()
    keys.sort()
    i=0
    tup=[]
    for x in keys:
        columnName,columnType=columnInfo[x]
        value,i=unpack(columnType,record,i)
        tup.append([columnName,value])
    return tup

def displayTuples(tuples,columnInfo):
    numTuples=len(tuples)
    numColumns=len(columnInfo)
    print '='*25*numColumns
    keys=columnInfo.keys()
    keys.sort()
    for x in keys:
        print '|',columnInfo[x][0].rjust(20),'|',
    i=0
    print 
    while i<numTuples:
        for x in keys:
            columnName=columnInfo[x][0]
            j=0
            while j<numColumns:
                if tuples[i][j][0]==columnName:
                    print '|',str(tuples[i][j][1]).rjust(20),'|',
                j+=1
        print 
        i+=1
    print '='*25*numColumns
    
def selectFromWhere(txt):
    tuples=[]
    if len(txt.split())<4:
        print 'i can not understand the command'
        return
    
    if txt.split()[1]!='*' or txt.split()[2]!='from':
        print 'i can not understand the command'
        return

    tableName=txt.split()[3]

    columnTableFile = open("information_schema.columns.tbl","r+b")
    record=columnTableFile.read()
    columnInfo={}
    i=0
    while i<len(record):
        b1=struct.unpack('>B',record[i])
        x1=struct.unpack('>'+str(b1[0])+"s",record[i+1:i+b1[0]+1])
        i=i+b1[0]+1
        b2=struct.unpack('>B',record[i])
        x2=struct.unpack('>'+str(b2[0])+"s",record[i+1:i+b2[0]+1])
        i=i+b2[0]+1
        b3=struct.unpack('>B',record[i])
        x3=struct.unpack('>'+str(b3[0])+"s",record[i+1:i+b3[0]+1])
        i=i+b3[0]+1
        ind=struct.unpack('>i',record[i:i+4])
        i=i+4
        b4=struct.unpack('>B',record[i])
        x4=struct.unpack('>'+str(b4[0])+"s",record[i+1:i+b4[0]+1])
        i=i+b4[0]+1
        b5=struct.unpack('>B',record[i])
        x5=struct.unpack('>'+str(b5[0])+"s",record[i+1:i+b5[0]+1])
        i=i+b5[0]+1
        b6=struct.unpack('>B',record[i])
        x6=struct.unpack('>'+str(b6[0])+"s",record[i+1:i+b6[0]+1])
        i=i+b6[0]+1
        if x1[0]==currentSchema and x2[0]==tableName:
            columnInfo[ind[0]]=[x3[0],x4[0]]
            
    tblFile = open(currentSchema+'.'+tableName+'.tbl',"r+b")
    if len(txt.split())==4:
        record=tblFile.read()
        tup=[]
        keys=columnInfo.keys()
        keys.sort()
        i=0
        while i<len(record):
            tup=[]
            for x in keys:
                value,i=unpack(columnInfo[x][1],record,i)
                tup.append([columnInfo[x][0],value])
            tuples.append(tup)

        displayTuples(tuples,columnInfo)
        columnTableFile.close()

    else:
        if currentSchema=='information_schema':
            print 'no where in using information schema'
            return
        if txt.split()[4]!='where':
            print 'where missing'
            return
        
        temp=txt[(txt.index('where')+6):]
        
        re1='((?:[a-z][a-z0-9_]*))'	# Variable Name 1

        rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
        m = rg.search(temp)
        if m:
            columnName=m.group(1)
        else:
            print 'enter a valid syntax'
            return

        temp=re.split('(=|>|<|>=|<=|!=)',temp)
        if len(temp)==3:
            op=temp[1]
            abc=temp[2]
        elif len(temp)==5:
            op=temp[1]+temp[3]
            abc=temp[4]
        else:
            print 'error in command'

        columnNdxFileTxt=currentSchema+'.'+tableName+'.'+columnName+'.ndx'
        if os.path.isfile(columnNdxFileTxt)==False:
            print 'columnName in where is incorrect. Modify the command and try again'

        keys=columnInfo.keys()
        keys.sort()
        for x in keys:
            if columnInfo[x][0]==columnName:
                columnType=columnInfo[x][1]

        ndxFile = open(columnNdxFileTxt,"r+b")
        record=ndxFile.read()
        i=0
        while i<len(record):
            value,i=unpack(columnType,record,i)
            num,i=unpack('int',record,i)
            loc=[]
            j=0
            while j<num:
                temp,i=unpack('int',record,i)
                loc.append(temp)
                j=j+1
            if op=='=':
                if value==typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
            elif op=='>':
                if value>typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
            elif op=='<':
                if value<typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
            elif op=='>=':
                if value>=typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
            elif op=='<=':
                if value<=typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
            elif op=='!=':
                if value!=typeCasted(abc,columnType):
                    for l in loc:
                        tuples.append(getTuple(tblFile,columnInfo,l))
    
        displayTuples(tuples,columnInfo)
        ndxFile.close()
    tblFile.close()
        
                
    
############main#############
print ''
splashScreen()
flag=1
while flag:
    print prompt,
    cmd=raw_input()
    
    if cmd[-1]!=';':
        print '; <-missing'
        continue;
    else:
        cmd=cmd[:-1]

    x=cmd.split()[0].lower()
    if x=='show':
        x=cmd.split()[1].lower()
        if x=='schemas':
            displayAllSchemas()
        elif x=='tables':
            displayAllTables(currentSchema)
        else:
            print "I didn't understand the command"
            
    elif x=='use':
        x=cmd.split()[1].lower()
        if checkSchema(x):
            currentSchema=x
        else:
            print 'Schema/Database not found'
            
    elif x=='create':
        x=cmd.split()[1].lower()
        if x=='schema':
            x=cmd.split()[2].lower()
            createSchema(x.lower())
        elif x=='table':
            createTable(cmd.lower())
        else:
            print "I didn't understand the command"
            
    elif x=='insert':
        if cmd.split()[1].lower()=='into':
            insertInto(cmd.lower())
        else:
            print "I didn't understand the command"
    elif x=='select':
        selectFromWhere(cmd.lower())
    elif x=='exit':
        print 'Bye'
        flag=0
    else:
        print "I didn't understand the command"
