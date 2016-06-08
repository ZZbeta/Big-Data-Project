from pyspark import SparkContext
sc=SparkContext(appName="validation")
import datetime
import dateutil.parser as par

def tryparse(date):
    kwargs={}
    try:
        parsedate=par.parse(date,**kwargs)
    except ValueError:
        parsedate='icannt'
    if parsedate != 'icannt':
        parsedate=parsedate.strftime('%d-%m-%Y')
        return parsedate
    else:
        return date

class MetaFileHandler:
    def __init__(self, file_name):
        self.file_name=file_name
    def meta_kv_mapper(self, x):
        return (x[0], x[2]),(x[1],x[4])
    def meta_validate_fields(self, x):
        err = ''
        i=0
        time1=tryparse(x[1][0])
        time2=tryparse(x[1][1])
        if len(time1)==9:
            i=6
        else:
            i=4
        if(time1[i:]<'2015'):
            err = err + 'field 1 date is too old'
        if(time2[i:]<'2015'):
            err = err + 'field 4 date is too old'
        return (x[0], err)
    def meta_kv_to_records(self, x):
        return [x[0][0], x[0][1], x[1]]

meta = MetaFileHandler('testdata.txt')
rdd=sc.textFile(meta.file_name)
rdd=rdd.map(lambda x:x.split(' '))
rdd_kv = rdd.map(lambda x: meta.meta_kv_mapper(x))
rdd_kv.collect()
rdd_kv_invalid = rdd_kv.map(lambda x: meta.meta_validate_fields(x)).filter(lambda x: x[1]     > '')
rdd_kv_invalid.collect()
rdd_error=rdd_kv_invalid.flatMap(lambda x: meta.meta_kv_to_records(x))
print rdd_error.collect()
