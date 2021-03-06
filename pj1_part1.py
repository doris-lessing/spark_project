import sys
from operator import add
import datetime
import numpy as np
from pyspark import SparkContext


def E1(lines):
    """
    统计土耳其公民中所有人中年龄最大的男人
    """
    male_birthday = lines.map(lambda x: x.split("\t"))\
                         .filter(lambda x: x[6] == 'E')\
                         .map(lambda x: (E1_helper(x[8]), x[2]+' '+x[3]))\
                         .sortByKey()
    # 读取第一个元素，即年龄最大的男人
    output = male_birthday.takeOrdered(100)
    for (date, name) in output:
        print(date, name)


def E1_helper(birthday):
    dates = birthday.split('/')
    if len(dates[0])==1:
        day='0'+dates[0]
    elif len(dates[0])==2:
        day=dates[0]
    else:
        day='99'
    if len(dates[1])==1:
        month='0'+dates[1]
    elif len(dates[1])==2:
        month=dates[1]
    else:
        month='99'
    year = dates[2]
    birthday_num=int(year+month+day)
    return birthday_num



def E1_2(lines):
    """
    统计数据中出生人数最少的日期和出生人数最多的日期
    """
    birthday = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (E1_2_helper(x[8]), 1))\
        .reduceByKey(add)\
        .sortBy(ascending=True, numPartitions=None, keyfunc=lambda x: x[1])
    output = birthday.collect()
    least_pop = output[0:10]
    most_pop = output[-1]
    print('least popular birthdate: ',least_pop)
    print('most popular birthdate: ',most_pop)

    birthday = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[8], 1))\
        .reduceByKey(add)\
        .sortBy(ascending=True, numPartitions=None, keyfunc=lambda x: x[1])
    output = birthday.collect()
    least_pop = output[0:10]
    most_pop = output[-1]
    print('least popular birthday: ',least_pop)
    print('most popular birthday: ',most_pop)


def E1_2_helper(birthday):
    dates = birthday.split('/')
    birthdate = dates[0]+'/'+dates[1]
    return birthdate


def E2(lines):
    """
    统计姓名中最常出现的字母
    """
    names = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: x[2]+x[3])\
        .flatMap(lambda x: list(x)) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x: x[1])

    output = names.collect()
    print('Most common letter in names', output[0])


def E3(lines):
    """
    统计该国人又的年龄分布，年龄段分(0-18， 19-28， 29-38，39-48, 49-59，>60)
    """
    age_range = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (E3_helper(x[8]),1)) \
        .reduceByKey(add)
    output = age_range.collect()
    print('Age distribution:')
    for (age, num) in output:
        print(age, num)

def E3_helper(birthday):
    """
    根据生日计算年龄段
    """
    age = get_age(birthday)

    if age <= 18:
        age_range = '0-18'
    elif age <= 28:
        age_range = '19-28'
    elif age <= 38:
        age_range = '29-38'
    elif age <= 48:
        age_range = '39-48'
    elif age <= 59:
        age_range = '49-59'
    elif age >= 60:
        age_range = '>=60'

    return age_range


def get_age(birthday):
    birth_year = int(birthday.split('/')[2])
    age = 2016 - birth_year  # 此数据集获取于2016,所以年龄用2016算
    return age


def E4(lines):
    """
    按月份，统计该国人口生日在每个月上的分布
    """
    birthmonth = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[8].split('/')[1], 1)) \
        .reduceByKey(add)

    output = birthmonth.collect()
    print('birth month distribution')
    for (char, num) in output:
        print(char, num)


def E5(lines):
    """
    统计一下该国的男女比例，男女人数
    """
    gender = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[6], 1)) \
        .reduceByKey(add)

    output = gender.collect()
    print('Male and Females')
    for (char, num) in output:
        print(char, num)


def N1(lines):
    """
    统计男性，女性最常见的10个姓，并用词云进行可视化展示
    """
    male_surname = lines.map(lambda x: x.split("\t")) \
        .filter(lambda x: x[6] == 'E') \
        .map(lambda x: (x[3],1)) \
        .reduceByKey(add) \
        .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x: x[1])
    male_output = male_surname.collect()

    female_surname = lines.map(lambda x: x.split("\t")) \
        .filter(lambda x: x[6] == 'K') \
        .map(lambda x: (x[3],1)) \
        .reduceByKey(add) \
        .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x: x[1])
    female_output = female_surname.collect()

    print('Male Surnames')
    i = 0
    for (char, num) in male_output:
        print(char, num)
        i += 1
        if i == 10:
            break

    i = 0
    print('Female Surnames')
    for (char, num) in female_output:
        print(char, num)
        i += 1
        if i == 10:
            break


def N2_3(lines):
    """
    统计每个城市市民的平均年龄，统计分析每个城市的人口老龄化程度，
    判断当前城市是否处 于老龄化社会，
    (当一个国家或地区60岁以上老年人又占人又总数的10%，或65岁以上老年人占人口总数的7%，
    即意味着这个国家或地区的人又处于老龄化社会)
    说一下该国平均人口最年轻的5个城市
    """

    # 计算各城市人口平均年龄
    city_age = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[11], get_age(x[8]))) \
        .groupByKey() \
        .mapValues(N2_3_helper_1) \
        .sortBy(ascending=True, numPartitions=None, keyfunc=lambda x: x[1])
    output = city_age.collect()
    for (char, num) in output:
        print(char, num)

    # 计算各城市人口60岁以上人口占比
    city_old_1 = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[11], get_age(x[8]))) \
        .groupByKey() \
        .mapValues(N2_3_helper_2) \
        .sortBy(ascending=True, numPartitions=None, keyfunc=lambda x: x[1])
    output = city_old_1.collect()
    for (char, num) in output:
        print(char, num)
    # 计算各城市人口65岁以上人口占比
    city_old_2 = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[11], get_age(x[8]))) \
        .groupByKey() \
        .mapValues(N2_3_helper_3) \
        .sortBy(ascending=True, numPartitions=None, keyfunc=lambda x: x[1])
    output = city_old_2.collect()
    for (char, num) in output:
        print(char, num)

def N2_3_helper_1(l):
    sum=0
    count=0
    for i in l:
        sum += i
        count += 1
    return sum/count

def N2_3_helper_2(l):
    total=0
    old =0
    for i in l:
        if i >60:
            old +=1
        total +=1
    return old/total

def N2_3_helper_3(l):
    total=0
    old =0
    for i in l:
        if i >65:
            old +=1
        total +=1
    return old/total

def N4_5(lines):
    """
    统计一下该国前10大人口城市中，每个城市的前3大姓氏，并分析姓氏与所在城市是否具有相关性
    计算一下该国前10大人口城市中，每个城市的人口生日最集中分布的是哪2个月
    """
    # 统计前10大人口城市
    city_population = lines.map(lambda x: x.split("\t")) \
        .map(lambda x: (x[11], 1))\
        .reduceByKey(add) \
        .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x: x[1])\
        .map(lambda x: x[0])

    city_population = city_population.collect()
    top_10_city = city_population[0:10]

    city_surname = lines.map(lambda x: x.split("\t"))\
        .filter(lambda x: x[11] in top_10_city)\
        .map(lambda x: (x[11], x[3]))\
        .groupByKey() \
        .mapValues(N4_5_helper_1)

    output = city_birthmonth.collect()
    for (city, surnames) in output:
        print(city, surnames)


    city_birthmonth = lines.map(lambda x: x.split("\t")) \
        .filter(lambda x: x[11] in top_10_city) \
        .map(lambda x: (x[11], x[8].split('/')[1])) \
        .groupByKey() \
        .mapValues(N4_5_helper_2)

    output = city_birthmonth.collect()
    for (city, surnames) in output:
        print(city, surnames)


def N4_5_helper_1(l):
    dic = {}
    for i in l:
        if i in dic.keys():
            dic[i] +=1
        else:
            dic[i] = 1
    res = sorted(dic.items(), key=lambda x: x[1], reverse=True)[0:3]
    return res

def N4_5_helper_2(l):
    dic = {}
    for i in l:
        if i in dic.keys():
            dic[i] +=1
        else:
            dic[i] = 1
    res = sorted(dic.items(), key=lambda x: x[1], reverse=True)[0:2]
    return res


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount<file>"
        exit(-1)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    E1(lines)
    E1_2(lines)
    E2(lines)
    E3(lines)
    E4(lines)
    E5(lines)
    N1(lines)
    N2_3(lines)
    N4_5(lines)
    sc.stop()
