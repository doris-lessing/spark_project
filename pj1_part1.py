import sys
from operator import add
import datetime

from pyspark import SparkContext



def E1(lines):
    """
    统计土耳其公民中所有人中年龄最大的男人
    """
    male_birthday = lines.map(lambda x: x.split("\t"))\
                         .filter(lambda x: x[6] == 'E')\
                         .map(lambda x: (datetime.datetime.strptime(x[8], '%d/%m/%Y'), x[2]+' '+x[3]))\
                         .groupByKey()\
                         .sortByKey(ascending=False)
    # 读取第一个元素，即年龄最大的男人
    output = male_birthday.top(1)
    for (date, name) in output:
        print(date, name[0])


def E1_2(lines):
    """
    统计数据中出生人数最少的日期和出生人数最多的日期
    """

def E2(lines):
    """
    统计姓名中最常出现的字母
    """

def E3(lines):
    """
    统计该国人又的年龄分布，年龄段分(0-18， 19-28， 29-38，39-48, 49-59，>60)
    """


def E4(lines):
    """
    按月份，统计该国人又生日在每个月上的分布
    """


def E5(lines):
    """
    统计一下该国的男女比例，男女人数
    """


def N1(lines):
    """
    统计男性，女性最常见的10个姓，并用词云进行可视化展示
    """

def N2_3(lines):
    """
    统计每个城市市民的平均年龄，统计分析每个城市的人口老龄化程度，
    判断当前城市是否处 于老龄化社会，
    (当一个国家或地区60岁以上老年人又占人又总数的10%，或65岁以上老年人占人又总数的7%，
    即意味着这个国家或地区的人又处于老龄化社会)
    说一下该国平均人又最年轻的5个城市
    """

def N4(lines):
    """
    统计一下该国前10大人又城市中，每个城市的前3大姓氏，并分析姓氏与所在城市是否具有相关性
    """


def N5(lines):
    """
    计算一下该国前10大人又城市中，每个城市的人又生日最集中分布的是哪2个月
    """


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount<file>"
        exit(-1)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    E1(lines)
    sc.stop()
