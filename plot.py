#!/usr/bin/env python
# coding: utf-8

# In[3]:


import wordcloud
import matplotlib.pyplot as plt
import numpy as np


# In[19]:


#E3. 统计该国人口的年龄分布，年龄段分(0-18， 19-28， 29-38，39-48, 49-59，>60)

ages = [3933254,12912118,10906150,9512763,12347424]
ranges = ['19-28','29-38','39-48','49-59','>=60']

y_pos = np.arange(len(ranges))
plt.rcdefaults()
fig, ax = plt.subplots()
ax.barh(y_pos, ages, align='center', color='#f1939c')
ax.set_yticks(y_pos)
ax.set_yticklabels(ranges)
ax.invert_yaxis()
ax.set_xlabel('Population')
ax.set_title('Age Distribution of the country')
for x, y in enumerate(ages):
    plt.text(y , x, '%s' % y)
#去掉边框
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.show()


# In[38]:


#E4. 按月份，统计该国人口生日在每个月上的分布
num = [7824976,4688993,5124982,4184062,4217321,3463381,4249153,3170640,3450705,3434428,2912085,2828919]
month = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

# 绘图
plt.bar(range(12), num, align = 'center', color = '#0eb0c9', alpha = 0.8)
# 添加轴标签
plt.ylabel('Population')
# 添加标题
plt.title('Birth Month Distribution')
# 添加刻度标签
plt.xticks(range(12), month)
# 显示图形
plt.show()


# In[44]:


# E5. 统计一下该国的男女比例，男女人数
num = [24534483,25077226]
gender = ['Male','Female']
plt.figure(figsize=(4, 5))
# 绘图
plt.bar(range(2), num, align = 'center', color = '#f1939c',width=0.6)
# 添加轴标签
plt.ylabel('Population')
# 添加标题
plt.title('Population of Male and Female')
# 添加刻度标签
plt.xticks(range(2), gender)
# 显示图形
plt.show()


# In[48]:


#N1. 统计男性，女性最常见的10个姓，并用词云进行可视化展示
dic ={}
dic['YILMAZ'] = 352338
dic['KAYA'] = 244272
dic['DEMIR'] = 231289
dic['SAHIN'] = 201958
dic['CELIK'] = 199622
dic['YILDIZ'] = 195162
dic['YILDIRIM'] = 191966
dic['OZTURK'] = 178610
dic['AYDIN'] = 177894
dic['OZDEMIR'] = 164085
wc = wordcloud.WordCloud(background_color="white", max_words=1000)
# generate word cloud
wc.generate_from_frequencies(dic)

# show
plt.imshow(wc, interpolation="bilinear")
plt.axis("off")
plt.show()


# In[103]:


# E2 平均年龄和老龄化
def get_city_age(filename):
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip('\n')
            line = line.split(' ')
            city.append(line[0])
            avg_age.append(float(line[1]))
    return city, avg_age

def bar_1(city, avg_age):
    plt.figure(figsize=(8, 16))
    plt.barh(range(len(avg_age)), avg_age , align = 'center', color = '#0eb0c9', alpha = 0.8)
    # 添加轴标签
    plt.ylabel('City')
    plt.xlabel('Average Age')
    # 添加标题
    plt.title('Average Age of People in Different Cities')
    # 添加刻度标签
    plt.yticks(range(len(city)), city)
    # 显示图形
    plt.show()
    
def plot_city_age(filename):
    city, avg_age = get_city_age(filename)
    city_1 = city[0:40]
    city_2 = city[40:81]
    avg_age_1 = avg_age[0:40]
    avg_age_2 = avg_age[40:81]
    bar_1(city_1,avg_age_1)
    bar_1(city_2,avg_age_2)
    
filename = './results_data/avg_age.txt'
plot_city_age(filename)


# In[104]:


filename2 = './results_data/>60.txt'
plot_city_age(filename2)


# In[ ]:




