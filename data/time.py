import matplotlib
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['font.sans-serif']=['SimHei'] # 解决中文乱码

labels = ['均匀分布', '指数分布', 'pareto分布']
mapreduce_time = [4669.495685000003, 3572.682625000002, 2898.367314999999]
static_cdc_time = [2910.585019999999, 2606.0880599999977, 2439.6302800000007]
dynamic_cdc_time = [2038.8249999999995, 1727.8990899999982, 1637.348660000001]

x = np.arange(len(labels))  # 标签位置
width = 0.22  # 柱状图的宽度，可以根据自己的需求和审美来改

fig, ax = plt.subplots()
rects1 = ax.bar(x - width, mapreduce_time, width, label='MapReduce方案',color='darkorange')
rects2 = ax.bar(x , static_cdc_time, width, label='CDC方案',color='green')
rects3 = ax.bar(x + width, dynamic_cdc_time, width, label='DCDC方案',color='skyblue')
# rects1 = ax.bar(x - width, mapreduce_time, width, label='MapReduce方案', color='white', edgecolor='black', hatch='///')
# rects2 = ax.bar(x, static_cdc_time, width, label='CDC方案', color='white', edgecolor='black', hatch='||')
# rects3 = ax.bar(x + width, dynamic_cdc_time, width, label='DCDC方案', color='white', edgecolor='black', hatch='\\\\')


# 为y轴、标题和x轴等添加一些文本。
plt.xlabel('任务执行时间/ms', fontsize=12)
plt.ylabel('任务执行时间/ms', fontsize=12)
# ax.set_title('任务执行时间对比图')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

fig.tight_layout()

plt.savefig("time.svg", dpi=300, bbox_inches='tight')
for i in range(3):
	print((dynamic_cdc_time[i] - mapreduce_time[i])/mapreduce_time[i])
	print((dynamic_cdc_time[i] - static_cdc_time[i])/static_cdc_time[i])
