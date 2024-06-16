import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties 
font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14) 

# 读取文件并解析数据

cdc_exponential_map_times = []
cdc_exponential_shuffle_times = []
static_cdc_exponential_map_times = []
static_cdc_exponential_shuffle_times = []

           
with open('cdc_exponential.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            cdc_exponential_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            cdc_exponential_shuffle_times.append(float(parts[1]))
    
with open('static_cdc_expoential.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            static_cdc_exponential_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            static_cdc_exponential_shuffle_times.append(float(parts[1]))

# 计算比值
cdc_exponential_ratios = [m / s for m, s in zip(cdc_exponential_map_times, cdc_exponential_shuffle_times)]
static_cdc_exponential_ratios = [m / s for m, s in zip(static_cdc_exponential_map_times, static_cdc_exponential_shuffle_times)]


# 绘制折线图
plt.figure(figsize=(10, 5))
# plt.plot(uniform_ratios, marker='o')
plt.plot(cdc_exponential_ratios, marker='x', label = "本课题方案")
plt.plot(static_cdc_exponential_ratios, marker='s', label = "CDC方案")
plt.legend(loc='best', prop=font)
plt.title('网络带宽呈指数分布时编码分布式计算方案和动态编码分布式计算方案的通信开销和计算开销的比值', fontproperties=font)
plt.xlabel('任务次数', fontproperties=font)
plt.ylabel('通信开销/计算开销', fontproperties=font)
plt.grid(True)

# 在显示图表前保存到本地文件
plt.savefig('static_dynamic_exponential_ratio.png', dpi=300, bbox_inches='tight')

import statistics
print(statistics.variance(cdc_exponential_ratios))
print(statistics.variance(static_cdc_exponential_ratios))