Paper Author：基于微软学术API和Hadoop的统计论文作者频率应用（云计算概论课程项目）<br>
<br>
js源代码:getPaper.js<br>
java源代码：PaperAuthor.java（以所给Lab-2C-InvertedIndex的词频统计为基础）<br>
JAR：PaperAuthor.jar<br>
已抓取的输入数据：authors/<br>
<br>
步骤：<br>
1.使用nodejs getPaper.js 获取微软学术搜索API数据并按会议期刊年份存储到authors<br>
　ps：修改js文件中的CorJ,CJname,Ybegin,Yend来定义会议或期刊及年份，如果使用已抓取的数据则该步骤可跳过<br>
2.将authors/添加到hadoop的输入<br>
3.hadoop中调用PaperAuthor.jar 输入authors<br>
4.输入想要统计的会议或期刊简称、年份区间<br>
5.cat输出即可查看各个作者的发表频率统计<br>
<br>
其他内容详见设计文档及源代码
