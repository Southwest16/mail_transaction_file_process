# email_transaction_file_parse
## 项目概况
- 登录邮箱
- 获取邮件附件
- 解压压缩包（zip/rar）
- 解析文件（excel/csv/html/pdf）

## 技术组件
- 技术组件：Python、MySQL。
- Python module：
    - poplib
    - pymysql
    - DBUtils
    - zipfile, rarfile
    - xlrd, csv, lxml, tabula

## 项目执行流程
1. 以固定的时间间隔登录邮箱，获取未解析的邮件列表；
2. 获取邮件附件中的压缩包（zip/rar）；
3. 解压附件（可能会出现压缩包嵌套压缩包的情况，需要递归地进行解压），并提取压缩包中的文件（excel/csv/html/pdf)；
4. 解析文件，并清洗结构化；
5. 将清洗之后的数据写入MySQL。
