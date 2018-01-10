import os
import sys
import logging
import logging.config
import re


################################################################################
# BiTool模板 - 公用类
################################################################################


# 定义BiTool父类，所有自定义子类均继承自BiTool
class BiTool(object):
    '''BiTool父类，抽象工具箱通用属性和方法，包括日志、环境部署、结果处理等'''
    def __init__(self,work_path,data_path,job_id):
        self.work_path = work_path
        self.data_path = data_path
        self.job_id = job_id

        try:
            self.__env_init()
            self.__logging()
        except Exception as e:
            # print(e)
            pass

    # 环境初始化
    def __env_init(self):
        self.tmp_path = os.path.join(self.data_path,'tmp_%s' % self.job_id)
        self.result_path = os.path.join(self.data_path,'result_%s' % self.job_id)
        self.output_path = os.path.join(self.data_path,'output_%s' % self.job_id)

        for dir_path in [self.tmp_path,self.result_path,self.output_path]:
            if os.path.exists(dir_path):
                __import__('shutil').rmtree(dir_path)

            os.makedirs(dir_path)

    # 日志配置
    def __logging(self):
        # debug日志，在result文件夹下，只对开发者开放
        self.__log_file_path = os.path.join(self.result_path,'debug.log')

        # 输出日志，在output文件夹下，只存放包括debug日志路径等信息，对用户开放
        self.log_output_file = 'output.log'
        self.log_output_file_path = os.path.join(self.output_path,self.log_output_file)

        sh_str = "echo 'log file path: %s' >> %s" % (self.__log_file_path,self.log_output_file_path)
        os.system(sh_str)

        logging_config = LOGGING_CONFIG
        logging_config['handlers']['log']['filename'] = self.__log_file_path
        logging.config.dictConfig(logging_config)

        self.logger = logging.getLogger('default')

    # 通过日志包装输出函数
    def log(self,msg,level='debug'):
        if level == 'debug':
            self.logger.debug(msg)
        else:
            pass

    # 检查模板表依赖
    def check_dependency(self,table_list=None):
        check_failed_list = []

        if table_list == None:
            table_list = []

        for table_name in table_list:
            sql_str = """hive -e "use bitool;
                desc {};
            "
            """.format(table_name)
            output = os.popen(sql_str)

            if 'Table not found' in output:
                check_failed_list.append(table_name)

        if len(check_failed_list) == 0:
            return True
        else:
            return False

    # 工作流
    def pipeline(self):
        pass

    # 打包压缩输出文件夹
    def __zip_output_dir(self):
        zipped_file_path = os.path.join(self.data_path,'bitool_result_%s.tar.gz' % self.job_id)

        sh_str = """
            if [[ -f {file_path} ]];then
                rm {file_path}
            fi
            tar -czvf {file_path} -C {output_path} .
            """.format(file_path=zipped_file_path,output_path=self.output_path)

        os.system(sh_str)

    # 将指定文件打包压缩（必须在result_path文件夹下）
    def output(self,file_list=None):
        if file_list is None:
            file_list = []

        for file in os.listdir(self.result_path):
            if file in file_list:
                file_path = os.path.join(result_path,file)
                sh_str = """cp -f {} {}
                """.format(file_path,self.output_path)

                os.system(sh_str)

        self.__zip_output_dir()

    # 清理
    def clear(self):
        pass

    # 关闭
    def close(self):
        pass


# 广告线子类
class BiToolAd(BiTool):
    pass


################################################################################
# BiTool模板 - 公用配置 & 函数
################################################################################


# 定义遍历类（支持对不存在键值的索引）
class cust_dict(dict):
    def __getitem__(self,item):
        try:
            return dict.__getitem__(self,item)
        except KeyError:
            value = self[item] = type(self)()
            return value


# 定义获取表最近分区函数（按hour/day/month等数字编码分区）
def get_latest_table_partition(full_table_name,part_type='day'):
    sql_str = """hive -e "show partitions %s;"
    """ % full_table_name
    output = os.popen(sql_str)
    lines = [line.strip() for line  in output.read().split('\n') if len(line.strip())>0]

    pattern = re.compile(r'.*{}=(\d+)'.format(part_type))

    partitions = []

    for line in lines:
        try:
            partition = re.match(pattern,line).groups()[0]
            partitions.append(partition)
        except:
            continue

    partition_counts = len(set(partition))

    if partition_counts == 0:
        latest_partition = None
    else:
        latest_partition = max(partitions)

    return (partition_counts,latest_partition)


# 定义日志配置字典，默认为debug级别
LOGGING_CONFIG = cust_dict({
    'version':1, #日志级别
    'disable_existing_loggers':False, #是否禁用现有的记录器

    #日志格式集合
    'formatters':{
        'standard':{
            #[具体时间][线程名:线程ID][日志名字:日志级别名称(日志级别ID)] [输出的模块:输出的函数]:日志内容
            'format':'[%(asctime)s][%(threadName)s:%(thread)d][%(name)s:%(levelname)s(%(lineno)d)]\n[%(module)s:%(funcName)s]:%(message)s'
        }
    },

    #过滤器
    'filters':{
    },

    #处理器集合
    'handlers':{
        #输出到文件
        'log':{
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'formatter':'standard',
            'filename':'debug.log', #输出位置
            'maxBytes':1024*1024*5, #文件大小 5M
            'backupCount': 5, #备份份数
            'encoding': 'utf8', #文件编码
        },
    },

    #日志管理器集合
    'loggers':{
        #管理器
        'default':{
            'handlers':['log'],
            'level':'DEBUG',
            'propagate':True, #是否传递给父记录器
        },
    }
})
