import threading
import time


class MyThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.Flag = True  # 停止标志位
        self.Parm = 0  # 用来被外部访问的
        # 自行添加参数

    def run(self):
        while (True):
            if (not self.Flag):
                break
            else:
                time.sleep(2)

    def setFlag(self, parm):  # 外部停止线程的操作函数
        self.Flag = parm  # boolean

    def setParm(self, parm):  # 外部修改内部信息函数
        self.Parm = parm

    def getParm(self):  # 外部获得内部信息函数
        return self.Parm


if __name__ == "__main__":
    testThread = MyThread()
    testThread.setDaemon(True)  # 设为保护线程，主进程结束会关闭线程
    testThread.getParm()  # 获得线程内部值
    testThread.setParm(1)  # 修改线程内部值
    testThread.start()  # 开始线程
    print(testThread.getParm())  # 输出内部信息
    time.sleep(2)  # 主进程休眠 2 秒
    testThread.setFlag(False)  # 修改线程运行状态
    time.sleep(2)  # 2019.04.25 修改
    print(testThread.is_alive())  # 查看线程运行状态