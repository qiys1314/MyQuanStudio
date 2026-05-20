# ==============================================================================
# 模块名称: main.py
# 功能概述: 应用程序的启动入口。
#           负责初始化 PyQt5 图形应用运行环境，实例化主窗口对象，并启动主事件循环。
#           包含系统单例锁机制，防止程序被多开导致数据库锁定崩溃。
# ==============================================================================

import sys
import socket  # ====== 引入网络底层库，用于制作单例互斥锁 ======
from PyQt5.QtWidgets import QApplication, QMessageBox 
from ui.main_window import MainWindow

# ====== 全局锁对象引用 ======
# 必须将 socket 设为全局变量。如果写在函数里面，函数执行完 socket 就会被 Python 的垃圾回收机制(GC)清理，
# 端口就会被释放，单例锁就失效了。
global_instance_socket = None
# ===================================

def main():
    """
    主程序执行流控制函数。
    解释: 封装应用启动逻辑，避免变量污染全局命名空间。
    """
    global global_instance_socket
    
    # 1. 实例化 QApplication
    # 注意：所有 PyQt 的图形界面（包括后面的拦截弹窗）必须在 QApplication 实例化之后才能调用
    app = QApplication(sys.argv)
    
    # ====== 单例锁机制 (防多开保护网) ======
    # 建立一个基于 TCP 协议的本地 Socket 对象
    global_instance_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # 尝试绑定本地回环地址 (127.0.0.1) 和一个不常用的自定义高位端口 (例如: 38583)
        global_instance_socket.bind(('127.0.0.1', 38583))
    except socket.error:
        # 抛出异常说明绑定失败，该端口已被前一个打开的自身进程霸占
        QMessageBox.warning(
            None, 
            "启动拦截", 
            "⚠️ 检测到终端已在运行中！\n\n为了保护底层 SQLite 数据库不被损坏，禁止多开。\n请在任务栏或系统后台找回已打开的窗口。"
        )
        # 弹出警告后，直接阻断代码运行，安全销毁当前多余的“影子进程”
        sys.exit(0)
    # ===================================================
    
    # 2. 实例化主窗口界面
    window = MainWindow()
    
    # 3. 渲染窗口
    window.show()
    
    # 4. 启动主事件循环并接管退出状态
    #       当用户关闭窗口时，exec_() 会返回一个退出状态码，交由 sys.exit() 安全释放进程资源。
    sys.exit(app.exec_())


# 确保当且仅当该脚本被直接运行时，才执行 main() 函数；
# 若被其他模块作为包导入，则不会意外触发应用启动。
if __name__ == "__main__":
    main()