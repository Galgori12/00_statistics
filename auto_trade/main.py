import sys
from PyQt5.QtWidgets import QApplication
from auto_trade.gui import MainDashboard

def main():
    app = QApplication(sys.argv)
    win = MainDashboard()
    win.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()