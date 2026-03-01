from PyQt5.QtCore import Qt, QTimer, QPoint
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QListWidget, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QPlainTextEdit, QMessageBox,
    QGroupBox, QSpinBox, QDoubleSpinBox, QCheckBox, QShortcut, QComboBox
)
from datetime import datetime
from auto_trade.api.market import load_stock_master, MarketAPI
from auto_trade.api.auth import KiwoomAuth
from PyQt5.QtWidgets import QHeaderView
from PyQt5.QtGui import QColor, QBrush, QKeySequence



class MainDashboard(QMainWindow):
    """
    - 좌측: 관심종목 테이블(전일종가/현재가/등락률)
    - 우측 상단: 종목별 매매 설정 테이블(매수여부/수량/익절%/손절%/상태)
    - 우측 하단: 로그
    """

    def __init__(self):
        super().__init__()
        self.setWindowTitle("AI 주식 자동매매 - 수호님 통합 대시보드")
        self.resize(1700, 950)

        self._auto_rr_idx = 0

        # ✅ 종목 마스터: code -> {name, lastPrice, ...}
        self.stock_master = load_stock_master()
        # ✅ 검색용: code -> name
        self.stock_dict = {c: v["name"] for c, v in self.stock_master.items()}

        self._sync_guard = set()  # code 단위로 재진입 방지

        # 관심종목 리스트
        self.watchlist = []  # [(code, name)]

        # ✅ 최신 현재가 캐시 (code -> int)
        self.current_prices = {}

        # ✅ 종목별 매매 설정 저장(코드 -> dict)
        self.trade_settings = {}

        # ✅ 키움 연결 객체
        self.auth = None
        self.market_api = None

        # (선택) 자동 새로고침
        self.auto_refresh_timer = QTimer(self)
        self.auto_refresh_timer.timeout.connect(self.on_refresh_clicked)
        self.auto_refresh_interval_ms = 3000  # 5초

        self._build_ui()
        self._bind_events()
        self._start_clock()
        self.search_input.installEventFilter(self)

        # ✅ 검색 팝업 선택 이동(방향키) - eventFilter 없이 안정적으로
        self.sc_down = QShortcut(QKeySequence(Qt.Key_Down), self.search_input)
        self.sc_up = QShortcut(QKeySequence(Qt.Key_Up), self.search_input)

        self.sc_down.activated.connect(self._popup_move_down)
        self.sc_up.activated.connect(self._popup_move_up)

        self.log(f"📚 종목 마스터 로딩 완료: {len(self.stock_master):,}개")
        self.log("✅ GUI 준비 완료 (PyQt5)")

    def on_auto_refresh_tick(self):
        if self.market_api is None:
            return
        n = self.table.rowCount()
        if n == 0:
            return

        r = self._auto_rr_idx % n
        self._auto_rr_idx += 1

        code = self.table.item(r, 0).text().strip()
        try:
            # 기존 on_refresh_clicked 안의 "한 종목 갱신 로직"을 여기로 옮기거나
            # 아래처럼 그냥 on_refresh_clicked를 한 종목만 갱신하게 분리해도 됨
            data = self.market_api.get_stock_info(code)
            cur = abs(int(data.get("cur_prc", "0")))
            if cur > 0:
                self.current_prices[code] = cur

            # ✅ 수량 자동계산/상태 갱신
            self._recalc_settings_qty_for_code(code)

        except Exception as e:
            self.log(f"❌ {code} 시세 실패: {e}")

    def _popup_move_down(self):
        if self.popup.isVisible() and self.popup.count() > 0:
            cur = self.popup.currentRow()
            if cur < 0:
                cur = 0
            self.popup.setCurrentRow(min(cur + 1, self.popup.count() - 1))

    def _popup_move_up(self):
        if self.popup.isVisible() and self.popup.count() > 0:
            cur = self.popup.currentRow()
            if cur < 0:
                cur = 0
            self.popup.setCurrentRow(max(cur - 1, 0))

    # ---------------- 정렬 ---------------

    def _mk_item(self, text: str, align: Qt.AlignmentFlag = None) -> QTableWidgetItem:
        it = QTableWidgetItem(text)
        if align is not None:
            it.setTextAlignment(int(align))
        return it

    def _center_widget(self, w: QWidget) -> QWidget:
        """셀 위젯(체크박스/버튼 등)을 가운데로 정렬하기 위한 컨테이너"""
        box = QWidget()
        lay = QHBoxLayout(box)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setAlignment(Qt.AlignCenter)
        lay.addWidget(w)
        return box

    # ---------------- UI ----------------
    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)

        # 상단: 검색 + 버튼
        top = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("종목명 또는 종목코드 입력 (예: 삼성전자 / 005930)")

        self.popup = QListWidget(self)
        self.popup.setWindowFlags(Qt.ToolTip)
        self.popup.setFocusPolicy(Qt.NoFocus)
        self.popup.hide()

        self.btn_connect = QPushButton("연결")
        self.btn_start = QPushButton("자동매매 시작(더미)")
        self.btn_stop = QPushButton("중지(더미)")
        self.btn_refresh = QPushButton("시세 새로고침")

        top.addWidget(QLabel("종목 검색:"))
        top.addWidget(self.search_input, 1)
        top.addWidget(self.btn_connect)
        top.addWidget(self.btn_start)
        top.addWidget(self.btn_stop)
        top.addWidget(self.btn_refresh)
        main.addLayout(top)

        # 가운데: 좌(관심종목 테이블) + 우(설정/로그)
        mid = QHBoxLayout()

        # ---- 좌측: 관심종목 ----
        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(
            ["코드", "종목명", "전일종가", "현재가", "등락률", "매수가", "수량", "수익률", "상태", "삭제"]
        )
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSortingEnabled(False)

        # 컬럼 폭: 종목명은 넓게, 나머지는 내용에 맞게
        h = self.table.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeToContents)  # 코드
        h.setSectionResizeMode(1, QHeaderView.Stretch)  # 종목명
        h.setSectionResizeMode(2, QHeaderView.Stretch)  # 전일종가
        h.setSectionResizeMode(3, QHeaderView.Stretch)  # 현재가
        h.setSectionResizeMode(4, QHeaderView.Stretch)  # 등락률
        h.setSectionResizeMode(5, QHeaderView.Stretch)  # 매수가
        h.setSectionResizeMode(6, QHeaderView.Stretch)  # 매수수량
        h.setSectionResizeMode(7, QHeaderView.Stretch)  # 수익률
        h.setSectionResizeMode(8, QHeaderView.ResizeToContents)  # 상태
        h.setSectionResizeMode(9, QHeaderView.ResizeToContents)  # 삭제

        # ---- 우측: 설정 + 로그 ----
        right = QVBoxLayout()
        right.setContentsMargins(10, 10, 10, 10)
        right.setSpacing(10)

        # (1) 계좌정보 패널 (표시 전용)
        account_box = QGroupBox("계좌정보")
        account_layout = QVBoxLayout(account_box)
        account_layout.setContentsMargins(15, 15, 15, 15)
        account_layout.setSpacing(8)

        # ✅ 상단: 계좌 선택 콤보
        top_row = QHBoxLayout()
        top_row.setSpacing(8)
        top_row.addWidget(QLabel("계좌:"))

        self.cmb_account = QComboBox()
        try:
            self.cmb_account.currentIndexChanged.disconnect()
        except Exception:
            pass
        self.cmb_account.currentIndexChanged.connect(self._on_account_changed)
        self.cmb_account.setMinimumWidth(200)
        top_row.addWidget(self.cmb_account)
        top_row.addStretch(1)

        account_layout.addLayout(top_row)

        # ✅ 요약 테이블 (4개 항목만)
        self.acc_table = QTableWidget(0, 2)
        self.acc_table.setMaximumHeight(140)
        self.acc_table.setHorizontalHeaderLabels(["항목", "값"])
        self.acc_table.verticalHeader().setVisible(False)

        h_acc = self.acc_table.horizontalHeader()
        h_acc.setVisible(False)
        h_acc.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        h_acc.setSectionResizeMode(1, QHeaderView.Stretch)

        self.acc_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.acc_table.setSelectionMode(QTableWidget.NoSelection)
        self.acc_table.setFocusPolicy(Qt.NoFocus)
        self.acc_table.setAlternatingRowColors(True)
        self.acc_table.verticalHeader().setDefaultSectionSize(28)

        account_layout.addWidget(self.acc_table)
        right.addWidget(account_box)  # ✅ 딱 1번만!

        # 초기 표시(연결 전) - ✅ 4개만
        self._set_account_info({
            "예수금": "-",
            "총 평가금액": "-",
            "총 손익": "-",
            "총 수익률": "-",  # 키 이름 통일 추천 (기존 손익률이면 함수에서 매핑)
        })

        # (2) 매매 설정 패널
        settings_box = QGroupBox("매매 설정")
        settings_layout = QVBoxLayout(settings_box)

        # ✅ 기본값(표에서만 사용)
        self.default_qty_value = 1
        self.default_amt_value = 1_000_000
        self.default_tp_value = 10.0
        self.default_sl_value = 5.0

        # 종목별 설정 테이블
        self.settings_table = QTableWidget(0, 9)
        self.settings_table.setHorizontalHeaderLabels(["코드", "종목명", "매수", "매수금액(원)", "수량", "익절%", "손절%", "매매상태", "적용"])
        self.settings_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.settings_table.setSortingEnabled(False)
        settings_layout.addWidget(self.settings_table)

        # 컬럼 자동 크기 조정
        header = self.settings_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)  # 코드
        header.setSectionResizeMode(1, QHeaderView.Stretch)  # 종목명 (늘어나게)
        header.setSectionResizeMode(2, QHeaderView.Stretch)  # 매수
        header.setSectionResizeMode(3, QHeaderView.Stretch)  # 매수금액(원)
        header.setSectionResizeMode(4, QHeaderView.Stretch)  # 수량
        header.setSectionResizeMode(5, QHeaderView.Stretch)  # 익절%
        header.setSectionResizeMode(6, QHeaderView.Stretch)  # 손절%
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)  # 매매상태 (늘어나게)
        header.setSectionResizeMode(8, QHeaderView.ResizeToContents)  # 적용 (고정)

        # 손절/익절 입력칸이 너무 좁아지지 않게 최소 폭 확보
        self.settings_table.setColumnWidth(1, 100)
        self.table.setColumnWidth(2, 110)
        self.table.setColumnWidth(3, 110)
        self.table.setColumnWidth(4, 80)
        self.table.setColumnWidth(5, 110)
        self.table.setColumnWidth(6, 80)
        self.settings_table.setColumnWidth(7, 120)
        self.settings_table.setColumnWidth(8, 70)  # 적용 버튼 폭 고정(중요)
        self.settings_table.setMinimumHeight(300)  # 테이블 너무 작아지는 거 방지(선택)

        # 행 높이 키우기 (가독성 ↑)
        self.settings_table.verticalHeader().setDefaultSectionSize(32)

        right.addWidget(settings_box,1)

        # (3) 로그
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("로그가 여기에 표시됩니다.")
        self.log_view.setMinimumHeight(220)  # 필요시 180~300 조절

        right.addWidget(QLabel("로그"), 0)
        right.addWidget(self.log_view, 2)  # stretch=2로 더 크게

        mid.addWidget(self.table, 4)
        mid.addLayout(right, 3)

        main.addLayout(mid, 1)

        # 상태바
        self.status_label = QLabel("상태: 대기")
        self.clock_label = QLabel("시간: --:--:--")
        self.statusBar().addWidget(self.status_label, 1)
        self.statusBar().addPermanentWidget(self.clock_label)

    def _bind_events(self):
        self.search_input.textChanged.connect(self._on_search_changed)
        self.search_input.returnPressed.connect(self._on_search_enter)
        self.popup.itemClicked.connect(self._on_popup_click)

        self.btn_connect.clicked.connect(self.on_connect_clicked)
        self.btn_start.clicked.connect(self.on_start_clicked)
        self.btn_stop.clicked.connect(self.on_stop_clicked)
        self.btn_refresh.clicked.connect(self.on_refresh_clicked)

    def eventFilter(self, obj, event):
        return super().eventFilter(obj, event)


    # ---------------- Util ----------------
    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_view.appendPlainText(f"[{ts}] {msg}")

    def set_status(self, msg: str):
        self.status_label.setText(f"상태: {msg}")

    def _set_account_info(self, info: dict):
        """계좌정보 테이블 업데이트 (간소화 + 색상 정리)"""
        if not hasattr(self, "acc_table") or self.acc_table is None:
            return

        # 우리가 보여줄 항목만 고정
        order = [
            "예수금",
            "총 평가금액",
            "총 손익",
            "총 수익률",  # 기존 "손익률" -> 통일 추천
        ]

        self.acc_table.setRowCount(len(order))

        def _to_number(s):
            try:
                t = str(s).replace(",", "").replace("%", "").strip()
                if t == "" or t == "-":
                    return None
                return float(t)
            except:
                return None

        def _format_value(key, value):
            """숫자 보기 좋게 포맷"""
            num = _to_number(value)
            if num is None:
                return str(value)

            if key == "총 수익률":
                return f"{num:+.2f}%"
            elif key in ("총 손익",):
                return f"{num:+,.0f}"
            elif key in ("예수금", "총 평가금액"):
                return f"{num:,.0f}"
            return str(value)

        for r, key in enumerate(order):
            raw_value = info.get(key, "-")
            display_value = _format_value(key, raw_value)

            # 항목명
            it_k = QTableWidgetItem(key)
            it_k.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
            font = it_k.font()
            font.setBold(True)
            it_k.setFont(font)

            # 값
            it_v = QTableWidgetItem(display_value)
            it_v.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)

            # 손익 / 수익률 색상 처리
            if key in ("총 손익", "총 수익률"):
                num = _to_number(raw_value)
                if num is not None:
                    if num > 0:
                        it_v.setForeground(QBrush(QColor(220, 60, 60)))  # 빨강
                    elif num < 0:
                        it_v.setForeground(QBrush(QColor(60, 110, 220)))  # 파랑

            self.acc_table.setItem(r, 0, it_k)
            self.acc_table.setItem(r, 1, it_v)

        # 보기 안정화
        self.acc_table.resizeRowsToContents()

    def _start_clock(self):
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._tick)
        self.timer.start(1000)
        self._tick()

    def _tick(self):
        self.clock_label.setText("시간: " + datetime.now().strftime("%H:%M:%S"))

    # ---------------- Search (Popup) ----------------
    def _on_search_changed(self, text: str):
        text = text.strip()
        if not text:
            self.popup.hide()
            return

        text_lower = text.lower()

        if text.isdigit():
            matches = [c for c in self.stock_dict.keys() if c.startswith(text)]
            items = [f"{c} {self.stock_dict[c]}" for c in matches[:30]]
        else:
            items = [
                f"{c} {n}"
                for c, n in self.stock_dict.items()
                    if text_lower in n.lower()
            ][:30]

        if not items:
            self.popup.hide()
            return

        self.popup.clear()
        self.popup.addItems(items)
        self.popup.setCurrentRow(0)

        p = self.search_input.mapToGlobal(QPoint(0, self.search_input.height()))

        self.popup.resize(self.search_input.width(), 260)
        self.popup.move(p)
        self.popup.show()
        self.popup.raise_()
        self.popup.move(p)
        self.search_input.setFocus()

    def _on_popup_click(self, item):
        self.popup.hide()
        code, name = item.text().split(" ", 1)
        self._add_to_watchlist(code, name)
        self.search_input.clear()

    def _on_search_enter(self):
        # 팝업이 떠 있고 후보가 있으면: 선택된 항목(없으면 0번째)을 바로 추가
        if self.popup.isVisible() and self.popup.count() > 0:
            item = self.popup.currentItem() or self.popup.item(0)
            if item is not None:
                self._on_popup_click(item)
            return

        text = self.search_input.text().strip()
        if not text:
            return

        text_lower = text.lower()

        # 코드 정확 입력
        if text.isdigit() and text in self.stock_dict:
            self._add_to_watchlist(text, self.stock_dict[text])
        else:
            found = None

            # 종목명 정확일치(대소문자 무시)
            for c, n in self.stock_dict.items():
                if text_lower == n.lower():
                    found = (c, n)
                    break

            # 없으면 부분 포함 첫 매칭
            if not found:
                for c, n in self.stock_dict.items():
                    if text_lower in n.lower():
                        found = (c, n)
                        break

            if found:
                self._add_to_watchlist(*found)
            else:
                QMessageBox.information(self, "검색", "해당 종목을 찾지 못했습니다.")

        self.search_input.clear()
        self.popup.hide()

    # ---------------- Watchlist / Tables ----------------
    def _add_to_watchlist(self, code: str, name: str):
        if any(code == c for c, _ in self.watchlist):
            self.log(f"⚠️ 이미 관심종목에 있음: {code} {name}")
            return

        self.watchlist.append((code, name))

        row = self.table.rowCount()
        self.table.insertRow(row)

        info = self.stock_master.get(code, {})
        prev = int(info.get("lastPrice", 0) or 0)

        self.table.setItem(row, 0, self._mk_item(code, Qt.AlignVCenter | Qt.AlignLeft))
        self.table.setItem(row, 1, self._mk_item(name, Qt.AlignVCenter | Qt.AlignCenter))
        self.table.setItem(row, 2, self._mk_item(f"{prev:,}" if prev else "-", Qt.AlignVCenter | Qt.AlignRight))
        self.table.setItem(row, 3, self._mk_item("-", Qt.AlignVCenter | Qt.AlignRight))
        self.table.setItem(row, 4, self._mk_item("-", Qt.AlignVCenter | Qt.AlignRight))
        self.table.setItem(row, 5, self._mk_item("-", Qt.AlignVCenter | Qt.AlignRight))  # 매수가
        self.table.setItem(row, 6, self._mk_item("-", Qt.AlignVCenter | Qt.AlignRight))  # 매수수량(NEW)
        self.table.setItem(row, 7, self._mk_item("-", Qt.AlignVCenter | Qt.AlignRight))  # 수익률
        self.table.setItem(row, 8, self._mk_item("대기", Qt.AlignVCenter | Qt.AlignCenter))  # 상태

        # 삭제 버튼(행 단위)
        btn_delete = QPushButton("삭제")
        btn_delete.clicked.connect(lambda _=False, c=code: self.remove_watch_row(c))
        self.table.setCellWidget(row, 9, btn_delete)

        # 우측 설정 테이블도 추가
        self._add_settings_row(code)

        self.log(f"⭐ 관심종목 추가: {code} {name}")

    def _add_settings_row(self, code: str):
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text() == code:
                return

        r = self.settings_table.rowCount()
        self.settings_table.insertRow(r)

        # 코드
        self.settings_table.setItem(r, 0, QTableWidgetItem(code))

        # 종목명
        name = self.stock_dict.get(code, "")
        name_item = QTableWidgetItem(name)
        name_item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignCenter))
        self.settings_table.setItem(r, 1, name_item)

        # 매수 체크
        chk = QCheckBox()
        chk.setChecked(True)
        self.settings_table.setCellWidget(r, 2, self._center_widget(chk))

        # 매수금액(3) NEW
        amt = QSpinBox()
        amt.setRange(0, 1_000_000_000)
        amt.setSingleStep(100_000)
        amt.setValue(self.default_amt_value)
        self.settings_table.setCellWidget(r, 3, amt)

        # 수량
        qty = QSpinBox()
        qty.setRange(1, 999999)
        qty.setValue(self.default_qty_value)
        self.settings_table.setCellWidget(r, 4, qty)

        # ✅ 매수금액 변경 -> 현재가 기준 수량 자동 계산
        # (주의) lambda에서 row가 바뀌는 문제를 막기 위해 기본 인자로 고정
        # 금액 변경 -> 수량 계산
        amt.valueChanged.connect(lambda _v, c=code: self._update_qty_by_amount_by_code(c))
        # 수량 변경 -> 금액 계산
        qty.valueChanged.connect(lambda _v, c=code: self._update_amount_by_qty_by_code(c))

        # 초기 1회 계산 시도 (현재가가 아직 없으면 스킵됨)
        self._update_qty_by_amount(r)

        # 익절
        tp = QDoubleSpinBox()
        tp.setRange(0.0, 10.0)
        tp.setDecimals(2)
        tp.setValue(self.default_tp_value)
        tp.setSuffix("%")
        self.settings_table.setCellWidget(r, 5, tp)

        # 손절
        sl = QDoubleSpinBox()
        sl.setRange(0.0, 10.0)
        sl.setDecimals(2)
        sl.setValue(self.default_sl_value)
        sl.setSuffix("%")
        self.settings_table.setCellWidget(r, 6, sl)

        # 상태
        st = QTableWidgetItem("대기")
        st.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignCenter))
        self.settings_table.setItem(r, 7, st)


        # 적용 버튼 (해당 행 설정을 저장/반영)
        btn_apply = QPushButton("적용")
        btn_apply.clicked.connect(lambda _=False, c=code: self.apply_settings_for_code(c))
        self.settings_table.setCellWidget(r, 8, btn_apply)

    def _update_qty_by_amount_by_code(self, code: str):
        """종목코드로 settings_table row를 찾아 수량/상태 갱신 (row 꼬임 방지)"""
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text().strip() == code:
                self._update_qty_by_amount(r)
                return

    def _update_amount_by_qty_by_code(self, code: str):
        """종목코드로 row 찾아 수량→금액 동기화"""
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text().strip() == code:
                self._update_amount_by_qty(r)
                return

    def _remove_settings_row(self, code: str):
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text() == code:
                self.settings_table.removeRow(r)
                return

    def _selected(self):
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            return None
        r = rows[0].row()
        code = self.table.item(r, 0).text()
        name = self.table.item(r, 1).text()
        return r, code, name

    def _selected_settings_rows(self):
        return self.settings_table.selectionModel().selectedRows()

    # ---------------- Buttons ----------------
    def on_connect_clicked(self):
        try:
            self.auth = KiwoomAuth()
            self.auth.login()
            if not self.auth.access_token:
                raise RuntimeError("토큰 발급 실패")

            self.market_api = MarketAPI(self.auth)
            self._load_account_info()
            self.set_status("연결됨")
            self.log("✅ 키움 연결 완료")

            # 자동 새로고침 시작(원치 않으면 이 두 줄 지우면 됨)
            self.auto_refresh_timer.start(self.auto_refresh_interval_ms)
            self.log(f"🔁 자동 새로고침 시작: {self.auto_refresh_interval_ms/1000:.0f}초마다")

        except Exception as e:
            self.set_status("연결 실패")
            self.log(f"❌ 연결 실패: {e}")

    def _load_account_info(self):
        if self.auth is None:
            return

        try:
            from auto_trade.api.account import AccountAPI
            acc_api = AccountAPI(self.auth)

            # 1) 계좌번호 조회 (표시용/선택용)
            accounts, raw = acc_api.get_account_numbers()

            if accounts:
                self.set_accounts(accounts)  # ✅ 콤보에 계좌 2개 세팅
                self.selected_account_no = accounts[0]
            else:
                self.set_accounts(["-"])
                self.selected_account_no = "-"

            # 2) 계좌평가현황(kt00004)로 예수금/평가/손익 채우기
            ev = acc_api.get_account_evaluation(qry_tp="1", dmst_stex_tp="KRX")

            # 응답이 래핑될 수 있어 방어적으로 “본문 dict” 찾기
            payload = ev
            for k in ("data", "output", "result", "res"):
                if isinstance(ev, dict) and isinstance(ev.get(k), dict):
                    payload = ev[k]
                    break

            def to_int(x):
                try:
                    if x is None: return 0
                    s = str(x).replace(",", "").strip()
                    if s == "": return 0
                    return int(float(s))
                except:
                    return 0

            def to_float(x):
                try:
                    if x is None: return 0.0
                    s = str(x).replace(",", "").strip().replace("%", "")
                    if s == "": return 0.0
                    return float(s)
                except:
                    return 0.0

            entr = to_int(payload.get("entr"))  # 예수금
            d2 = to_int(payload.get("d2_entra"))  # D+2추정예수금
            eval_amt = to_int(payload.get("tot_est_amt"))  # 유가잔고평가액(총 평가금액으로 사용)
            pnl = to_int(payload.get("lspft"))  # 누적투자손익(총 손익으로 사용)
            pnl_rt = to_float(payload.get("lspft_rt"))  # 누적손익율(총 손익률로 사용)

            info = {
                "계좌번호": self.selected_account_no,
                "계좌명": payload.get("acnt_nm", "-") if isinstance(payload, dict) else "-",
                "예수금": f"{entr:,}" if entr else "0",
                "D+2 예수금": f"{d2:,}" if d2 else "0",
                "총 평가금액": f"{eval_amt:,}" if eval_amt else "0",
                "총 손익": f"{pnl:,}" if pnl else "0",
                "손익률": f"{pnl_rt:.2f}%" if pnl_rt else "0.00%",
            }

            self._set_account_info(info)
            self.log(str(info))
            self.log(f"🏦 계좌정보 로드 완료: {self.selected_account_no}")

        except Exception as e:
            self.log(f"❌ 계좌정보 로드 실패: {e}")
            self._set_account_info({
                "계좌번호": "-",
                "계좌명": "-",
                "예수금": "-",
                "D+2 예수금": "-",
                "총 평가금액": "-",
                "총 손익": "-",
                "손익률": "-",
            })

    def on_start_clicked(self):
        self.set_status("자동매매 실행(더미)")
        self.log("🚀 자동매매 시작 (현재는 더미)")

    def on_stop_clicked(self):
        self.set_status("중지(더미)")
        self.log("🛑 중지 (현재는 더미)")

        if self.auto_refresh_timer.isActive():
            self.auto_refresh_timer.stop()
            self.log("⏹️ 자동 새로고침 중지")

    def on_refresh_clicked(self):
        # 타이머로 돌리기 때문에 연결 없으면 조용히 리턴
        if self.market_api is None:
            return
        if self.table.rowCount() == 0:
            return

        for r in range(self.table.rowCount()):
            code = self.table.item(r, 0).text()

            try:
                data = self.market_api.get_stock_info(code)  # ka10001
                cur = abs(int(data.get("cur_prc", "0")))      # 현재가

                # ✅ 현재가 캐시
                if cur > 0:
                    self.current_prices[code] = cur

                info = self.stock_master.get(code, {})
                prev = int(info.get("lastPrice", 0) or 0)

                if prev > 0:
                    chg = cur - prev
                    pct = (chg / prev) * 100.0
                    pct_str = f"{pct:+.2f}%"
                else:
                    pct_str = "-"

                cur_item = QTableWidgetItem(f"{cur:,}")
                cur_item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignRight))

                pct_item = QTableWidgetItem(pct_str)
                pct_item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignRight))

                # ✅ 전일종가 대비 상승/하락 색상 적용 (현재가 + 등락률)
                if prev > 0:
                    if cur > prev:
                        brush = QBrush(QColor("red"))
                        cur_item.setForeground(brush)
                        pct_item.setForeground(brush)
                    elif cur < prev:
                        brush = QBrush(QColor("blue"))
                        cur_item.setForeground(brush)
                        pct_item.setForeground(brush)

                self.table.setItem(r, 3, cur_item)
                self.table.setItem(r, 4, pct_item)
                status_it = QTableWidgetItem("갱신완료")
                status_it.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                self.table.setItem(r, 8, status_it)

                # ✅ 설정 테이블의 수량도 현재가 갱신 시 재계산(매수금액 기준)
                self._recalc_settings_qty_for_code(code)

            except Exception as e:
                status_it = QTableWidgetItem("갱신실패")
                status_it.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                self.table.setItem(r, 8, status_it)
                self.log(f"❌ {code} 시세 실패: {e}")

    def on_buy_clicked(self):
        sel = self._selected()
        if not sel:
            QMessageBox.information(self, "매수", "좌측 테이블에서 종목을 선택하세요.")
            return
        r, code, name = sel
        self.log(f"🟢 매수(테스트): {code} {name}")
        self.table.setItem(r, 7, QTableWidgetItem("매수요청(테스트)"))

        # 설정 테이블 상태도 변경
        self._set_settings_status(code, "매수요청(테스트)")

    def on_sell_clicked(self):
        sel = self._selected()
        if not sel:
            QMessageBox.information(self, "매도", "좌측 테이블에서 종목을 선택하세요.")
            return
        r, code, name = sel
        self.log(f"🔴 매도(테스트): {code} {name}")
        self.table.setItem(r, 5, QTableWidgetItem("매도요청(테스트)"))

        self._set_settings_status(code, "매도요청(테스트)")

    def on_remove_clicked(self):
        sel = self._selected()
        if not sel:
            QMessageBox.information(self, "제거", "좌측 테이블에서 종목을 선택하세요.")
            return
        r, code, name = sel

        self.table.removeRow(r)
        self.watchlist = [(c, n) for (c, n) in self.watchlist if c != code]
        self._remove_settings_row(code)

        self.log(f"🗑️ 관심종목 제거: {code} {name}")

    def remove_watch_row(self, code: str):
        for r in range(self.table.rowCount()):
            it = self.table.item(r, 0)
            if it and it.text().strip() == code:
                self.table.removeRow(r)
                break

        self.watchlist = [(c, n) for (c, n) in self.watchlist if c != code]
        self._remove_settings_row(code)

        self.log(f"🗑️ 관심종목 제거: {code}")

    def apply_settings_for_code(self, code: str):
        """우측 매매설정 테이블의 '적용' 버튼: 해당 종목 설정을 저장하고 좌측 상태에 반영."""
        row = None
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text().strip() == code:
                row = r
                break
        if row is None:
            return

        buy_chk = self.settings_table.cellWidget(row, 2)
        amt_w = self.settings_table.cellWidget(row, 3)
        qty_w = self.settings_table.cellWidget(row, 4)
        tp_w = self.settings_table.cellWidget(row, 5)
        sl_w = self.settings_table.cellWidget(row, 6)

        settings = {
            "buy_enabled": bool(buy_chk.isChecked()) if buy_chk else False,
            "buy_amount": int(amt_w.value()) if amt_w else 0,
            "qty": int(qty_w.value()) if qty_w else 0,
            "take_profit_pct": float(tp_w.value()) if tp_w else 0.0,
            "stop_loss_pct": float(sl_w.value()) if sl_w else 0.0,
            "updated_at": datetime.now(),
        }
        self.trade_settings[code] = settings

    # 좌측 테이블 상태도 표시
        for tr in range(self.table.rowCount()):
            it = self.table.item(tr, 0)
            if it and it.text().strip() == code:
                self.table.setItem(tr, 7, QTableWidgetItem("설정적용"))
                break

        self._set_settings_status(code, "설정적용")
        self.log(
            f"✅ 설정 적용: {code} | 매수={'ON' if settings['buy_enabled'] else 'OFF'}"
            f", 금액={settings['buy_amount']:,}원, 수량={settings['qty']}, 익절={settings['take_profit_pct']:.2f}%, 손절={settings['stop_loss_pct']:.2f}%"
            )


# ---------------- Settings helpers ----------------
    def _set_settings_status(self, code: str, status: str):
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text().strip() == code:
                item = QTableWidgetItem(status)
                item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignCenter))
                self.settings_table.setItem(r, 7, item)
                return

    def _get_code_by_settings_row(self, row: int) -> str:
        it = self.settings_table.item(row, 0)
        return it.text() if it else ""

    def _update_amount_by_qty(self, row: int):
        """수량 변경 시 현재가 기준으로 금액 자동 계산"""
        code = self._get_code_by_settings_row(row)
        if not code:
            return

        # ✅ 재진입 방지
        if code in self._sync_guard:
            return
        self._sync_guard.add(code)
        try:
            cur = self.current_prices.get(code, 0)
            if cur <= 0:
                cur = self._ensure_current_price(code)
                if cur <= 0:
                    self._set_settings_status(code, "현재가 없음")
                    return

            amt_widget = self.settings_table.cellWidget(row, 3)
            qty_widget = self.settings_table.cellWidget(row, 4)
            if amt_widget is None or qty_widget is None:
                return

            qty = int(qty_widget.value())
            new_amount = int(qty * cur)

            # ✅ amount setValue는 신호 차단
            amt_widget.blockSignals(True)
            amt_widget.setValue(new_amount)
            amt_widget.blockSignals(False)

            cur_item = self.settings_table.item(row, 7)
            cur_status = cur_item.text().strip() if cur_item else ""
            if cur_status in ("금액 부족", "현재가 없음"):
                self._set_settings_status(code, "대기")

        finally:
            self._sync_guard.discard(code)

    def _update_qty_by_amount(self, row: int):
        """매수금액 변경 시 현재가 기준으로 수량 자동 계산(예산 근접, 소액 초과 허용)"""
        code = self._get_code_by_settings_row(row)
        if not code:
            return

        # ✅ 재진입 방지
        if code in self._sync_guard:
            return
        self._sync_guard.add(code)
        try:
            cur = self.current_prices.get(code, 0)
            if cur <= 0:
                cur = self._ensure_current_price(code)
                if cur <= 0:
                    self._set_settings_status(code, "현재가 없음")
                    return

            amt_widget = self.settings_table.cellWidget(row, 3)
            qty_widget = self.settings_table.cellWidget(row, 4)
            if amt_widget is None or qty_widget is None:
                return

            amount = int(amt_widget.value())

            # ✅ 예산 근접 로직(올림 허용)
            MAX_OVER_KRW = 50_000
            MAX_OVER_PCT = 0.02

            q_floor = amount // cur
            if q_floor <= 0:
                qty = 0
            else:
                q_ceil = q_floor + 1
                cost_floor = q_floor * cur
                cost_ceil = q_ceil * cur
                under = amount - cost_floor
                over = cost_ceil - amount
                allow_over = (over <= MAX_OVER_KRW) and (over <= amount * MAX_OVER_PCT)
                qty = q_ceil if (allow_over and over < under) else q_floor

            if qty <= 0:
                qty = 1  # spin 최소 1
                self._set_settings_status(code, "금액 부족")
            else:
                cur_item = self.settings_table.item(row, 7)
                if cur_item and cur_item.text().strip() == "금액 부족":
                    self._set_settings_status(code, "대기")

            # ✅ qty setValue는 신호 차단
            qty_widget.blockSignals(True)
            qty_widget.setValue(int(qty))
            qty_widget.blockSignals(False)

        finally:
            self._sync_guard.discard(code)

    def _ensure_current_price(self, code: str) -> int:
        """현재가 캐시에 없으면 1회 조회해서 캐시에 넣고 반환"""
        cur = self.current_prices.get(code, 0)
        if cur > 0:
            return cur

        if self.market_api is None:
            return 0

        try:
            data = self.market_api.get_stock_info(code)  # ka10001
            cur = abs(int(data.get("cur_prc", "0")))
            if cur > 0:
                self.current_prices[code] = cur
            return cur
        except Exception as e:
            self.log(f"⚠️ {code} 현재가 단건 조회 실패: {e}")
            return 0

    def _recalc_settings_qty_for_code(self, code: str):
        """현재가 갱신 시 해당 코드 행의 수량을 매수금액 기준으로 재계산."""
        for r in range(self.settings_table.rowCount()):
            it = self.settings_table.item(r, 0)
            if it and it.text() == code:
                self._update_qty_by_amount(r)
                return
# ----------------- Account 관련 -----------------------
    def _mask_account(self, acc_no: str) -> str:
        s = str(acc_no)
        if len(s) <= 4:
            return s
        return s[:4] + "****" + s[-2:]

    def set_accounts(self, account_numbers):
        self.cmb_account.blockSignals(True)
        self.cmb_account.clear()
        for acc in account_numbers:
            self.cmb_account.addItem(self._mask_account(acc), acc)
        self.cmb_account.blockSignals(False)

    def _on_account_changed(self, idx):
        acc_no = self.cmb_account.itemData(idx)
        if not acc_no or acc_no == "-":
            return

        self.selected_account_no = acc_no
        self.log(f"🔄 계좌 변경: {acc_no}")
        self._reload_selected_account()

    def _reload_selected_account(self):
        if self.auth is None:
            return

        try:
            from auto_trade.api.account import AccountAPI
            acc_api = AccountAPI(self.auth)

            ev = acc_api.get_account_evaluation(qry_tp="1", dmst_stex_tp="KRX")

            payload = ev
            for k in ("data", "output", "result", "res"):
                if isinstance(ev, dict) and isinstance(ev.get(k), dict):
                    payload = ev[k]
                    break

            def to_int(x):
                try:
                    return int(float(str(x).replace(",", "").strip()))
                except:
                    return 0

            def to_float(x):
                try:
                    return float(str(x).replace(",", "").replace("%", "").strip())
                except:
                    return 0.0

            entr = to_int(payload.get("entr"))
            eval_amt = to_int(payload.get("tot_est_amt"))
            pnl = to_int(payload.get("lspft"))
            pnl_rt = to_float(payload.get("lspft_rt"))

            self._set_account_info({
                "예수금": entr,
                "총 평가금액": eval_amt,
                "총 손익": pnl,
                "총 수익률": pnl_rt,
            })

        except Exception as e:
            self.log(f"❌ 계좌 변경 로드 실패: {e}")