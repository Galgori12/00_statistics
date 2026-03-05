# stkstats.analysis: 전략별 분석 모듈
#
# 실행 예 (프로젝트 루트에서):
#   python -m stkstats.analysis.entry_tp_sl.resolve_both
#   python -m stkstats.analysis.entry_tp_sl.grid_search_tp_sl
#   python -m stkstats.analysis.arm_delay.arm_delay_dd
#   python -m stkstats.analysis.first_dip.first_dip_0to3_open
#   python -m stkstats.analysis.gap_dip.gap_entry_grid
#   python -m stkstats.analysis.sl_bounce.sl_only_bounce
#   python -m stkstats.analysis.cooldown.cooldown_entry
#   python -m stkstats.analysis.data.attach_limit_close
#   python -m stkstats.analysis.data.build_daily_after_t1
#   python -m stkstats.analysis.project_status
#
# 하위 패키지:
#   entry_tp_sl  - 진입 97% / TP 107% / SL 96% 핵심 전략, 그리드 탐색
#   arm_delay    - 진입 후 ARM(일정 시간) drawdown / emergency SL
#   first_dip    - 첫 dip 시점·깊이 기반 진입 분석
#   gap_dip      - 갭×딥 그리드, 히트맵, EV
#   sl_bounce    - SL 구간·반등 분석
#   cooldown     - 쿨다운 진입
#   data         - 이벤트/일봉 보강, 파생 데이터 구축
