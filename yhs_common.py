"""
yhs 공통함수들
"""
import os.path
from datetime import datetime, timedelta
import inspect
from dotenv import dotenv_values

YES = True
NO = False

env = dotenv_values(".env")
YHS_LOGGER_ENABLE = env['YHS_LOGGER_ENABLE']


def get_nowtimestr() -> str:
    now_utc = datetime.utcnow()
    now_kor = now_utc + timedelta(hours=9)
    return now_kor.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def get_todaystr() -> str:
    now_utc = datetime.utcnow()
    now_kor = now_utc + timedelta(hours=9)
    return now_kor.strftime('%Y-%m-%d')


def get_daystr(day_offset: int = 0) -> str:
    now_utc = datetime.utcnow()
    now_kor = now_utc + timedelta(hours=(9 + day_offset*24))
    return now_kor.strftime('%Y-%m-%d')


def get_time_str(in_sec: int) -> str:
    """초를 입력받아 읽기쉬운 한국 시간으로 변환"""
    hours = int(in_sec/3600)
    minutes = int((in_sec % 3600)/60)
    seconds = (in_sec % 3600) % 60

    result = []
    if hours >= 1:
        result.append(str(hours) + '시간')
    if minutes >= 1:
        result.append(str(minutes) + '분')
    if seconds >= 1:
        result.append(str(seconds) + '초')
    return ' '.join(result)


def log(clog=None):
    if YHS_LOGGER_ENABLE == 'Y':
        cf = inspect.currentframe()
        line_no = cf.f_back.f_lineno
        func_name = cf.f_back.f_code.co_name
        module_name = os.path.splitext(os.path.basename(cf.f_back.f_code.co_filename))[0]  # 전체 경로에서 확장자 없이 파일명만 취한다.

        if clog is None:
            print(f'<YHS_LOG> {get_nowtimestr()} | {module_name}.{func_name} ({line_no})')
        else:
            print(f'<YHS_LOG> {get_nowtimestr()} | {module_name}.{func_name} ({line_no}) | {clog}')
        pass
