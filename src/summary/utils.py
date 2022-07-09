from datetime import datetime


def try_parsing_date(text: str, frm: str):
    try:
        return datetime.strptime(text, frm)
    except Exception as e:
        print('DATUM-ERROR', e)