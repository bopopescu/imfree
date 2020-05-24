from datetime import timedelta, datetime


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def epoch_sec(process_dt):
    epoch = datetime(1970, 1, 1)
    date_start = datetime.strptime(str(process_dt) + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
    date_end = datetime.strptime(str(process_dt) + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
    epoch_start = (date_start - epoch).total_seconds()
    epoch_end = (date_end - epoch).total_seconds()

    return [epoch_start, epoch_end]
