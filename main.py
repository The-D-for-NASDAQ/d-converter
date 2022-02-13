import sys
import master
from datetime import datetime, timedelta
from time import sleep


date = master.date
# master.file_path = '/data/' + date + '.NASDAQ_ITCH50'
master.file_path = '/Users/yevheniy/Desktop/12302019.NASDAQ_ITCH50'

try:
    master.symbol = sys.argv[1].upper()
except:
    print('Symbol parameter is missing!')
    exit(1)


master.symbol_locate = master.get_symbol_locate()

process_iteration_until = datetime.now(tz=master.nasdaq_tz).replace(hour=0, minute=1, second=0, microsecond=0)
end_trading_time = datetime.now(tz=master.nasdaq_tz).replace(hour=16, minute=0, second=0, microsecond=0)

while process_iteration_until < end_trading_time:
    begin_time = datetime.now()

    master.convert_to_csv(process_iteration_until)
    added_rows = str(len(master.symbol_data_dict))
    master.save_to_csv(process_iteration_until)
    print('At: ' + str(datetime.now(tz=master.nasdaq_tz)) + ' | ' + 'Processed until: ' + str(process_iteration_until) + ' | ' + 'Processing time: ' + str(datetime.now() - begin_time) + ' | ' + 'Added rows: ' + added_rows)

    process_iteration_until = process_iteration_until + timedelta(minutes=1)
    to_next_run = process_iteration_until - datetime.now(tz=master.nasdaq_tz).replace()

    if to_next_run.total_seconds() > 0:
        print('Sleeping seconds: ' + str(to_next_run.total_seconds()))
        sleep(to_next_run.total_seconds())

# send events from 9:30 (start of trading session)
