from datetime import datetime
from math import ceil
import pandas as pd

message_length = {
    b'S': 11,  # system_event_message
    b'R': 38,  # stock_dictionary
    b'H': 24,  # stock_trading_action
    b'Y': 19,  # reg_sho_short_sale_price
    b'L': 25,  # market_participant_position
    b'V': 34,  # mwcb_decline_level_message
    b'W': 11,  # mwcb_status_message
    b'K': 27,  # ipo_quoting_period_update
    b'J': 34,  # limit_up_down_auction_collar
    b'h': 20,  # operational_halt
    b'A': 35,  # add_order_no_mpid_attribution
    b'F': 39,  # add_order_with_mpid_attribution
    b'E': 30,  # order_executed_message
    b'C': 35,  # order_executed_with_price_message
    b'X': 22,  # order_cancel_message
    b'D': 18,  # order_delete_message
    b'U': 34,  # order_replace_message
    b'P': 43,  # non_cross_trade_message
    b'Q': 39,  # cross_trade_message
    b'B': 18,  # broken_trade_execution_message
    b'I': 49,  # noii_message
    b'N': 19,  # rpii_message
}

aapl_time_pointers_metadata = pd.DataFrame(columns=['minute', 'first_event_pointer', 'last_event_pointer']).set_index('minute')


def to_int(arr):
    return int.from_bytes(arr, byteorder='big', signed=False)


def get_aapl_locate(file):
    while file.read(2):  # Entries are separated by two zero bytes.
        message_type = file.read(1)
        data = file.read(message_length[message_type])

        if message_type == b'R' and data[10:18] == b'AAPL    ':
            return data[0:2]


def process_file(file):
    while file.read(2):  # Entries are separated by two zero bytes.
        message_type = file.read(1)
        data = file.read(message_length[message_type])

        if data[0:2] == aapl_stock_locate and message_type not in [b'S', b'R', b'H']:  # get only events with trades
            process_aapl_event(data, file.tell())


def process_aapl_event(event_body, pointer):
    minute = int(ceil(to_int(event_body[4:10])) / 1e9 / 60)  # minute

    try:
        aapl_time_pointers_metadata.loc[minute]  # it will throw a key error
        aapl_time_pointers_metadata['last_event_pointer'].loc[minute] = pointer
    except:
        print(minute)
        aapl_time_pointers_metadata.loc[minute] = {
            'first_event_pointer': pointer,
            'last_event_pointer': pointer
        }


if __name__ == '__main__':
    begin_time = datetime.now()

    file_path = '/Users/yevheniy/Desktop/12302019.NASDAQ_ITCH50'

    with open(file_path, 'rb') as f:
        aapl_stock_locate = get_aapl_locate(f)

    with open(file_path, 'rb') as f:
        process_file(f)

    aapl_time_pointers_metadata.to_csv('aapl_%i.metadata' % to_int(aapl_stock_locate))

    print(datetime.now() - begin_time)
