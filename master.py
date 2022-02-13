import os
from datetime import datetime

import pandas as pd
import pytz


# set by parent file
symbol = ''
file_path = ''


file_pointer = 0
symbol_locate = None
file_order_number_pointers = {}


message_length = {
    b'S': 11,  # system_event_message
    b'R': 38,  # stock_dictionary
    b'H': 24,  # stock_trading_action
    b'Y': 19,  # reg_sho_short_sale_price
    b'L': 25,  # market_participant_position      TODO: may be used for MPID
    b'V': 34,  # mwcb_decline_level_message
    b'W': 11,  # mwcb_status_message
    b'K': 27,  # ipo_quoting_period_update
    b'J': 34,  # limit_up_down_auction_collar
    b'h': 20,  # operational_halt                 TODO: may be helpful to say d-trades that data is not full
        b'A': 35,  # add_order_no_mpid_attribution
        b'F': 39,  # add_order_with_mpid_attribution
        b'E': 30,  # order_executed_message
        b'C': 35,  # order_executed_with_price_message
        b'X': 22,  # order_cancel_message
        b'D': 18,  # order_delete_message
        b'U': 34,  # order_replace_message
        b'P': 43,  # non_cross_trade_message
        b'Q': 39,  # cross_trade_message
    b'B': 18,  # broken_trade_execution_message   TODO: ignore for now
    b'I': 49,  # noii_message
    b'N': 19,  # rpii_message
}

nasdaq_tz = pytz.timezone('US/Eastern')
date = datetime.now(tz=nasdaq_tz).strftime('%Y%m%d')

symbol_data_dict_pointer = 0
symbol_data_dict = {}


def to_int(arr):
    return int.from_bytes(arr, byteorder='big', signed=False)


def to_timestamp(arr):
    # ignore timezones in itch file (reset to UTC)
    return datetime.fromtimestamp(to_int(arr) / 1e9, tz=pytz.utc).strftime('%H:%M:%S.%f')[:-3]


def get_symbol_locate():
    with open(file_path, 'rb') as file:
        symbol_in_byte = bytes(symbol.ljust(8), 'UTF-8')
        while file.read(2):  # Entries are separated by two zero bytes.
            message_type = file.read(1)
            data = file.read(message_length[message_type])

            if message_type == b'R' and symbol_in_byte == data[10:18]:
                return data[0:2]


def convert_to_csv(process_until):
    global file_pointer, symbol_data_dict_pointer

    process_until_nanoseconds = int((process_until - process_until.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() * 1e9)

    with open(file_path, 'rb') as file:
        file.seek(file_pointer)
        while file.read(2):  # Entries are separated by two zero bytes.
            start_message_pointer = file.tell()
            message_type = file.read(1)
            data = file.read(message_length[message_type])

            order_number = to_int(data[10:18])  # not correct for P and Q (trade) messages

            if message_type == b'B':
                print(message_type)

            if symbol_locate != data[0:2]:
                continue

            if to_int(data[4:10]) >= process_until_nanoseconds:
                file.seek(start_message_pointer - 2 if start_message_pointer > 0 else 0)
                break

            if message_type in [b'E', b'C', b'X', b'D', b'U']:
                old_pointer = file.tell()
                file_order_number_pointer = file_order_number_pointers[order_number]
                file.seek(file_order_number_pointer['pointer'])
                # if message_with_all_data has b'U' type than side and other data from it will be INCORRECT!!!!!
                message_with_all_data = file.read(message_length[file_order_number_pointer['message_type']] + 1)  # +1 because message type included
                file.seek(old_pointer)

            match message_type:
                case b'A':  # add_order_no_mpid_attribution
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,                                             # Date
                        to_timestamp(data[4:10]),                         # Timestamp
                        order_number,                                     # OrderNumber
                        'ADD BID' if data[18:19] == b'B' else 'ADD ASK',  # EventType
                        symbol,                                           # Ticker
                        to_int(data[31:35]) / 1e4,                        # Price
                        to_int(data[19:23]),                              # Quantity
                        None,                                             # MPID
                        'NASDAQ'                                          # Exchange
                    ]
                    file_order_number_pointers[order_number] = {'pointer': start_message_pointer, 'message_type': message_type}
                    symbol_data_dict_pointer += 1
                case b'F':  # add_order_with_mpid_attribution
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'ADD BID' if data[18:19] == b'B' else 'ADD ASK',
                        symbol,
                        to_int(data[31:35]) / 1e4,
                        to_int(data[19:23]),
                        str(data[35:39]),
                        'NASDAQ'
                    ]
                    file_order_number_pointers[order_number] = {'pointer': start_message_pointer, 'message_type': message_type}
                    symbol_data_dict_pointer += 1
                case b'E':  # order_executed_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'EXECUTE BID' if message_with_all_data[19:20] == b'B' else 'EXECUTE ASK',
                        symbol,
                        0,
                        to_int(data[18:22]),
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                case b'C':  # order_executed_with_price_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'EXECUTE BID' if message_with_all_data[19:20] == b'B' else 'EXECUTE ASK',
                        symbol,
                        to_int(data[31:35]) / 1e4,
                        to_int(data[18:22]),
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                case b'X':  # order_cancel_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'CANCEL BID' if message_with_all_data[19:20] == b'B' else 'CANCEL ASK',
                        symbol,
                        0,
                        to_int(data[18:22]),
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                case b'D':  # order_delete_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'DELETE BID' if message_with_all_data[19:20] == b'B' else 'DELETE ASK',
                        symbol,
                        0,
                        0,
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                case b'U':  # order_replace_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        order_number,
                        'DELETE BID' if message_with_all_data[19:20] == b'B' else 'DELETE ASK',
                        symbol,
                        0,
                        0,
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                    new_order_number = to_int(data[18:26])
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        new_order_number,
                        'ADD BID' if message_with_all_data[19:20] == b'B' else 'ADD ASK',
                        symbol,
                        to_int(data[30:34]) / 1e4,
                        to_int(data[26:30]),
                        str(message_with_all_data[36:40]) if file_order_number_pointer['message_type'] == b'F' else None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                    file_order_number_pointers[new_order_number] = file_order_number_pointers[order_number]
                case b'P':  # non_cross_trade_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        0,
                        'TRADE BID' if data[18:19] == b'B' else 'TRADE ASK',
                        symbol,
                        to_int(data[31:35]) / 1e4,
                        to_int(data[19:23]),
                        None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1
                case b'Q':  # cross_trade_message
                    symbol_data_dict[symbol_data_dict_pointer] = [
                        date,
                        to_timestamp(data[4:10]),
                        0,
                        'CROSS',
                        symbol,
                        to_int(data[26:30]) / 1e4,
                        to_int(data[10:18]),
                        None,
                        'NASDAQ'
                    ]
                    symbol_data_dict_pointer += 1

        file_pointer = file.tell()


def save_to_csv(process_until):
    global symbol_data_dict, symbol_data_dict_pointer

    if not symbol_data_dict:
        return

    symbol_data_df = pd.DataFrame.from_dict(symbol_data_dict, orient='index')
    symbol_data_df.columns = ['Date', 'Timestamp', 'OrderNumber', 'EventType', 'Ticker', 'Price', 'Quantity', 'MPID', 'Exchange']

    base_file_path = 'data/' + date + '/' + symbol + '/'
    if not os.path.exists(base_file_path):
        os.makedirs(base_file_path)

    symbol_data_df_path = base_file_path + process_until.strftime('%H%M') + '.csv'

    symbol_data_df.to_csv(
        symbol_data_df_path,
        index=False,
        float_format='%.2f',
        mode='a',
        header=not os.path.exists(symbol_data_df_path)
    )

    symbol_data_dict_pointer = 0
    symbol_data_dict = {}
