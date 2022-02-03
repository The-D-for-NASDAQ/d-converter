from datetime import datetime
import pandas as pd
import pytz

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

nasdaq_tz = pytz.timezone('UTC')


def to_int(arr):
    return int.from_bytes(arr, byteorder='big', signed=False)


def to_timestamp(arr):
    return datetime.fromtimestamp(to_int(arr) / 1e9, tz=nasdaq_tz).strftime('%H:%M:%S.%f')[:-3]


def process_file(file, pointer):
    while file.read(2):  # Entries are separated by two zero bytes.
        start_message_pointer = file.tell()
        message_type = file.read(1)
        data = file.read(message_length[message_type])

        order_number = to_int(data[10:18])  # not correct for P and Q (trade) messages

        if message_type == b'B':
            print(message_type)

        if symbol_locate != data[0:2]:
            continue

        if message_type in [b'E', b'C', b'X', b'D', b'U']:
            old_pointer = file.tell()
            file_order_number_pointer = file_order_number_pointers[order_number]
            file.seek(file_order_number_pointer['pointer'])
            message_with_all_data = file.read(message_length[file_order_number_pointer['message_type']] + 1)  # +1 because message type included
            file.seek(old_pointer)

        # Date
        # Timestamp
        # OrderNumber
        # EventType
        # Ticker
        # Price
        # Quantity
        # MPID
        # Exchange

        match message_type:
            case b'A':  # add_order_no_mpid_attribution
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'ADD BID' if data[18] == b'B' else 'ADD ASK',
                    symbol,
                    to_int(data[31:35]) / 1e4,
                    to_int(data[19:23]),
                    None,
                    'NASDAQ'
                ]
                file_order_number_pointers[order_number] = {'pointer': start_message_pointer, 'message_type': message_type}
                pointer += 1
            case b'F':  # add_order_with_mpid_attribution
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'ADD BID' if data[18] == b'B' else 'ADD ASK',
                    symbol,
                    to_int(data[31:35]) / 1e4,
                    to_int(data[19:23]),
                    data[35:39],
                    'NASDAQ'
                ]
                file_order_number_pointers[order_number] = {'pointer': start_message_pointer, 'message_type': message_type}
                pointer += 1
            case b'E':  # order_executed_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'EXECUTE BID' if message_with_all_data[19] == b'B' else 'EXECUTE ASK',
                    symbol,
                    0,
                    to_int(message_with_all_data[20:24]),
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                pointer += 1
            case b'C':  # order_executed_with_price_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'EXECUTE BID' if message_with_all_data[19] == b'B' else 'EXECUTE ASK',
                    symbol,
                    to_int(data[31:35]) / 1e4,
                    to_int(message_with_all_data[20:24]),
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                pointer += 1
            case b'X':  # order_cancel_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'CANCEL BID' if message_with_all_data[19] == b'B' else 'CANCEL ASK',
                    symbol,
                    0,
                    to_int(data[18:22]),
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                pointer += 1
            case b'D':  # order_delete_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'DELETE BID' if message_with_all_data[19] == b'B' else 'DELETE ASK',
                    symbol,
                    0,
                    0,
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                pointer += 1
            case b'U':  # order_replace_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    order_number,
                    'DELETE BID' if message_with_all_data[19] == b'B' else 'DELETE ASK',
                    symbol,
                    0,
                    0,
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                replaced_order_number = to_int(data[18:26])
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    replaced_order_number,
                    'ADD BID' if message_with_all_data[18] == b'B' else 'ADD ASK',
                    symbol,
                    to_int(data[30:34]) / 1e4,
                    to_int(data[26:30]),
                    message_with_all_data[36:40] if file_order_number_pointer['message_type'] == b'F' else None,
                    'NASDAQ'
                ]
                file_order_number_pointers[replaced_order_number] = {'pointer': start_message_pointer, 'message_type': message_type}
                pointer += 1
            case b'P':  # non_cross_trade_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    0,
                    'TRADE BID' if data[18] == b'B' else 'TRADE ASK',
                    symbol,
                    to_int(data[31:35]) / 1e4,
                    to_int(data[19:23]),
                    None,
                    'NASDAQ'
                ]
                pointer += 1
            case b'Q':  # cross_trade_message
                symbol_data_dict[pointer] = [
                    Date,
                    to_timestamp(data[4:10]),
                    0,
                    'CROSS',
                    symbol,
                    to_int(data[26:30]) / 1e4,
                    to_int(data[10:18]),
                    None,
                    'NASDAQ'
                ]
                pointer += 1


def get_symbol_locate(file):
    symbol_in_byte = bytes(symbol.ljust(8), 'UTF-8')
    while file.read(2):  # Entries are separated by two zero bytes.
        message_type = file.read(1)
        data = file.read(message_length[message_type])

        if message_type == b'R' and symbol_in_byte == data[10:18]:
            return data[0:2]


if __name__ == '__main__':
    begin_time = datetime.now()

    symbol = 'AAPL'
    file_path = '/Users/yevheniy/Desktop/12302019.NASDAQ_ITCH50'

    with open(file_path, 'rb') as f:
        symbol_locate = get_symbol_locate(f)

    Date = datetime.now().strftime('%Y%m%d')

    file_order_number_pointers = {}

    symbol_data_dict_pointer = 0
    symbol_data_dict = {}

    with open(file_path, 'rb') as f:
        process_file(f, symbol_data_dict_pointer)

    symbol_data_df = pd.DataFrame.from_dict(symbol_data_dict, orient='index')
    symbol_data_df.columns = ['Date', 'Timestamp', 'OrderNumber', 'EventType', 'Ticker', 'Price', 'Quantity', 'MPID',
                              'Exchange']

    symbol_data_df.to_csv(symbol + Date + '.csv', index=False)

    print(datetime.now() - begin_time)
