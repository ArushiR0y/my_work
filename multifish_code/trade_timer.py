from datetime import datetime, timedelta

def get_trading_times(file_path: str):
    """
    Reads trading times from a file and returns a list of datetime objects
    for the current date.

    :param file_path: Path to the file containing trading times in HH:MM format
    :return: List of datetime objects for the current date
    """
    # Read the file and parse the times
    with open(file_path, 'r') as file:
        trading_times = [line.strip() for line in file]

    # Get the current date
    current_date = datetime.now().date()

    # Combine each time with the current date to create datetime objects
    datetime_objects = []
    for time_str in trading_times:
        time_obj = datetime.strptime(time_str, "%H:%M").time()
        datetime_obj = datetime.combine(current_date, time_obj)
        datetime_objects.append(datetime_obj)

    return datetime_objects



