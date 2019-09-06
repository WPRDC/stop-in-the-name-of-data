## Code for converting fieldnames and fieldwidths into boundaries for the scraped fields.
## to be compared against existing documentation on the format and contents of the 
## fixed-width files.

## Note that fieldwidths and field_names have been manually copied from extract_stop_data.py.
fieldwidths = [-1, 4, -1, 8, -1, 32, -1, 6, -1, 3, -1, 3, -1, 3, -2, 6, -1, 4]
# Negative widths represent ignored padding fields.
fieldwidths += [-1, 6, 7, -1, 8, -2, 8, -7, 4, -8, 1, -29, 4, -11, 4]
fieldwidths += [-7, 5, -14, 5, -1, 5, -47, 6]
fieldwidths = tuple(fieldwidths)
# Keep non-string fields as strings here
# and convert them (to integers/floats/datetimes)
# in the pipeline code, sending it virtual n-line CSV files.
field_names = ['stop_sequence_number', #
    'stop_id', #
    'stop_name', #
    'arrival_time', #
    'ons', #
    'offs', #
    'load', #
    'date',#
    'route',
    'pattern_variant', #
    'block_number', #
    'latitude', #
    'longitude', #
    'scheduled_trip_start_time', #trip
    'day_of_week',#
    'bus_number', #
    'scheduled_stop_time',# Schd
    'actual_run_time',
    'schedule_deviation',
    'dwell_time', #Dwell
    'departure_time' #Depart
    ]
cs = [0]
for x in fieldwidths:
    cs.append(cs[-1]+abs(x))

i_name = 0
i_column = 0
col = 0
for k,width in enumerate(fieldwidths):
    if width < 0:
        pass # This is just padding.
    else:
        print(field_names[i_name], fieldwidths[i_column], cs[k], cs[k] + fieldwidths[i_column])
        i_name += 1
    col += abs(width)
    i_column += 1
