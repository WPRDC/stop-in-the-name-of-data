import sys, json, csv, os, traceback
from marshmallow import fields, pre_load, post_load
from datetime import datetime, timedelta

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import time

from parameters.local_parameters import STOP_USE_SETTINGS_FILE, PRODUCTION, TEST_PACKAGE_ID

# Parsing code obtained from
#   https://stackoverflow.com/questions/4914008/how-to-efficiently-parse-fixed-width-files
try:
    from itertools import izip_longest  # added in Python 2.6
except ImportError:
    from itertools import zip_longest as izip_longest  # name change in Python 3.x

try:
    from itertools import accumulate  # added in Python 3.2
except ImportError:
    def accumulate(iterable):
        'Return running totals (simplified version).'
        total = next(iterable)
        yield total
        for value in iterable:
            total += value
            yield total
from collections import OrderedDict, defaultdict
from pprint import pprint

from notify import send_to_slack

missing_route_codes = defaultdict(int)

def make_parser(fieldwidths):
    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
    flds = tuple(izip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                                for fw in fieldwidths)
    return parse


def replace_value(record,f,old_value,new_value):
    if record[f].strip() == old_value:
        record[f] = new_value
    return record

def convert_string_to_time(time_string):
    """Convert time_string to time, accounting for time strings with hours
    of 24 or higher (which spill over to the next day)."""

    day_offset = 0
    if time_string is not None:
        try:
            if len(time_string) == 4:
                converted_time = datetime.strptime(time_string, "%H%M").time()
            else:
                converted_time = datetime.strptime(time_string, "%H%M%S").time()
        except ValueError:
            if len(time_string) == 6:
                fixed_time_str = str.zfill(str(int(time_string) - 240000),6) # To handle strings like 241331
                day_offset = 1
                converted_time = datetime.strptime(fixed_time_str, "%H%M%S").time()
            elif len(time_string) == 4:
                fixed_time_str = str.zfill(str(int(time_string) - 2400),4) # To handle strings like 2410
                day_offset = 1
                converted_time = datetime.strptime(fixed_time_str, "%H%M").time()
            else:
                print("convert_string_to_time unable to parse time_string = {}".format(time_string))
                raise
    return converted_time, day_offset


def convert_to_isodatetime(date_part,time_string):
    """Try combining date_part and time_string to get a datetime, accounting
    for time strings with hours of 24 or higher (which spill over to the next
    day)."""

    if time_string is not None and date_part is not None:
        time_part, day_offset = convert_string_to_time(time_string)
        if time_part is not None and date_part is not None:
            dt = datetime.combine(date_part, time_part) + timedelta(days=day_offset)
            return dt.isoformat()
        else:
            return None
    return None

class StopUseSchema(pl.BaseSchema):
    stop_sequence_number = fields.String(allow_none=False) # 999 values are converted to "NA" rather than None, since this is a primary key.
    # stop_sequence_number is a good primary-key component because a give stop_name can appear twice in a route with a loop but with different
    # stop_sequence_number values, differentiating the beginning of the route from the end of the route.
    stop_id = fields.String(allow_none=True)
    stop_name = fields.String(allow_none=True)
    route_decoded = fields.String(allow_none=True)
    route = fields.String(allow_none=True)
    bus_number = fields.String(allow_none=False) # key
    block_number = fields.String(allow_none=True)
    pattern_variant = fields.String(allow_none=True)
    date = fields.Date(allow_none=False) # key
    day_of_week = fields.Integer(allow_none=True) #day_of_week = fields.String(dump_to='day_of_week_code', allow_none=True)
    # The days of the week are ordered, so it makes sense to treat the day of the week as an integer.

    # [ ] Which (if any) of these should be datetimes?
    arrival_time_raw = fields.String(allow_none=True)
    arrival_time = fields.DateTime(allow_none=True)
    on = fields.Integer(allow_none=False)
    off = fields.Integer(allow_none=False)
    load = fields.Integer(allow_none=False)
    departure_time_raw = fields.String(allow_none=True)
    departure_time = fields.DateTime(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    scheduled_trip_start_time_raw = fields.String(allow_none=True)
    scheduled_trip_start_time = fields.DateTime(allow_none=True)
    scheduled_stop_time_raw = fields.String(allow_none=True) # Scheduled stoptimes need to be datetimes since they can have values like 2410.
    scheduled_stop_time = fields.DateTime(allow_none=True) # Scheduled stoptimes need to be datetimes since they can have values like 2410.
    actual_run_time = fields.Float(allow_none=True)
    schedule_deviation = fields.Float(allow_none=True)
    dwell_time = fields.Float(allow_none=True)

    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.


    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same
    #   type is not guaranteed. If you need to guarantee order of different
    #   processing steps, you should put them in the same processing method.

    @pre_load
    def convert_NAs_and_route(self, data):
        for f in data.keys():
            if type(data[f]) == str:
                data[f] = data[f].strip()

        if data['route'] in route_lookup.keys():
            data['route_decoded'] = route_lookup[data['route']]
        else:
            data['route_decoded'] = None

            route_code = data['route']
            global missing_route_codes

            # [ ] Eventually enable notifications here.
            if route_code not in missing_route_codes:
                missing_route_codes[route_code] += 1
                if route_code is None or len(route_code) >= 3:
                    error_message = "No real route designation found for route value {}.".format(route_code)
                    print(error_message)
                    #send_to_slack("SITNOD: "+error_message)
                    #raise ValueError(error_message)
                else:
                    #print("Send notification that an unknown route has been found.")
                    print("New unknown route found: {}".format(route_code))

        data = replace_value(data,'stop_sequence_number','999','NA')
        data = replace_value(data,'stop_id','00009999',None)
        #data = replace_value(data,'stop_name','Not Identified - Cal',None)
        data = replace_value(data,'pattern_variant','NA',None)
        data = replace_value(data,'actual_run_time','99.90',None)
        data = replace_value(data,'schedule_deviation','99',None)

    @pre_load
    def fix_times_and_dates(self, data):
        day_offset = 0
        date_object = datetime.strptime(data['date'], "%m%d%y").date()
        data['date'] = date_object.isoformat()
        data['departure_time_raw'] = str(data['departure_time'])
        data['arrival_time_raw'] = str(data['arrival_time'])
        data['scheduled_trip_start_time_raw'] = str(data['scheduled_trip_start_time'])
        data['scheduled_stop_time_raw'] = str(data['scheduled_stop_time'])
        # [ ] Make the 'schedule' names more similar.

        data['departure_time'] = convert_to_isodatetime(date_object, data['departure_time'])
        data['arrival_time'] = convert_to_isodatetime(date_object, data['arrival_time'])

        data = replace_value(data,'scheduled_stop_time','9999',None)
        data['scheduled_stop_time'] = convert_to_isodatetime(date_object, data['scheduled_stop_time'])
        data = replace_value(data,'scheduled_trip_start_time','9999',None)
        data['scheduled_trip_start_time'] = convert_to_isodatetime(date_object, data['scheduled_trip_start_time'])

# Resource Metadata
#package_id = '626e59d2-3c0e-4575-a702-46a71e8b0f25'     # Production
#package_id = '85910fd1-fc08-4a2d-9357-e0692f007152'     # Stage
###############
# FOR SOME PART OF THE BELOW PIPELINE, I THINK...
#The package ID is obtained not from this file but from
#the referenced settings.json file when the corresponding
#flag below is True.
def check_for_collisions(list_of_dicts,primary_keys):
    """This function only checks whether collisions occur among the rows to be
    sent in the current chunk. Use this function when rows are overwriting old
    rows. (This was a diagnostic function to try to determine why rows were
    overwriting each other, but basically there's some duplicate rows in the data.)"""
    global first_match
    counts = defaultdict(int)
    old_row = {}
    old_r = {}
    total = 0
    currently_matching = False
    current_streak = 0
    for r,d in enumerate(list_of_dicts):
        index = tuple([d[k] for k in primary_keys])
        counts[index] += 1

        if counts[index] > 1:
            total += 1
            #print("#{} | {}: last = {} =>".format(r, counts[index],index))
            #print("Old row (with r = {}):".format(old_r[index]))
            ##pprint(old_row[index])
            #print("New row:")
            if index in first_match:
                print("          >>> Another old match was found: {}".format(index))
            if d == old_row[index]:
                # Aggregate consecutively matched rows into ranges.
                if not currently_matching:
                    if index not in first_match:
                        first_match.append(index)
                current_streak += 1
                currently_matching = True
                #print("       THESE ROWS MATCH EXACTLY.")
            else:
                if currently_matching:
                    print("   !!  Streak ended at {}.".format(current_streak))
                    current_streak = 0
                currently_matching = False
                print("       These rows differ in one or more fields.")
                pprint(d)
        else:
            if currently_matching:
                print("   **  Streak ended at {}, len(first_match) = {}.".format(current_streak,len(first_match)))
                current_streak = 0
            currently_matching = False

        old_row[index] = d
        old_r[index] = r
    print("{} total collisions found.".format(total))
    return total



def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def send_data_to_pipeline(package_id,resource_name,schema,list_of_dicts,field_names,primary_keys,clear_first,chunk_size=5000):
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': resource_name}
    #else:
        #kwargs = {'resource_id': ''}

    # Call function that converts fixed-width file into a CSV file. The function
    # returns the target file path.

    # Synthesize virtual file to send to the FileConnector
    from tempfile import NamedTemporaryFile
    ntf = NamedTemporaryFile()

    # Save the file path
    target = ntf.name

    write_to_csv(target,list_of_dicts,field_names)

    # Testing temporary named file:
    #ntf.seek(0)
    #with open(target,'r') as g:
    #    print(g.read())

    ntf.seek(0)
    #target = '/Users/drw/WPRDC/Tax_Liens/foreclosure_data/raw-seminull-test.csv'
    #target = process_foreclosures.main(input = fixed_width_file)

    server = "test-production"
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(STOP_USE_SETTINGS_FILE) as f:
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)


    stop_use_pipeline = pl.Pipeline('stop_use_pipeline',
                                      'Pipeline for Bus Stop-Use Data',
                                      log_status=False,
                                      settings_file=STOP_USE_SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0,
                                      chunk_size=chunk_size
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=schema().serialize_to_ckan_fields(capitalize=False),
              package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              clear_first=clear_first,
              key_fields=primary_keys,
              method='upsert',
              **kwargs).run()
    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {} on {}".format(kwargs['resource_name'],site))
        log.write("Finished upserting to {} at {} \n".format(kwargs['resource_name'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    else:
        print("Piped data to {} on {}".format(kwargs['resource_id'],site))
        log.write("Finished upserting to {} at {} \n".format(kwargs['resource_id'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    log.close()
    ntf.close()
    assert not os.path.exists(target)

stop_use_package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package

jobs = [
    {
        'package': stop_use_package_id,
        'source_file': '',
        'source_directory': '',
        'resource_name': 'Stop-Use Data (beta)',
        'schema': StopUseSchema
    },
]

def process_job(job,use_local_files,clear_first,test_mode,mute_alerts,filepaths):
    package_id = job['package'] if not test_mode else TEST_PACKAGE_ID
    resource_name = job['resource_name']
    schema = job['schema']
    fields0 = schema().serialize_to_ckan_fields()
    # Eliminate fields that we don't want to upload.
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
    # Add some new fields.
    #fields0.append({'id': 'assignee', 'type': 'text'})
    fields_to_publish = fields0
    print("fields_to_publish = {}".format(fields_to_publish))
    field_names_to_publish = [f['id'] for f in fields_to_publish]

    #line = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'
    #fieldwidths = (2, -10, 24)  # negative widths represent ignored padding fields
    #parse = make_parser(fieldwidths)
    #fields = parse(line)
    #print('format: {!r}, rec size: {} chars'.format(parse.fmtstring, parse.size))
    #print('fields: {}'.format(fields))
    ##Output:
    ##format: '2s 10x 24s', rec size: 36 chars
    ##fields: ('AB', 'MNOPQRSTUVWXYZ0123456789')

    fieldwidths = [-1, 4, -1, 8, -1, 32, -1, 6, -1, 3, -1, 3, -1, 3, -2, 6, -1, 4]
    # Negative widths represent ignored padding fields.
    fieldwidths += [-1, 6, 7, -1, 8, -2, 8, -7, 4, -8, 1, -29, 4, -11, 4]
    fieldwidths += [-7, 5, -14, 5, -1, 5, -47, 6]
    fieldwidths = tuple(fieldwidths)
    parse = make_parser(fieldwidths)
    print('format: {!r}, rec size: {} chars'.format(parse.fmtstring, parse.size))

    # Keep non-string fields as strings here
    # and convert them (to integers/floats/datetimes)
    # in the pipeline code, sending it virtual n-line CSV files.
    field_names = ['stop_sequence_number', #
        'stop_id', #
        'stop_name', #
        'arrival_time', #
        'on', #
        'off', #
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


    # Check that field_names are all contained within fields_to_publish.
    # (Extra fields may have been added to field_names_to_publish.)
    #print("set difference = {}".format(set(field_names) - set(field_names_to_publish)))
    assert len(set(field_names) - set(field_names_to_publish)) == 0

    #Check that all primary keys are in field_names. # The ETL library should do this.
    primary_keys = ['date','arrival_time','block_number','stop_name','stop_sequence_number','latitude','longitude']
    # Experimenting with a 100k-row sample has shown that this 7-key combination seems to eliminate all the uninteresting duplicates.
    # Adding 'on', 'off', and 'load' does not change the resulting row count (about 97030 rows).

    # stop_name is sometimes converted to None...!
    # Experimenting with converting route value of 0 to None and using
    # block_number instead as a primary key.
    # What about bus_number or route? Wouldn't bus_number be a logical thing to sort by? Date first, then bus_number, then arrival_time/stop_sequence_number?
    # Shouldn't stop_sequence_number+bus-route-identifier be synonymous with stop_name?
    # Which fields do we want to have indexed?

    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.

    assert len(set(primary_keys) - set(field_names)) == 0

    if len(filepaths) == 0:
        filepaths = ['a_sample'] # This is the fixed-width file containing the raw data.

    first_match = []

    first_line = 2
    list_of_dicts = []
    n = 0
    chunk_size = 5000
    #total_collisions = 0
    for filename in filepaths:
        with open(filename, 'r', newline='\r\n') as f:
            for n,line in enumerate(f):
                if n >= first_line:
                    fields = parse(line)
                    #if n == 2 or n==34:
                    #    pprint(list(zip(field_names,fields)))
                    named_fields = OrderedDict(zip(field_names,fields))
                    list_of_dicts.append(named_fields)

                if len(list_of_dicts) == chunk_size:
                    # Push data to ETL pipeline
                    #total_collisions += check_for_collisions(list_of_dicts,primary_keys)
                    send_data_to_pipeline(package_id,resource_name,schema,list_of_dicts,field_names_to_publish,primary_keys,clear_first,chunk_size+1)
                    list_of_dicts = []

        #total_collisions += check_for_collisions(list_of_dicts,primary_keys)
        send_data_to_pipeline(package_id,resource_name,schema,list_of_dicts,field_names_to_publish,primary_keys,clear_first)
    print("Here's the tally of uncategorized route codes:")
    pprint(missing_route_codes)
    if not mute_alerts:
        send_to_slack("SITNOD was unable to find these route codes: {}".format(missing_route_codes))
#print("Total collisions (within 5000-record chunks): {}".format(total_collisions))


def main(selected_job_codes,use_local_files=False,clear_first=False,test_mode=False,mute_alerts=False,filepaths=[]):
    if selected_job_codes == []:
        selected_jobs = list(jobs)
    else:
        selected_jobs = [j for j in jobs if (j['source_file'] in selected_job_codes)]
    for job in selected_jobs:
        process_job(job,use_local_files,clear_first,test_mode,mute_alerts,filepaths)

if __name__ == '__main__':
#   # stuff only to run when not called via 'import' here
    with open('RouteCodes.csv', mode='r') as infile:
        reader = csv.reader(infile)
        route_lookup = {r[1]:r[0] for r in reader}

    route_lookup['0'] = None

    args = sys.argv[1:]
    copy_of_args = list(args)
    mute_alerts = False
    use_local_files = False
    clear_first = False
    test_mode = not PRODUCTION # Use PRODUCTION boolean from parameters/local_parameters.py to set whether test_mode defaults to True or False
    job_codes = [j['source_file'] for j in jobs]
    selected_job_codes = []
    filepaths = []
    try:
        for k,arg in enumerate(copy_of_args):
            if arg in ['mute']:
                mute_alerts = True
                args.remove(arg)
            elif arg in ['local']:
                use_local_files = True
                args.remove(arg)
            elif arg in ['clear_first']:
                clear_first = True
                args.remove(arg)
            elif arg in ['test']:
                test_mode = True
                args.remove(arg)
            elif arg in job_codes:
                selected_job_codes.append(arg)
                args.remove(arg)
            else: # Check whether the argument could be a local file name or path.
                file_exists = os.path.exists(arg)
                if file_exists:
                    args.remove(arg)
                    filepaths.append(arg)
        if len(args) > 0:
            print("Unused command-line arguments: {}".format(args))

        main(selected_job_codes,use_local_files,clear_first,test_mode,mute_alerts,filepaths)
    except:
        e = sys.exc_info()[0]
        msg = "Error: {} : \n".format(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        msg = ''.join('!! ' + line for line in lines)
        print(msg) # Log it or whatever here
        if not mute_alerts:
            channel = "@david" if test_mode else "#etl-hell"
            send_to_slack(msg,username='PLI Violations ETL assistant',channel=channel,icon=':illuminati:')
