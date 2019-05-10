# [ ] Combine date and time into datetime?
import sys, json, datetime, csv, os
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import time

from parameters.local_parameters import STOP_USE_SETTINGS_FILE

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
from collections import OrderedDict
from pprint import pprint

from notify import send_to_slack

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

def convert_string_to_isotime(f):
    if f is not None:
        if len(f) == 4:
            u = datetime.datetime.strptime(f, "%H%M").time().isoformat()
        else:
            u = datetime.datetime.strptime(f, "%H%M%S").time().isoformat()
        f = u
        return u
    return f

class StopUseSchema(pl.BaseSchema): 
    real_arrival_datetime = fields.DateTime(allow_none=False)
    stop_sequence_number = fields.String(allow_none=True)
    stop_id = fields.String(allow_none=True)
    stop_name = fields.String(allow_none=True)
    route = fields.String(allow_none=True)
    bus_number = fields.String(allow_none=False) # key
    block_number = fields.String(allow_none=True)
    pattern_variant = fields.String(allow_none=True)
    date = fields.Date(allow_none=False) # key
    day_of_week = fields.Integer(allow_none=True) #day_of_week = fields.String(dump_to='day_of_week_code', allow_none=True)
    # The days of the week are ordered, so it makes sense to treat the day of the week as an integer.

    # [ ] Which (if any) of these should be datetimes?
    arrival_time = fields.Time(allow_none=True)
    on = fields.Integer(allow_none=False)
    off = fields.Integer(allow_none=False)
    load = fields.Integer(allow_none=False)
    departure_time = fields.Time(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    scheduled_trip_start_time = fields.Time(allow_none=True)
    scheduled_stop_time = fields.Time(allow_none=True)
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
            data['route'] = route_lookup[data['route']]
        else:
            if len(data['route']) < 3:
                print("Send notification that an unknown route has been found.")
            else:
                error_message = "No real route designation found for route value {}.".format(data['route'])
                send_to_slack("SITNOD: "+error_message)
                raise ValueError(error_message)

        data = replace_value(data,'stop_sequence_number','999',None)
        data = replace_value(data,'stop_id','00009999',None)
        #data = replace_value(data,'stop_name','Not Identified - Cal',None) 
        data = replace_value(data,'pattern_variant','NA',None) 
        data = replace_value(data,'actual_run_time','99.90',None) 

    @pre_load
    def fix_times_and_dates(self, data):
        day_offset = 0
        date_object = datetime.datetime.strptime(data['date'], "%m%d%y").date()
        data['date'] = date_object.isoformat()
        try:
            data['departure_time'] = convert_string_to_isotime(data['departure_time'])
        except:
            if len(data['departure_time']) == 6:
                original = str(data['departure_time'])
                data['departure_time'] = str.zfill(str(int(data['departure_time']) - 240000),6)
                day_offset = 1
                data['departure_time'] = convert_string_to_isotime(data['departure_time'])
            else:
                raise ValueError("Unable to convert departure time {}".format(data['departure_time']))

        try:
            arrival_time = datetime.datetime.strptime(data['arrival_time'], "%H%M%S").time()
            data['arrival_time'] = convert_string_to_isotime(data['arrival_time'])
        except:
            if len(data['arrival_time']) == 6:
                original = str(data['arrival_time'])
                data['arrival_time'] = str.zfill(str(int(data['arrival_time']) - 240000),6)
                arrival_time = datetime.datetime.strptime(data['arrival_time'], "%H%M%S").time()
                data['arrival_time'] = convert_string_to_isotime(data['arrival_time'])
            else:
                raise ValueError("Unable to convert arrival time {}".format(data['arrival_time']))

        dt = datetime.datetime.combine(date_object, arrival_time) + datetime.timedelta(days=1)
        data['real_arrival_datetime'] = dt.isoformat()

        data = replace_value(data,'scheduled_stop_time','9999',None)
        if data['scheduled_stop_time'] is not None:
            data['scheduled_stop_time'] = convert_string_to_isotime(data['scheduled_stop_time'])

        data = replace_value(data,'scheduled_trip_start_time','9999',None)
        if data['scheduled_trip_start_time'] is not None:
            data['scheduled_trip_start_time'] = convert_string_to_isotime(data['scheduled_trip_start_time'])

        #if data['departure_time'] is not None:
        #    data['departure_time'] = datetime.datetime.strptime(data['departure_time'], "%H%M%S").time().isoformat()
        #if data['arrival_time'] is not None:
        #    data['arrival_time'] = datetime.datetime.strptime(data['arrival_time'], "%H%M%S").time().isoformat()

    #        try: # This may be the satisfactions-file format.
    #            data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d").date().isoformat()
    #        except:
    #            try:
    #                data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S.%f").date().isoformat()
    #            except:
    #                # Try the original summaries format
    #                try:
    #                    data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%Y-%m-%d %H:%M:%S").date().isoformat()
    #                except:
    #                    # Try the format I got in one instance when I exported the 
    #                    # data from CKAN and then reimported it:
    #                     data['filing_date'] = datetime.datetime.strptime(data['filing_date'], "%d-%b-%y").date().isoformat()
    #    else:
    #        print("No filing date for {} and data['filing_date'] = {}".format(data['dtd'],data['filing_date']))
    #        data['filing_date'] = None

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
    rows."""
    global first_match
    from collections import defaultdict
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

def send_data_to_pipeline(schema,list_of_dicts,field_names,primary_keys,chunk_size=5000):
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': 'Stop-Use Data (alpha)'}
    #else:
        #kwargs = {'resource_id': ''}
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.

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
    package_id = settings['loader'][server]['package_id']

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
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=primary_keys,
              method='upsert',
              **kwargs).run()
    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {} on {}".format(kwargs['resource_name'],site))
        log.write("Finished upserting {} at {} \n".format(kwargs['resource_name'],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    else:
        print("Piped data to {} on {}".format(kwargs['resource_id'],site))
        log.write("Finished upserting {} at {} \n".format(kwargs['resource_id'],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    log.close()
    ntf.close()
    assert not os.path.exists(target)

schema = StopUseSchema
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
primary_keys = ['date','arrival_time','block_number','stop_name','stop_sequence_number','on','off','load','latitude','longitude']
# stop_name is sometimes converted to None...!
# Experimenting with converting route value of 0 to None and using 
# block_number instead as a primary key.

# Collisions occur if the fourth key is stop_sequence_number (but leaving it out in favor of stop_name seems to drop more records than is desirable).
assert len(set(primary_keys) - set(field_names)) == 0

with open('RouteCodes.csv', mode='r') as infile:
    reader = csv.reader(infile)
    route_lookup = {r[1]:r[0] for r in reader}

route_lookup['0'] = None

filename = 'a_sample' # This is the fixed-width file containing the raw data.

if len(sys.argv) > 1:
    filename = sys.argv[1]

first_match = []

first_line = 2
list_of_dicts = []
n = 0
chunk_size = 5000
total_collisions = 0
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
            total_collisions += check_for_collisions(list_of_dicts,primary_keys)
            send_data_to_pipeline(schema,list_of_dicts,field_names_to_publish,primary_keys,chunk_size+1)
            list_of_dicts = []

total_collisions += check_for_collisions(list_of_dicts,primary_keys)
send_data_to_pipeline(schema,list_of_dicts,field_names_to_publish,primary_keys)
print("Total collisions (within 5000-record chunks): {}".format(total_collisions))
#pprint(dict(named_fields))
#pprint(list_of_dicts)


#if __name__ == "__main__":
#   # stuff only to run when not called via 'import' here
#    main()
