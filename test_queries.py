import ckanapi

from credentials import site, API_key, TEST_PACKAGE_ID

from icecream import ic

from query_util import get_package_parameter
from byway import set_table, clear_table, check_table
from extract_stop_data import jobs

ckan = ckanapi.RemoteCKAN(site, apikey=API_key)


def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.

    # Note that this doesn't work for private datasets.
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def find_resource_id(site,package_id,resource_name,API_key=None):
#def get_resource_id_by_resource_name():
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site, package_id, 'resources', API_key)
    for r in resources:
        if 'name' in r and r['name'] == resource_name:
            return r['id']
    return None

stop_use_package_id = TEST_PACKAGE_ID
package_id = stop_use_package_id
#resource_name = 'Stop-Use Data - all-in-one  - 2016-04'
#resource_name = 'Stop-Use Data - improved - 2016-03'
cumulative_resource_name = "{} (Cumulative)".format(jobs[0]['resource_name'])
resource_name = "{} - {}".format(jobs[0]['resource_name'], '2016-04')

resource_id = find_resource_id(site, package_id, resource_name, API_key)
route = '67'
#query = 'SELECT * FROM "{}" LIMIT 3'.format(resource_id)
#query = 'SELECT "on" FROM "{}" LIMIT 3'.format(resource_id)
#query = 'SELECT SUM("on") FROM "{}" LIMIT 2'.format(resource_id) # Query took too long
#query = 'SELECT SUM(load) as sum_loads FROM "{}" WHERE route_decoded = \'{}\' LIMIT 3'.format(resource_id, route) # This query took a while, but returned with 3,008,446.
# Since it doesn't make sense to add the load from any segment, this is not a meaningful value, but it does demonstrate the functioning of the query (and reveals
# that it takes a while to run, but does not time out. This is probably for three months of data (and about 14 million records).
#query = 'SELECT "SUM(on)" as sum_ons FROM "{}" WHERE route_decoded = \'{}\''.format(resource_id, route)
#query = 'SELECT AVG(load) as avg_load FROM "{}" WHERE route_decoded = \'{}\''.format(resource_id, route)
query = 'SELECT AVG(load) as avg_load FROM "{}" WHERE route = \'{}\''.format(resource_id, route)

query = 'SELECT AVG(load) as avg_load FROM "{}" WHERE block_number = \'0011110\''.format(resource_id) # query took too long

query = 'SELECT AVG(load) as avg_load FROM "{}" WHERE stop_name = \'9TH ST AT PENN AVE\''.format(resource_id) # query took too long
query = 'SELECT AVG(load) as avg_load FROM "{}" WHERE stop_id = \'P01040\''.format(resource_id) # This query is really fast, because stop_id was an indexed field.



#query = 'SELECT "ons" FROM "{}" WHERE route = \'{}\' LIMIT 30'.format(resource_id, route)
query = 'SELECT SUM(ons) FROM "{}" WHERE route = \'{}\''.format(resource_id, route)
#query = 'SELECT SUM(on) AS ons FROM "{}" WHERE route_decoded = \'{}\''.format(resource_id, route)
#query = 'SELECT sum(on) AS ons, sum(offs) AS offs FROM {} WHERE route = \'{}\''.format(resource_id, route)
#query = 'SELECT sum(ons) AS all_ons, sum(offs) AS all_offs FROM {} WHERE route = \'{}\''.format(resource_id, route)
#query = 'SELECT sum(ons) AS all_ons, sum(offs) AS all_offs FROM {} WHERE route = \'{}\' AND date < {} AND date > {}'.format(resource_id, route)
needs_setting = check_table(resource_id)
if needs_setting:
    set_table(resource_id)
results = query_resource(site, query, API_key)
ic(results)
if needs_setting:
    clear_table(resource_id)
