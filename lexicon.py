import sys, csv, json, ckanapi, fire
from pprint import pprint
from credentials import site, ckan_api_key as API_key

# Base functions: 1) Download the data dictionary for a given resource ID.
#   2) Upload a data dictionary for a given resource ID.
#   3) Validate a data dictionary (either a file or one found attached to a CKAN resource).
#   4) Convert a data dictionary to the default integrated-data-dictionary format for a given CKAN instance, allowing a conversion schema to passed as an argument.

#####
DEFAULT_CKAN_DD_COLUMNS = ['column', 'type', 'label', 'description']


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    # obtained from https://code.activestate.com/recipes/577058/

    # Then modified to work under both Python 2 and 3.
    # (Python 3 renamed "raw_input()" to "input()".)
    global input

    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    try: input = raw_input
    except NameError: pass

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def get_resource_parameter(site,resource_id,parameter=None,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

def get_schema(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
    except:
        return None

    return schema

def convert_definitions_to_fields(definitions):
    fields = []
    for d in definitions:
        field = {'id': d['column']}
        field_info = {}
        if 'type' in d:
            field_info['type_override'] = d['type']
        if 'label' in d:
            field_info['label'] = d['label']
        if 'description' in d:
            field_info['notes'] = d['description']
    return fields

def get_ckan_data_dictionary(resource_id,API_key=None):
    # Example of a minimal data dictionary, as returned from the CKAN API:
    # [{'id': '_id', 'type': 'int'},
    #  {'id': 'CREATED_ON',
    #   'info': {'label': 'Date created',
    #            'notes': 'Date the request was created',
    #            'type_override': ''},
    #   'type': 'timestamp'}]

    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    r = ckan.action.datastore_search(resource_id=resource_id)
    # A tabular resource with no uploaded/entered integrated
    # data dictionary will only lack the 'info' dictionaries for each field.

    # Non-tabular data will elicit a response like this:
    #   ckanapi.errors.NotFound: Resource "9d1c01df-2abd-45f3-8748-7b2e1cf8c47f" was not found.
    if 'fields' not in r:
        return None
    return r['fields']


def update_ckan_data_dictionary(definitions,resource_id,API_key=None):
    # Use the datastore_create endpoint to update the integrated data dictionary
    # resource_id (string) - resource id that the data is going to be stored against.
    # force (bool (optional, default: False)) - set to True to edit a read-only resource
    # resource (dictionary) - resource dictionary that is passed to resource_create(). Use instead of resource_id (optional)
    # aliases (list or comma separated string) - names for read only aliases of the resource. (optional)
    # fields (list of dictionaries) - fields/columns and their extra metadata. (optional)
    # records (list of dictionaries) - the data, eg: [{"dob": "2005", "some_stuff": ["a", "b"]}] (optional)
    # primary_key (list or comma separated string) - fields that represent a unique key (optional)
    # indexes (list or comma separated string) - indexes on table (optional)
    # triggers (list of dictionaries) - trigger functions to apply to this table on update/insert. functions may be created with datastore_function_create(). eg: [ {"function": "trigger_clean_reference"}, {"function": "trigger_check_codes"}]
    
    fields = convert_definitions_to_fields(definitions)
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    r = ckan.action.datastore_create(resource_id=resource_id, fields=fields)
    # The above line yields this weird error:
    # ckanapi.errors.ValidationError: {u'__type': u'Validation Error', u'read-only': [u'Cannot edit read-only resource. Either pass"force=True" or change url-type to "datastore"']}
    #r = ckan.action.datastore_create(resource_id=resource_id, fields=fields, force=True)
    # Setting force=True squashes the error, but no new data-dictionary values are in evidence.
    return r

def download(resource_id):
    from credentials import site, ckan_api_key as API_key
    dd = get_ckan_data_dictionary(resource_id, API_key)
    filename = "{}-dd.csv".format(resource_id)
    print("Saving data dictionary to {}".format(filename))
    with open(filename, 'w') as f:
        json.dump(dd, f, indent=4)

def upload(resource_id,filename):
    schema = get_schema(site,resource_id,API_key)

    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        definitions = []
        for row in reader:
            definitions.append(row)

    # Validate new data-dictionary definitions.
    # 1) Check columns.
    if not set(cols).issubset(set(DEFAULT_CKAN_DD_COLUMNS)):
        raise ValueError("WHOA! {} contains one or more fields not expected for CKAN's integrated data dictionaries: {}".format(cols, set(cols).difference(set(DEFAULT_CKAN_DD_COLUMNS))))

    if DEFAULT_CKAN_DD_COLUMNS[0] != cols[0]:
        raise ValueError("The first column of the data dictionary should be {}, not {}.".format(DEFAULT_CKAN_DD_COLUMNS[0], cols[0]))

    # 2) Check that all rows in given data dictionary match fields already in the CKAN resource.
    defined = [x[cols[0]] for x in definitions]
    fields = [d['id'] for d in schema]
    if not set(defined).issubset(set(fields)):
        raise ValueError("Some of the defined fields in the data dictionary don't appear in the CKAN resource: {}".format(set(defined).difference(set(fields))))

    # 3) Check for changes to the type column.
    idd = get_ckan_data_dictionary(resource_id,API_key)
    type_by_field = {f['id']:f['type'] for f in idd} 

    for definition in definitions:
        new_type = definition['type']
        field = definition['column']
        old_type = type_by_field[field]
        if old_type != new_type:
            proceed = query_yes_no("Should the type of {} be changed from {} to {}?".format(field,old_type,new_type), default="no")
            if not proceed:
                raise ValueError("Aborting this whole operation!")
    # End validation phase
    pprint(definitions)

    result = update_ckan_data_dictionary(definitions,resource_id,API_key)

def convert(filename):
    pass

if __name__ == '__main__':
    fire.Fire()
