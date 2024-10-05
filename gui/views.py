from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.template.loader import render_to_string
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Count

from rest_framework.response import Response
from rest_framework.decorators import api_view

from cfs.models import (VariableProperty, VariablePropertyKeys, Variable, Cell_Method,
                        Location, Collection)
from cfs.db.interface import VariableProperyInterface, VariableInterface
from gui.serializers import VariableSerializer

# Create your views here.

def index(request):
    """ Core index view """
    return HttpResponse('Hello world')

def view(request):
    return render(request, 'gui/view.html')

def oldview(request):
    return render(request,'gui/view0.html')

def view1(request):
    return render(request,'gui/view1.html')

###
### API Rest queries follow
###

@api_view(['GET'])
def get_variable_property_by_key(request):
    """ 
    Get all the variable properties for a particular key
    """
    key_value = request.GET.get('key', None)
    
    # Check if key_value is valid or exists in VariablePropertyKeys
    if key_value not in VariablePropertyKeys.values:
        return Response({'error': 'Invalid key'}, status=400)
    
    # Filter VariableProperty entries with the selected key
    properties = VariableProperty.objects.filter(key=key_value)
    
    # Prepare data for response (list of dictionaries)
    data = [{'id': prop.id, 'key': prop.key, 'value': prop.value} for prop in properties]

    return Response(data)


@api_view(['GET'])
def get_cell_methods(request): 
    """ 
    Get availabe cell methods
    """
    cell_methods = Cell_Method.objects.all()
    pairs= [{'id': cm.id, 'method': str(cm)} for cm in cell_methods]
    return Response(pairs)


@api_view(['GET'])
def get_available_keys(request):
    ''' 
    Fetch all available keys from VariablePropertyKeys
    '''
    keys = [{'value': key, 'label': label} for key, label in VariablePropertyKeys.choices]
    
    return Response(keys)

@api_view(['GET'])
def get_variable_properties_by_key(request):
    # Get the key parameter from the query string
    key_value = request.GET.get('key', None)
    
    # Check if key_value is valid or exists in VariablePropertyKeys
    if key_value not in VariablePropertyKeys.values:
        return Response({'error': 'Invalid key'}, status=400)
    
    # Filter VariableProperty entries with the selected key
    properties = VariableProperty.objects.filter(key=key_value)
    
    # Prepare data for response (list of dictionaries)
    data = [{'id': prop.id, 'key': prop.key, 'value': prop.value} for prop in properties]

    return Response(data)

@api_view(['GET'])
def get_variable_properties(request):
    """ 
    This is used for the faceted browse
    """
    cell_methods = request.query_params.getlist('cell_methods')  # Accept multiple values

    # Query for variables based on the selected cell methods
    variables = Variable.objects.filter(cell_methods__id__in=cell_methods)

    # Collect standard names and long names
    standard_names = []
    long_names = []

    for variable in variables:
        for prop in variable.key_properties.all():
            if prop.key == 'SN' and prop.value not in [name['value'] for name in standard_names]:
                standard_names.append({'value': prop.value})
            elif prop.key == 'LN' and prop.value not in [name['value'] for name in long_names]:
                long_names.append({'value': prop.value})

    return Response({'standard_names': standard_names, 'long_names': long_names})

@api_view(['GET'])
def get_view_initial_options(request):
    cell_methods = Cell_Method.objects.all()[:5]  # Fetch first 5 cell methods
    standard_names = VariableProperty.objects.filter(key='SN')[:5]  # Fetch first 5 standard names
    long_names = VariableProperty.objects.filter(key='LN')[:5]  # Fetch first 5 long names

    initial_data = {
        'cell_methods': [{'id': cm.id, 'method': str(cm)} for cm in cell_methods],
        'standard_names': [{'value': prop.value} for prop in standard_names],
        'long_names': [{'value': prop.value} for prop in long_names]
    }

    return Response(initial_data)


@api_view(['GET'])
def vocab_select(request):
    """ 
    Get a vocabulary for use in drop downs. Optinally filter the
    vocabulary to only show the options given some other constraint.
    """
    vocab = request.query_params.get('vocab')  # Do not accept multiple values
    loc = request.query_params.get('location',[])
    col = request.query_params.get('collection',[])
    
    key = VariablePropertyKeys.mykey(vocab)
    if loc:
        loc = loc.split(',')
    if col:
        col = col.split(',')
    data = [{'id':v.id, 'name':v.value} for v in 
            VariableProperyInterface.filter_properties(keylist=[key],
                                                       collection_ids=col,
                                                       location_ids=loc)]
    return Response(data)

@api_view(['GET'])
def entity_select(request):
    entity = request.query_params.get('entity')  # Do not accept multiple values
    target = {'collection':Collection, 'location':Location}[entity]
    data = [{'id':v.id, 'name':v.name} for v in target.objects.all()]
    return Response(data)

@api_view(['POST'])
def select_variables(request):
    selections = request.data.get('selections')  # Get the selections (use request.data for DRF)
    page_number = request.data.get('page')  # Default to page 1
    print(selections, page_number)

    properties_sname = set(selections['dd-sname'])
    properties_lname = set(selections['dd-lname'])
    properties_tave = set(selections['dd-tave'])
    properties_ens = set(selections['dd-ens'])
    results = VariableInterface.filter_by_property_keys(
        [properties_sname, properties_lname, properties_tave, properties_ens]
        )

    print(f'Before ordering and distinction we have {results.count()} variables')
    # We need to order the results, so we're doing it this way:
    results = (results
                .order_by('key_properties', 'id')  # First order by property value, then by id
                )
    if page_number == 1:
        n = results.count()
        sdata = results.aggregate(
            nspatial=Count('spatial_domain', distinct=True),
            ntime=Count('time_domain', distinct=True)
            )
        summary = f'<p>Total Results {n}. Includes <ul>'
        summary += f"<li>{sdata['nspatial']} unique spatial domains, and </li>"
        summary += f"<li>{sdata['ntime']} unique time domain(s).</li></ul>"
        #not enough work for a template
    else:
        summary=''
        
    # Paginate results using vanilla django pagination, the DRF one didnt' work
    paginator = Paginator(results, 10)

    try:
        page_results = paginator.page(page_number)  # Get the specific page
    except PageNotAnInteger:
        # If page_number is not an integer, deliver the first page.
        page_results = paginator.page(1)
    except EmptyPage:
        # If page_number is out of range (e.g. 9999), deliver the last page of results.
        page_results = paginator.page(paginator.num_pages)

    serializer = VariableSerializer(page_results, many=True)
    
    print(serializer.data)

    html = render_to_string('gui/variable.html', {'results': serializer.data})

    print('page',page_results.number,' of ', paginator.num_pages)
    #Return the rendered HTML along with pagination info
    
    myresponse = {
        
        'html': html,
        'total': paginator.count,  # Total count of results
        'page': page_results.number,  # Current page
        'total_pages': paginator.num_pages  # Total number of pages
    }
    if summary:
        myresponse['summary'] = summary

    return JsonResponse(myresponse)


  





   