from django.shortcuts import render
from django.http import HttpResponse

from rest_framework.response import Response
from rest_framework.decorators import api_view
from cfs.models import VariableProperty, VariablePropertyKeys, Variable, Cell_Method

# Create your views here.

def index(request):
    """ Core index view """
    return HttpResponse('Hello world')

def view(request):
    return render(request, 'gui/view.html')

def oldview(request):
    return render(request,'gui/view0.html')

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





   