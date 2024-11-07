# views/api.py
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.http import JsonResponse
from django.shortcuts import redirect
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.urls import reverse
from django.template.loader import render_to_string
from gui.forms import RelationshipForm, DateRangeForm
from django.db.models import Count
from django.core.exceptions import ObjectDoesNotExist
from cfs.models import VariableProperty, Cell_Method, Collection, Variable, Location
from cfs.db.interface import (VariableProperyInterface, VariableInterface,
                              CollectionInterface, RelationshipInterface,
                              TagInterface, VariablePropertyKeys)
from gui.serializers import VariableSerializer
from .helpers import _filterview, _summary

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
    print(data)
    return Response(data)

@api_view(['POST'])
def select_variables(request):
    page_number = request.data.get('page')  # Default to page 1
    
    selections = request.data.get('selections') 
    results = _filterview(selections)

    print(f'Before ordering and distinction we have {results.count()} variables')
    # We need to order the results, so we're doing it this way:
    results = (results
                .order_by('key_properties', 'id')  # First order by property value, then by id
                )
    if page_number == 1:
        n = results.count()
        sdata = results.aggregate(
            nspatial=Count('spatial_domain', distinct=True),
            ntime=Count('time_domain', distinct=True),
            )
        # putting this in the aggregate didn't quite work.
        nvariants = results.filter(
                key_properties__properties__key='VL'
            ).values('key_properties__properties__value').distinct().count()

        summary = _summary(sdata, n, nvariants)
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

    html = render_to_string('gui/variable.html', {'results': serializer.data})

    print('page',page_results.number,' of ', paginator.num_pages)
    #Return the rendered HTML along with pagination info
    
    myresponse = {
        
        'html': html,
        'total': paginator.count,  # Total count of results
        'page': page_results.number,  # Current page
        'total_pages': paginator.num_pages,  # Total number of pages
        'header': "Selected Variables"
    }
    if summary:
        myresponse['summary'] = summary

    return JsonResponse(myresponse)



@api_view(['POST'])
def make_quarks(request):

    selections = request.data.pop('selections')
    results = _filterview(selections)
    form = DateRangeForm(request.data)
    if form.is_valid():
        # Process data
        quark_name = form.cleaned_data['quark_name']
        start_day = form.cleaned_data['start_day']
        start_month = form.cleaned_data['start_month']
        start_year = form.cleaned_data['start_year']
        end_day = form.cleaned_data['end_day']
        end_month = form.cleaned_data['end_month']
        end_year = form.cleaned_data['end_year']

        interface=CollectionInterface()
        try:
            interface.make_quarks(quark_name, (start_day,start_month,start_year),
                             (end_day,end_month,end_year), results)
        except Exception as e:
            print(str(e))
            return JsonResponse({'message': 'Invalid data', 'errors': str(e)}, status=400)
        return JsonResponse({'message': 'Quark collection created successfully!'}, status=200)
    else:
        return JsonResponse({'message': 'Invalid data', 'errors': form.errors}, status=400)
  

@api_view(['POST'])
def add_to_collection(request):
    
    collection_name = request.data.get('collection_name')
    selections = request.data.get('selections') 
    results = _filterview(selections)
    interface = CollectionInterface()

    try:
        collection = interface.retrieve(name=collection_name)
        created=False
    except ValueError:
        collection = interface.create(name=collection_name)
        created=True
    try:
        interface.add_variables(collection, results)
        count = results.count()
        if created:
            msg = f'{count} variables added to new collection {collection_name}.'
        else:
            msg = f'{count} variables added to collection {collection_name} (which now has {collection.variables.count()} variables).'
    except Exception as e:
        msg = str(e)
    return JsonResponse({"message":msg})


@api_view(['POST'])
def update_collection_description(request):
    id = request.data.get('id')
    description=request.data.get('text')

    collection = CollectionInterface.update_description(id, description)
    return JsonResponse({'msg':'Updated {{collection}}'})

@api_view(['DELETE'])
def delete_collection(request, id):
    try:
        CollectionInterface.delete(id)
        return JsonResponse({'success': True, 'msg': 'Collection deleted successfully.'}, status=200)
    except ObjectDoesNotExist:
        return JsonResponse({'success': False, 'msg': 'Collection does not exist'}, status=404)
    except PermissionError as e:
        return JsonResponse({'success': False, 'msg': str(e)}, status=403)
    except Exception as e:
         return JsonResponse({'success': False, 'msg': str(e)}, status=500)
  

@api_view(['POST'])
def get_collection(request):
    page_number = request.data.get('page')  # Default to page 1
    id = request.data.get('id')
    collection = CollectionInterface.retrieve(id=id)
    results = collection.variables.all()

    print(f'Before ordering and distinction we have {results.count()} variables')
    # We need to order the results, so we're doing it this way:
    results = (results
                .order_by('key_properties', 'id')  # First order by property value, then by id
                )
    if page_number == 1:
        n = results.count()
        sdata = results.aggregate(
            nspatial=Count('spatial_domain', distinct=True),
            ntime=Count('time_domain', distinct=True),
            )
        # putting this in the aggregate didn't quite work.
        nvariants = results.filter(
                key_properties__properties__key='VL'
            ).values('key_properties__properties__value').distinct().count()

        summary = _summary(sdata,n,nvariants)
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

    html = render_to_string('gui/variable.html', {'results': serializer.data})

    print('page',page_results.number,' of ', paginator.num_pages)
    #Return the rendered HTML along with pagination info
    
    myresponse = {
        
        'html': html,
        'total': paginator.count,  # Total count of results
        'page': page_results.number,  # Current page
        'total_pages': paginator.num_pages,  # Total number of pages
        'header': f'Collection "{collection.name}" Variables'
    }
    if summary:
        myresponse['summary'] = summary
        outbound, inbound = RelationshipInterface.get_triples(collection)
        myresponse['related'] = render_to_string('gui/related.html',
                                                 {'inbound':inbound, 'outbound':outbound})
        form = RelationshipForm(initial={'known_collection': collection.name})
        predicate_list = RelationshipInterface.get_predicates()
        target_list = [(c.id,c.name) for c in CollectionInterface.all()]
        target_list = [c for c in target_list if c!=(collection.id,collection.name)]
        form.fields['related_collection'].widget.choices = target_list
        form.fields['relationship_from'].widget.choices = [(p, p) for p in predicate_list]
        form.fields['relationship_to'].widget.choices =  [(p, p) for p in predicate_list]
        myresponse['relform'] = render_to_string('gui/relform.html', {'form':form})
      

    return JsonResponse(myresponse)

@api_view(['POST'])
def new_related(request):
    form = RelationshipForm(request.POST)
    if form.is_valid():
        known_collection = form.cleaned_data['known_collection']
        related_collection = form.cleaned_data['related_collection']
        relationship_from = form.cleaned_data['relationship_from']
        relationship_to = form.cleaned_data['relationship_to']
        
        subject = CollectionInterface.retrieve(name=known_collection)
        object = CollectionInterface.retrieve(id=related_collection)

        RelationshipInterface.add_single(subject.name, object.name, relationship_to)

        if relationship_from: 
            RelationshipInterface.add_single(object.name, subject.name, relationship_from)
        
    else:
        print(form.errors)
    
    return redirect(reverse('collections'))

@api_view(['POST'])
def update_tags(request, collection_id):
    
    if request.method == 'POST':
        # Handle adding new/existing tags via AJAX or standard form submission
        tag_names = request.POST.getlist('tags[]')  # If using AJAX with TomSelect
        print(tag_names)
        TagInterface.set_collection_tags(collection_id, tag_names)
        return redirect(reverse('collections'))
    else:
        raise ValueError('Only receive posts')








   