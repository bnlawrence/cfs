import os

from django import template
from django.db import transaction
from django.db.models import Q, Count,OuterRef, Subquery

from cfs.models import (Cell_MethodSet, Cell_Method, Collection, CollectionType, 
                            Domain, File, FileType, Location, Manifest,  
                            VariableProperty, VariablePropertyKeys,
                            Relationship, Tag, TimeDomain, Variable)

from time import time


import logging
logger = logging.getLogger(__name__)

register = template.Library()
@register.filter


def get_obj_field(obj, key):
    return obj[key]
#FIXME: When and how is the filter used, it's come from the old db.py, but it's a GOB thing.

class GenericInterface:
    # This is the replacement for GenericHandler, I will migrate the subclassses
    # over time as I can.
    """
    A reusable mixin that provides create, retrieve, and get_or_create
    methods for the overall interface to the various model classes. 
    """
    model = None
    @classmethod
    def all(cls):
        return cls.model.objects.all()
    
    @classmethod
    def count(cls):
        return cls.model.objects.count()

    @classmethod
    def create(cls, **kwargs):
        """ Create a new instance of the model. """
        instance = cls.model(**kwargs)
        try:
            instance.save()
        except Exception as e:
            if str(e).startswith('UNIQUE constraint'):
                raise ValueError(f'{cls.model.__name__} instance with unique constraint already exists.')
        return instance

    @classmethod
    def retrieve(cls, **kwargs):
        """ Retrieve a single instance. """
        results = cls.model.objects.filter(**kwargs)
        if results.count() == 0:
            raise ValueError(f'No such {cls.model} instance with {kwargs}')
        elif results.count() > 1:
            raise ValueError(f'Unable to match a single {cls.model.__name__}.')
        return results.first()
    
    @classmethod
    def retrieve_all(cls, **kwargs):
        """
        Retrieve all existing intnaces based on keywords.
        """
        try:
            return cls.model.objects.filter(**kwargs).all()
        except cls.model.DoesNotExist:
            raise ValueError(f'No {cls.model.__name__} instance matching {kwargs}')


    @classmethod
    def get_or_create(cls, **kwargs):
        """ Get an existing instance or create a new one. """
        return cls.model.objects.get_or_create(**kwargs)

    @classmethod
    def delete(cls, instance):
        """ Delete the given instance. """
        instance.delete()

    @classmethod
    def queryset_delete(cls, queryset):
        """ 
        Delete a specific set of instances returned from some query against this model.
        """
        if queryset.model != cls.model:
            raise PermissionError(
                f'Attempt to delete a queryset of{queryset.model} with {cls.model.__name__}.queryset_delete')
        for thing in queryset:
            thing.delete()


class GenericHandler:
    """
    A reusable mixin that provides create, retrieve, and get_or_create
    methods for the overall interface to the various model classes. 
    """
    def __init__(self, model_class):
        self.model = model_class

    def all(self):
        return self.model.objects.all()
    
    def count(self):
        return self.model.objects.count()

    def create(self, **kwargs):
        """
        Create a new instance of the model.
        """
        instance = self.model(**kwargs)
        try:
            instance.save()
        except Exception as e:
            if str(e).startswith('UNIQUE constraint'):
                raise ValueError(f'{self.model.__name__} instance with unique constraint already exists.')
        return instance
    
    def retrieve(self, **kwargs):
        """ 
        Retrieve a single instance, raise an error if multiple items match the query,
        return None if none found.
        """
        results = self.retrieve_all(**kwargs)
        nresults = len(results)
        if nresults == 0:
            if self.model.__name__=='File':
                raise FileNotFoundError
            else:
                return None
        elif nresults > 1: 
            raise ValueError('Unable to match a single {self.model.__name__} - got {nresults} instances')
        return results[0]

    def get_or_create(self, **kwargs):
        """
        Class method to get an existing instance or create a new one if it doesn't exist.
        """
        instance, created = self.model.objects.get_or_create(**kwargs)
        return instance, created
    
    def queryset_delete(self, queryset):
        """ 
        Delete a specific set of instances returned from some query against this model.
        """
        if queryset.model != self.model:
            raise PermissionError(
                f'Attempt to delete a queryset of{queryset.model} with {self.model.__name__}.queryset_delete')
        for thing in queryset:
            thing.delete()

    def retrieve_all(self, **kwargs):
        """
        Retrieve all existing intnaces based on keywords.
        """
        try:
            return self.model.objects.filter(**kwargs).all()
        except self.model.DoesNotExist:
            raise ValueError(f'No {self.model.__name__} instance matching {kwargs}')

    


class CellMethodsInterface(GenericHandler):

    def __init__(self):
        super().__init__(Cell_Method)
    
    def set_get_or_create(self, methods):
        """ 
        Handles getting and creating cell methods as a set.
        Expects each element of the method set to be 
        : methods : list of (axis, method) pairs
        : returns : An instance of CellMethodSet
        """
        methods = [self.get_or_create_by_pair(m) for m in methods]
        method_set = Cell_MethodSet.get_or_create_from_methods(methods)
        return method_set

    def get_or_create_by_pair(self, method):
        """ 
        The standard cell method creation interface takes a pair 
        of key words, this interface just takes a method described
        as a tuple pair.
        : method : tuple describing a cell method (axis, method)
        : returns : A CellMethod instance
        """    
        kw = {k:v for k,v in zip(['axis','method'],list(method))}
        cm, created = self.get_or_create(**kw)
        return cm
    
    def retrieve(self,method):
        """ Retrieve a cell method supplied as a tuple"""
        kw = {k:v for k,v in zip(['axis','method'],list(method))}
        return super().retrieve(**kw)
    
    

class CollectionInterface(GenericInterface):
    model = Collection

    def create(self, **kw):
        """ Convenience Interface to handle proxied keywords """

        proxied = kw.pop('_proxied',{})
        understood = vars(Collection)
        kwargs = {'_proxied':proxied}
        if 'description' not in kw:
            kwargs['description'] = '(none)'
        for k,v in kw.items():
            if k in understood:
                kwargs[k]=v
            else:
                kwargs['_proxied'][k]=v
        result = super().create(**kwargs)
        result.save()
        return result

    @classmethod
    def add_variable(cls,collection, variable):
        """
        Add a variable to the existing collection.
        """
        collection.variables.add(variable)

    @classmethod
    def add_variables(cls,collection, variable_set):
        """
        Add a queryset of variables to the existing collection
        in one transaction.
        """
        with transaction.atomic():
            collection.variables.add(*variable_set)

    @classmethod
    def delete(cls, collection, force=False):
        """
        Delete any related variables.
        """
        if isinstance(collection, str):
            collection = cls.retrieve(name=collection)
        elif isinstance(collection,int):
            collection = cls.retrieve(id=collection)
        collection.do_empty(force)
        super().delete(collection)

    @classmethod
    def retrieve_all(cls, name_contains=None, description_contains=None, 
                 contains=None, tagname=None, facet=None, **kw):
        """
        Retrieve collections based on various filters.
        """
        if [name_contains, description_contains, contains, tagname, facet].count(None) <= 3:
            raise ValueError("Cannot search on more than one of name, description, tag, facet")
        #FIXME much of this is redundant

        results = Collection.objects
        
        if name_contains:
            results=results.filter(name__contains=name_contains)
        if description_contains:
            results=results.filter(description__contains=description_contains)
        if contains:
            results = results.filter(
                Q(name__contains=contains) | Q(description__contains=contains)
            )
        if tagname:
            results = results.filter(tags__name=tagname)
        if facet:
            key, value = facet
            query = {f'_proxied__{key}': value}
            results=results.filter(**query)

        if kw:
            results = results.filter(**kw)


        return results.all()
    
    @classmethod
    def delete_subdirs(cls, collection, self_destruct=False):
        """ 
        This deletes all the subdirectories of a specific collection,
        that is, all the collections which hold variables which are 
        described by the files of the parent collection. 
        Do not use the self_destruct argument, that's for internal use.
        """
        removed = 0
        child_relationships = collection.related_to.filter(predicate="parent_of")
        if child_relationships:
            for relation in child_relationships:
                child = relation.related_to
                removed += cls.delete_subdirs(child, self_destruct=True)
        if self_destruct:
            collection.delete()
            return removed+1
        else:
            return removed

    @staticmethod
    def findall_with_variable(variable):
        """Find all collections with a given variable"""
        coldict = {}
        for file in variable.in_files.all():
            for collection in Collection.objects.filter(files=file).all():
                if collection not in coldict:
                    coldict[collection] = 1
                else:
                    coldict[collection] += 1
        return coldict

    @classmethod
    def add_type(cls, collection, key, value):
        """ 
        Add a particular collection property to the collection.
        These key,value pairs should only be used when they 
        are likely to be imoportant terms for searching across
        collections
        : collection : name or instance
        : key : key to add 
        : value : key to use 
        """
        if not isinstance(collection, Collection):
            collection=cls.retrieve(name=collection)
        if key == '_type':
            try:
                value = FileType.get_value(value)
            except KeyError:
                raise ValueError('The special collection key _type must have value which is a FileType key')
        term, created = CollectionType.objects.get_or_create(key=key,value=value)
        collection.type.add(term)
        
    @classmethod
    def update_description(cls, id, description):
        c = Collection.objects.get(id=id)
        c.description = description
        c.save()
        return c

        

class DomainInterface(GenericHandler):
        
    def __init__(self):
        super().__init__(Domain)


    def get_or_create(self,kw):
        td, created = super().get_or_create(**kw)
        return td
    
    def retrieve(self, **properties):
        """ 
        Retreive domain by properties
        """
        d = super().retrieve(**properties)
        if len(d) > 1:
            raise ValueError('Multiple domains match your query')
        return d[0]
    
    def retrieve_by_name(self, name):
        """ 
        Retrieve spatial domain by name
        """
        try:
            return super().retrieve(name=name)
        except:
            return None

    def all(self):
        """ 
        Get all the domains
        """
        return Domain.objects.all()


class FileInterface(GenericHandler):
    
    def __init__(self):
        super().__init__(File)
        self.location=LocationInterface()

    def _doloc(self, properties):
        location = properties.pop('location',None)
        if location is None:
            raise ValueError('Cannot create a file without putting it in a location')
        if not isinstance(location,Location):
            location, created = self.location.get_or_create(name=location)
        return properties, location


    def findall_with_variable(self, variable):
        """
        Find all files with a given variable
        """
        return variable.in_files.all()
    
    def findall_by_type(self,type):
        """
        Find all files of a given type 
        """
        if type not in FileType:
            raise ValueError(f'Cannot query files by invalid type {type}')
        return File.objects.filter(type=type).all()
    
    def findall_from_variableset(self, variables):
        """ 
        Find all files from a given set of variables
        """
        files = File.objects.filter(variable__in=variables)
        return files
    
    def create(self, props):

        properties = props.copy()
        properties, location = self._doloc(properties)
        file = super().create(**properties)
        file.locations.add(location)
        location.volume += file.size
        location.save()
        return file
    
    def get_or_create(self, props):
        properties = props.copy()
        properties, location = self._doloc(properties)
        file, created = super().get_or_create(**properties)
        if not created and location in file.locations.all():
            logger.warning('Adding an existing file to a location where it already exists')
        else:
            file.locations.add(location)
            location.volume += file.size
            location.save()
        return file, created
    
    def in_location(self, location_name):

        return File.objects.filter(locations__name=location_name).all()
        
    
class LocationInterface(GenericHandler):
    def __init__(self):
        super().__init__(Location)

    def create(self, name):
        return super().create(name=name)

    def retrieve(self, location_name):
        return super().retrieve(name=location_name)

    def find_all(self):
        return Location.objects.all()
    
    def delete(self, name):
        instance = self.retrieve(name)
        instance.delete()

    def get_or_create(self,name):
        return super().get_or_create(name=name)


class ManifestInterface(GenericHandler):
    def __init__(self):
        super().__init__(Manifest)
        self.location = LocationInterface()

    def add(self, properties):
        """
        Add a CFA manifest
        This should always be unique.
        Can be deleted by deleting the parent file.
        """
        if 'cells' in properties:
            cells = properties.pop('cells')
            logger.info(f'Removed cells {cells} from manifest, why did we want them?')
        # parse fragment file dictionaries into proper files
        fragments = properties.pop('fragments')
        # pull out bases if they exist
        for k,f in fragments.items():
            base = f.pop('base',None)
            if base is not None:
                loc, created = self.location.get_or_create(base)
                f['location']=loc
        properties.pop('_bounds_ncvar')  #not intended for the database
        with transaction.atomic():
            # we do this directly for efficiency, and to bypass
            # the interface file check on size, which we may not know
            # for fragment files.
            m = Manifest.objects.create(**properties)
            # extract locations and prepare file objects 
            locations = [f.pop('location',None) for f in fragments.values()]

            # Check for existing files or prepare new ones for creation
            existing_files = []
            file_objects = []
            for f in fragments.values():
                existing_file = File.objects.filter(**f).first()
                if existing_file:
                    existing_files.append(existing_file)
                else:
                    file_objects.append(File(**f))
            
            # Bulk create new files
            if file_objects:
                File.objects.bulk_create(file_objects)

            # Combine both newly created and existing files
            all_files = file_objects + existing_files

            # now we can add the locations
            for loc, f in zip(locations,all_files):
                f.locations.add(loc)
            m.fragments.add(*all_files)
            # no saves needed, all done by the transaction
        return m

    def get_or_create(self):
        """ We do not want to allow access to the superclass method"""
        raise NotImplemented
    


class RelationshipInterface(GenericInterface):

    model = Relationship   
    
    @classmethod
    def add_single(cls, collection_one, collection_two, relationship):
        """
        Add a oneway <relationship> between <collection_one> and <collection_two>.
        e.g. add_relationship('julius','betrayed_by','brutus')
        brutus is not betrayed_by julius. 
        : collection_one : subject collection name
        : collection_two : object collection name
        : relationship : predicate
        : returns : relationship instance
        """
        #def add_relationship(self, collection_one, collection_two, relationship):
        c1 = CollectionInterface.retrieve(name=collection_one)
        c2 = CollectionInterface.retrieve(name=collection_two)
        rel = Relationship(subject=c1, predicate=relationship, related_to=c2)
        rel.save()
        return rel

    @classmethod
    def delete_by_name(cls, relationshipname):
        """
        Delete a relationship, from wherever it is used
        """
        cls.model.delete(name=relationshipname)

    @classmethod
    def add_double(
        cls, collection_one, collection_two, relationship_12, relationship_21):
        """
        Add a pair of relationships between <collection_one>  and <collection_two> such that
        collection_one has relationship_12 to collection_two and
        collection_two is a relationship_21 to collection_one.
        e.g. add_relationship('father_x','son_y','parent_of','child_of')
        (It is possible to add a one way relationship by passing relationship_21=None)
        """
        #def add_relationships(
        cls.add_single(collection_one, collection_two, relationship_12)
        if relationship_21 is not None and collection_one != collection_two:
            cls.add_single(collection_two, collection_one, relationship_21)

    @classmethod
    def retrieve(cls, collection, outbound=True, relationship=None):
        """
        Find all relationships from or to a  <collection>, optionally
        which have <relationship> as the predicate.
        """
        #def retrieve_relationships(self, collection, relationship=None):
        c = CollectionInterface.retrieve(name=collection)
        if relationship:
            if outbound:
                r = c.related_to.objects 
            else:
                r = c.subject.objects
            return r.filter(predicate=relationship).all()
        else:
            if outbound:
                return c.related_to.all()
            else:
                return c.subject.all()
    
    @classmethod
    def get_triples(cls, collection):
        """ Get all triples for a particular collection instance"""
        print(collection.related_to.all())
        outbound = [(r.predicate, r.related_to.name) for r in collection.related_to.all()]
        inbound = [(r.predicate, r.subject.name) for r in collection.subject.all()]
        return outbound, inbound
    
    @classmethod
    def get_predicates(cls):
        predicates = cls.model.objects.values_list('predicate', flat=True).distinct()
        return predicates


class TagInterface(GenericHandler):

    def __init__(self):
        super().__init__(Tag)
        self.collection=CollectionInterface()

    def create(self,name):
        super().create(name=name)

    def add_to_collection(self, collection_name, tagname):
        """
        Associate a tag with a collection
        """
        tag, s = self.get_or_create(name=tagname)
        c = self.collection.retrieve(name=collection_name)
        c.tags.add(tag)

    def remove_from_collection(self, collection_name, tagname):
        """
        Remove a tag from a collection
        """
        c = self.collection.retrieve(name=collection_name)
        tag = Tag(name=tagname)
        c.tags.remove(tag)


class TimeInterface(GenericHandler):
    def __init__(self):
        super().__init__(TimeDomain)
    def get_or_create(self,kw):
        if kw == {}:
            td = None
        else: 
             td, created = super().get_or_create(**kw)
        return td
   

class VariableProperyInterface:
    """ 
    Used only in the GUI front end to provide choice sets of variables
    """
    @classmethod
    def filter_properties(cls, keylist=[], collection_ids=[], location_ids=[]):
        """ 
        Generate a subset of properties depending on whether or
        not the properties belong to variables in a colletion, and
        whether or not they are stored in location.
        """
        print(keylist, collection_ids, location_ids)
        try:
            result = VariableProperty.objects.all()
            if collection_ids:
                collections = Collection.objects.filter(id__in=collection_ids)
                result = result.filter(variable__contained_in__in=collections).distinct()
            if keylist:
                result = result.filter(key__in=keylist)
            if location_ids:
                print('Not implemented in variablepropertyinterface')  #FIXME
            return result.all()
        except Exception as err:
            print(str(err))
            return VariableProperty.objects.all()
       


class VariableInterface(GenericHandler):
    def __init__(self):
        super().__init__(Variable)
        self.varprops = {v:k for k, v in VariablePropertyKeys.choices}
        self.xydomain=DomainInterface()
        self.tdomain=TimeInterface()
        self.cellm=CellMethodsInterface()
        self.file = FileInterface()

    def _construct_properties(self,varprops, ignore_proxy=False):
        """ 
        Used to parse a set of variable properties in words into appropriate
        model instances for inserting and querying the database
        """

        definition, extras = {'key_properties':[]},{}
        
        for key in varprops:
            if key in VariablePropertyKeys.labels:
                ekey = self.varprops[key]
                out_value, _ = VariableProperty.objects.get_or_create(
                                        key=ekey, value=varprops[key])
                definition['key_properties'].append(out_value)
            elif key == 'spatial_domain':
                definition[key]=self.xydomain.get_or_create(varprops[key])
            elif key == 'time_domain':
                definition[key]=self.tdomain.get_or_create(varprops[key])
            elif key == 'cell_methods':
                method_set = self.cellm.set_get_or_create(varprops[key])
                definition[key]=method_set
            elif key in ['in_file','in_manifest']:
                definition[key]=varprops[key]
            else:
                extras[key]=varprops[key]
        if not ignore_proxy:
            definition['_proxied']=extras
        for x in ['cell_methods','in_manifest']:
            if x not in definition:
                definition[x] = None
        return definition
    
    def add_to_collection(self, collection, variable):
        """
        Add variable to a collection
        """
        c = Collection.objects.get(name=collection)
        # django handles duplicates gracefully
        c.variables.add(variable)
        c.save()

    @classmethod
    def filter_by_property_keys(cls, list_of_keysets):
        """ 
        Return a queryset of variables which have been
        filtered by the key properties which lie in
        the list of keysets. Each keyset will be 
        a list of property ids.
        """
        results = Variable.objects.all()
        for value in list_of_keysets:
            if value:
                results = results.filter(key_properties__properties__id__in=value)
        return results
        

    def retrieve_by_keyvalue(self, key, value):
        """Retrieve single variable by arbitrary property"""
        return self.retrieve({key:value})

    def retrieve_by_queries(self, queries, from_collection=None):
        """Retrieve variable by a list of common query types, limit queries to collection
        if wished.
        Each element of the list is a key,value pair
        : key : Can be one of the following
                'id,' 'long_name', 'standard_name', 'temporal_resolution', 'nominal_resolution'
                'cell_method', 'spatial_domain', 'time_domain', 'in_file'
                It can also be any other arbitrary property which we might expect to find
                in the proxied properties. But querying on those will be very slow!
        : values : 'id','long_name','standard_name' are identity strings
                   'time_resolution' : an integer describing a temporal resolution interval
                   'nominal_resolution' : string describing a nominal spatial resolution
                   'cell_method' : a db cell method instance
                   'spatial_domain ' : a db (spatial) domain instance
                   'time_domain' :  a db time domain instance
                   'in_file': a db file instance
                   'in_manifest': a db manifest instance
        """
        if from_collection is None:
             base = Variable.objects
        else:
            if not isinstance(from_collection,Collection):
                from_collection = Collection.objects.get(name=from_collection)
            base = from_collection.variables
           
        proxied = []
        for key,value in queries:
            #print('Query step', key, value)
            if key == 'key_properties':
                base = base.filter(key_properties__properties__in=value)
            elif key in ["spatial_domain","temporal_domain"]:
                base = base.filter(**{key:value})
            elif key == 'in_file':
                base = base.filter(in_file=value)
            elif key == 'in_manifest':
                base = base.filter(in_manifest=value)
            elif key == "cell_methods":
                base = base.filter(cell_methods__methods=value)
            elif key == 'temporal_resolution':
                base = base.filter(time_domain__interval=value)
            elif key == 'nominal_resolution':
                base = base.filter(spatial_domain__nominal_resolution=value)
            else:
                #it's a proxied field, ideally we'd do this, but we can't, doesn't work with sqllite
                #base = base.filter(_proxied__contains={key:value})
                proxied.append((key,value))
                
        if proxied:
            print(f'Querying {[k for k,v in proxied]} will be horrendously slow. Try to avoid')
            # If it's a regular need we should move this key value pair to a term.
            # At this point we have no good choices for speed
            candidates = base.all()
            for k,v in proxied:
                results = []
                for c in candidates:
                    if k in c:
                        if c[k]==v:
                            results.append(c)
                candidates = results
            return results
        else:
            return base.all()
    

    def retrieve_in_collection(self, collection_name):
        C = Collection.objects.get(name=collection_name)
        return C.variables.all()
    
    def retrieve_all_collections(self, variable):
        return variable.contained_in.all()

    
    def get_or_create(self, varprops, unique=True):
        """
        If there is a variable corresponding to varprops with the same full set of properties, 
        return it, otherwise create it. Varprops should be a dictionary which includes at least 
        an identiy and an atomic origin in the key properties.
        In general we should not doing a retrieve when we want to create, hence the default
        value of unique.
        """
        try:
            props = self._construct_properties(varprops)
        except:
            logger.debug('Crash coming')
            logger.debug(varprops)
            raise
        kp = [p.key for p in props['key_properties']]
        if 'ID' not in kp:
            raise ValueError('Variable definitions must include identity')
        if len(props)!= 7:
            raise ValueError('Insufficient properties to create a variable')
        args = [props[k] for k in ['_proxied','key_properties','spatial_domain',
                                   'time_domain','cell_methods','in_file','in_manifest']]
        var, created = Variable.get_or_create_unique_instance(*args)
        if unique and not created:
            raise PermissionError('Attempt to re-create existing variable {var}')
        return var
    
    def all(self):
        return Variable.objects.all()

    def retrieve_by_key(self, key, value):
        return self.retrieve_by_queries([(key,value),])


    def retrieve_by_properties(self, properties, from_collection=None, unique=False):
        """
        Retrieve variable by arbitrary set of properties expressed as a dictionary.
        (Basically an interface to retrieve_by_queries.)
        """
        queries = []

        for k in ['in_file','cell_methods']:
            if k in properties:
                v = properties.pop(k)
                if k == 'in_file':
                    if not isinstance(v, File):
                        v = self.file.retrieve(**v)
                    queries.append((k,v))
                elif k == 'cell_methods':
                    for x in v:
                        cm = self.cellm.retrieve(x)
                        queries.append(('cell_methods', cm))
        properties = self._construct_properties(properties)
        for present in ['_proxied','key_properties','cell_methods']:
            if not properties[present]:
                properties.pop(present)
        for k, v in properties.items():
            queries.append((k,v))
        results = self.retrieve_by_queries(queries, from_collection=from_collection)
        if unique:
            if len(results) > 1:
                raise ValueError('Query retrieved multiple variables ({c}) - but uniqueness was requested')
        return results
        

class CollectionDB:


    def __init__(self):
        
        self.collection=CollectionInterface()
        self.variable=VariableInterface()
        self.file=FileInterface()
        self.manifest=ManifestInterface()
        self.cell=CellMethodsInterface()
        self.xydomain=DomainInterface()
        self.tdomain=TimeInterface()
        self.tag=TagInterface()
        self.location=LocationInterface()
        self.relationship=RelationshipInterface()


    def clone(self, instance):
        instance.pk = None
        instance.save()
        return instance

    def upload_file_to_collection(self, location, collection, f, lazy=0, update=True):
        """
        Convenience API to upload_files_to_collection, simply wraps <f> into a list
        and calls upload_files_to_collection. See that function for explanation of
        arguments.
        """
        return self.upload_files_to_collection(location, collection, 
                                               [f], lazy, update=True, progress=False )[0]


    def upload_file_to_collection(self, location_name, collection_name, filedata):
        """
        Upload a file and set of variables described by <filedata> to the database.
        This method should only be used for the first time these data are exposed
        to the database. Use "update_file_location" to add aditional copies or
        to move the data between locations known to the database.  

        The first thing we do is check that the variables are not yet known to
        the database. In the case of quarks, this will be ok as they will be subsets
        of the atomic dataset already known to the database, so will appear
        unique _for upload purposes_.

        : location_name : the location where the file is stored
        : collection_name : the primary collection for these data.
        : filedata : A dictionary with the following structure
        {'properties':{Dictionary of file properties},
         'variables': List of dictionaries of variable properties.
         'manifests': If the file is a CFA file, this will be as set of 
                      manifests which describe the fragment 
                      locations of variables which share the same fragment 
                      files.}

        This filedata dictionary should be generated by the parsefields_to_dict
        routine which utilises CF python to build the necessary dictionary
        content, including, for CFA files, the internal linkage between 
        variables and manifests. (This is accomplished by the use of UUIDs 
        which are constructed only for use in this method, and discarded 
        during this upload to the databaes. 

        """

        try:
            c = self.collection.retrieve(name=collection_name)
            loc = self.location.retrieve(location_name)
        except Collection.DoesNotExist:
            raise ValueError("Collection not yet available in database")
        except Location.DoesNotExist:
            raise ValueError("Location not yet available in database")
        
        # manually controlling rollback
        step = 0
        created = []
        #pathology=[]
        #manology=[]
        try:
            file = self.file.create(filedata['properties'])
            created.append(file)
            manifests={}
            step = 1
            manidata = filedata.pop('manifests',{})
            for key,value in manidata.items():
                #manology.append(value.copy())
                manifest = value.pop('manikey')
                value['cfa_file'] = file
                step = 2
                manifests[key] = self.manifest.add(value)
                created.append(manifests[key])
            step = 3
            vars = []
            for v in filedata['variables']:
                #pathology.append(v.copy())
                key = v.pop('manikey',None)
                if key is not None:
                    v['in_manifest'] = manifests[key]
                v['in_file'] = file
                t0 = time()
                try:
                    var = self.variable.get_or_create(v)
                except:
                    #import json
                    #with open('crash.json','w') as f:
                    #    for m in manology:
                    #        if m['bounds'] is not None:
                    #            m['bounds']=m['bounds'].tolist()
                    #    p = {'manifests':manology,'variables':pathology}
                    #    json.dump(p,f)
                    #    print('dumped')
                    raise
                        
                t1 = time()-t0
                logger.info(f'Created ({t1:.2f}s) {var}')
                created.append(var)
                vars.append(var)
            step = 4
            for v in vars:
                self.variable.add_to_collection(c.name,v)
            return vars
        
        except ExceptionGroup as e:
            # roll back
            items = len(created)
            for c in reversed(created):
                c.delete()
            logger.fatal(f'Failure encountered at step {step}, {items} items deleted. Problem was:')
            match step:
                case 0:
                    logger.fatal(filedata['properties'])
                case 1:
                    logger.fatal(manidata)
                case 2:
                    logger.fatal(manifests[key])
                case 3:
                    logger.fatal(v)
                case 4:
                    logger.fatal(f'Problem with adding to collection {c}')

            msg = str(e)+f' (Failed after step {step}, {items} items deleted)'
            raise Exception(msg)
