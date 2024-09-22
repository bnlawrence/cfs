import os

from django import template
from django.db import transaction
from django.db.models import Q, Count,OuterRef, Subquery

from core.db.models import (Cell_MethodSet, Cell_Method, Collection, CollectionType, 
                            Domain, File, FileType, Location, Manifest,  
                            VariableProperty, VariablePropertyKeys,
                            Relationship, Tag, TimeDomain, Variable)

from tqdm import tqdm

import logging
logger = logging.getLogger(__name__)

register = template.Library()
@register.filter


def get_obj_field(obj, key):
    return obj[key]
#FIXME: When and how is the filter used, it's come from the old db.py, but it's a GOB thing.


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

    def retrieve_all(self, **kwargs):
        """
        Retrieve all existing intnaces based on keywords.
        """
        try:
            return self.model.objects.filter(**kwargs).all()
        except self.model.DoesNotExist:
            raise ValueError(f'No {self.model.__name__} instance matching {kwargs}')
    
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

class CollectionInterface(GenericHandler):
    
    def __init__(self):
        super().__init__(Collection)

    def create(self,name, description='(none)', kw={}):
        """ Convenience Interface """
        
        kwargs = {'name':name, 'description':description,'_proxied':kw.pop('_proxied',{})}
        understood = vars(Collection)
        for k,v in kw.items():
            if k in understood:
                kwargs[k]=v
            else:
                kwargs['_proxied'][k]=[v]
       
        return super().create(**kwargs)

    
    def add_variable(self, variable):
        """
        Add a variable to the existing collection.
        """
        self.model.variables.add(variable)

    def delete(self, collection, force=False, unique_only=True):
        """
        Delete any related variables.
        """
        collection.do_empty(force, unique_only)
        collection.delete()

    
    def retrieve(self, name_contains=None, description_contains=None, 
                 contains=None, tagname=None, facet=None):
        """
        Retrieve collections based on various filters.
        """
        if [name_contains, description_contains, contains, tagname, facet].count(None) <= 3:
            raise ValueError("Cannot search on more than one of name, description, tag, facet")
        #FIXME much of this is redundant
        
        if name_contains:
            return Collection.objects.filter(name__contains=name_contains)
        if description_contains:
            return Collection.objects.filter(description__contains=description_contains)
        if contains:
            return Collection.objects.filter(
                Q(name__contains=contains) | Q(description__contains=contains)
            )
        if tagname:
            return Collection.objects.filter(tags__name=tagname)
        if facet:
            key, value = facet
            query = {f'_proxied__{key}': value}
            return Collection.objects.filter(**query)

        return Collection.objects.all()
    
        
    def collection_delete_subdirs(self, collection, self_destruct=False):
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
                removed += self.collection_delete_subdirs(child, self_destruct=True)
        if self_destruct:
            if collection.n_files!=0:
                raise PermissionError('Sub directory {c} contains files. Cannot delete')
            collection.delete()
            return removed+1
        else:
            return removed

    def collection_find_all_with_variable(self, variable):
        """Find all collections with a given variable"""
        coldict = {}
        for file in variable.in_files.all():
            for collection in Collection.objects.filter(files=file).all():
                if collection not in coldict:
                    coldict[collection] = 1
                else:
                    coldict[collection] += 1
        return coldict

    def collection_generate(self, replacedb, col):
        """Function to help generate collections"""
        vars = self.retrieve_variables_in_collection(col)
        for v in vars:
            for memberid, replace in replacedb.items():
                files = v.in_files.distinct()
                newvar = self.clone(v)
                for file in files:
                    newfile = self.retrieve_or_make_file(
                        match=os.path.basename(
                            file.name.replace("cn134", replace[0]).replace(
                                "999", replace[1][1]
                            )
                        )
                    ).last()
                    newvar.in_files.remove(file)
                    newvar.in_files.add(newfile)
                    col.files.add(newfile)
                    newfile.save()

    def add_type(self, collection, key, value):
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
            collection=self.retrieve_by_name(collection)
        if key == '_type':
            try:
                value = FileType.get_value(value)
            except KeyError:
                raise ValueError('The special collection key _type must have value which is a FileType key')
        term, created = CollectionType.objects.get_or_create(key=key,value=value)
        collection.type.add(term)

    def retrieve_by_name(self, collection_name):
        """
        Retrieve a particular collection via it's name <collection_name>.
        """
        results = self.retrieve(name_contains=collection_name)
        if len(results) == 0:
            raise ValueError(f'Collection {collection_name} not found')
        else:
            # uniquness enforced by model 
            return results[0]

    def collections_retrieve(
        self,
        name_contains=None,
        description_contains=None,
        contains=None,
        tagname=None,
        facet=None,
    ):
        # retrieve_collections
        """
        Return a list of all collections as collection instances,
        optionally including those which contain:

        - the string <name_contains> somewhere in their name OR
        - <description_contains> somewhere in their description OR
        - the string <contains> is either in the name or the description OR
        - with specific tagname OR
        - the properties dictionary for the collection contains key with value - facet = (key,value)

        """
        if [name_contains, description_contains, contains, tagname, facet].count(
                None
        ) <= 3:
            raise ValueError(
                "Invalid request to <get_collections>, cannot search on more than one of name, description, tag, facet"
            )

        if name_contains:
            return Collection.objects.filter(name__contains=name_contains).all()
        elif description_contains:
            return Collection.objects.filter(description__contains=description_contains).all()
        elif contains:

            return Collection.objects.filter(
                Q(name__contains=contains) | Q(description__contains=contains).all()
            )
        elif tagname:
            return Collection.objects.filter(tags__name=tagname).all()
            # tag = Tag.objects.get(name=tagname)
            # return tag.in_collections
        elif facet:
            # FIXME: I am not sure what is going on with facets and properties right now.
            # So  I've gone back to using the proxied rather than properties option.
            # as of September 2024 to get unit tests going. I expect to be back.
            key, value = facet
            # return Collection.objects.filter(properties_key=key, properties_value=value)
            query = {f'_proxied__{key}': value}
            return Collection.objects.filter(**query).all()
        else:
            return Collection.objects.all()
        

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
        super().__init__(Location)

    def add(self, properties):
        """
        Add a CFA manifest
        This should always be unique.
        Can be deleted by deleting the parent file.
        """
        print(properties)
        if 'cells' in properties:
            cells = properties.pop('cells')
            logger.info(f'Removed cells {cells} from manifest, why did we want them?')
        # parse fragment file dictionaries into proper files
        fragments = properties.pop('fragments')
        properties.pop('_bounds_ncvar')  #not intended for the database
        with transaction.atomic():
            # we do this directly for efficiency, and to bypass
            # the interface file check on size, which we may not know
            # for fragment files.
            m = Manifest.objects.create(**properties)
            file_objects = [File(**f) for k,f in fragments.items()]
            File.objects.bulk_create(file_objects)
            m.fragments.add(*file_objects)
            # no saves needed, all done by the transaction

    def get_or_create(self):
        """ We do not want to allow access to the superclass method"""
        raise NotImplemented


class RelationshipInterface(GenericHandler):   
    def __init__(self):
        super().__init__(Relationship)
        self.collection=CollectionInterface()

    def add_single(self, collection_one, collection_two, relationship):
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
        c1 = self.collection.retrieve_by_name(collection_one)
        c2 = self.collection.retrieve_by_name(collection_two)

        rel = Relationship(subject=c1, predicate=relationship, related_to=c2)
        rel.save()
        return rel

    def delete_by_name(self, relationshipname):
        """
        Delete a tag, from wherever it is used
        """
        super().delete(name=relationshipname)

    def add_double(
        self, collection_one, collection_two, relationship_12, relationship_21):
        """
        Add a pair of relationships between <collection_one>  and <collection_two> such that
        collection_one has relationship_12 to collection_two and
        collection_two is a relationship_21 to collection_one.
        e.g. add_relationship('father_x','son_y','parent_of','child_of')
        (It is possible to add a one way relationship by passing relationship_21=None)
        """
        #def add_relationships(
        self.add_single(collection_one, collection_two, relationship_12)
        if relationship_21 is not None and collection_one != collection_two:
            self.add_single(collection_two, collection_one, relationship_21)

    
    def retrieve(self, collection, outbound=True, relationship=None):
        """
        Find all relationships from or to a  <collection>, optionally
        which have <relationship> as the predicate.
        """
        #def retrieve_relationships(self, collection, relationship=None):
        c = self.collection.retrieve_by_name(collection)
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
        c = self.collection.retrieve_by_name(collection_name)
        c.tags.add(tag)

    def remove_from_collection(self, collection_name, tagname):
        """
        Remove a tag from a collection
        """
        c = self.collection.retrieve_by_name(collection_name)
        tag = Tag(name=tagname)
        c.tags.remove(tag)


class TimeInterface(GenericHandler):
    def __init__(self):
        super().__init__(TimeDomain)
    def get_or_create(self,kw):
        td, created = super().get_or_create(**kw)
        return td
   

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
                out_value, create = VariableProperty.objects.get_or_create(
                                        key=ekey, value=varprops[key])
                definition['key_properties'].append(out_value)
            elif key == 'spatial_domain':
                definition[key]=self.xydomain.get_or_create(varprops[key])
            elif key == 'time_domain':
                definition[key]=self.tdomain.get_or_create(varprops[key])
            elif key == 'cell_methods':
                method_set = self.cellm.set_get_or_create(varprops[key])
                definition[key]=method_set
            elif key == 'in_file':
                definition[key]=varprops[key]
            else:
                extras[key]=varprops[key]
        if not ignore_proxy:
            definition['_proxied']=extras
        if 'cell_methods' not in definition:
            definition['cell_methods'] = None
        return definition
    
    def add_to_collection(self, collection, variable):
        """
        Add variable to a collection
        """
        c = Collection.objects.get(name=collection)
        # django handles duplicates gracefully
        c.variables.add(variable)
        c.save()

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
                base = base.filter(key_properties=value)
            elif key in ["spatial_domain","temporal_domain"]:
                base = base.filter(**{key:value})
            elif key in ['in_file']:
                base = base.filter(in_file=value)
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
    

    def retrieve_in_collection(self, collection):
        collection = self.collection_retrieve(collection)
        variables = Variable.objects.filter(in_collection__in=collection)
        return variables
    
    def get_or_create(self, varprops, unique=True):
        """
        If there is a variable corresponding to varprops with the same full set of properties, 
        return it, otherwise create it. Varprops should be a dictionary which includes at least 
        an identiy and an atomic origin in the key properties.
        In general we should not doing a retrieve when we want to create, hence the default
        value of unique.
        """

        props = self._construct_properties(varprops)
        kp = [p.key for p in props['key_properties']]
        if 'ID' not in kp:
            raise ValueError('Variable definitions must include identity')
        if len(props)!= 6:
            raise ValueError('Insufficient properties to create a variable')
        args = [props[k] for k in ['_proxied','key_properties','spatial_domain',
                                   'time_domain','cell_methods','in_file']]
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
            if k == 'key_properties':
                for vv in v:
                    queries.append((k,vv))
            else:
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
            c = self.collection.retrieve_by_name(collection_name)
            loc = self.location.retrieve(location_name)
        except Collection.DoesNotExist:
            raise ValueError("Collection not yet available in database")
        except Location.DoesNotExist:
            raise ValueError("Location not yet available in database")
        
        # manually controlling rollback
        step = 0
        created = []
        try:
            file = self.file.create(filedata['properties'])
            created.append(file)
            manifests={}
            step = 1
            manidata = filedata.pop('manifests',[])
            for key,value in manidata.items():
                manifest = value.pop('manikey')
                value['cfa_file'] = file
                step = 2
                manifests[key] = self.manifest.add(value)
                created.append(manifests[key])
            step = 3
            vars = []
            for v in filedata['variables']:
                key = v.pop('manikey',None)
                if key is not None:
                    v['in_manifest'] = manifests[key]
                v['in_file'] = file
                var = self.variable.get_or_create(v)
                created.append(var)
                vars.append(var)
            step = 4
            for v in vars:
                self.variable.add_to_collection(c.name,v)
        
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
