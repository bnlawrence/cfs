import os

from django import template
from django.db.models import Q, Count,OuterRef, Subquery

from core.db.models import (Cell_MethodSet, Cell_Method, Collection, CollectionType, 
                            Domain, File, FileType, Location, 
                            VariableProperty, VariablePropertyKeys,
                            Relationship, Tag, TimeDomain, Variable)

from tqdm import tqdm

register = template.Library()
@register.filter

def get_obj_field(obj, key):
    return obj[key]
#FIXME: When and how is the filter used, it's come from the old db.py, but it's a GOB thing.

class CollectionDB:


    def __init__(self):
        self.varprops = {v:k for k, v in VariablePropertyKeys.choices}

    @property
    def _tables(self):
        """
        List the names of all the tables in the database interface
        """
        return self.engine.table_names()
    
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
                definition[key]=self.domain_get_or_create(varprops[key])
            elif key == 'time_domain':
                definition[key]=self.temporal_get_or_create(varprops[key])
            elif key == 'cell_methods':
                method_set = self.cell_methods_get_or_create(varprops[key])
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

    def cell_methods_get_or_create(self, methods):
        """ Handles getting and creating cell methods """
        methods = [self.cell_method_get_or_create(*m) for m in methods]
        method_set = Cell_MethodSet.get_or_create_from_methods(methods)
        return method_set


    def cell_method_add(self, axis, method):
        """
        Add a new cell method to database, raise an error if it already exists.
        Returns the new cell method.
        """
        try:
            cm = self.cell_method_retrieve(axis=axis, method=method)
        except Cell_Method.DoesNotExist:
            cm = Cell_Method(axis=axis, method=method)
            return cm
        else:
            raise ValueError(f"Attempt to add an existing cell method {cm}")

    def cell_method_get_or_create(self, axis, method):
        """
        Retrieve a specfic cell method, if it doesn't exist, create it, and return it.
        """
        cm, created = Cell_Method.objects.get_or_create(axis=axis, method=method)
        if created:
            cm.save()
        return cm
       
    def cell_method_retrieve(self, axis, method):
        """
        Retrieve a specific cell method
        """
        cm = Cell_Method.objects.get(axis=axis, method=method)

        return cm


    def clone(self, instance):
        instance.pk = None
        instance.save()
        return instance

    def collection_aggregate_existing(
        self, grouping, name, description="Saved collection", grouping_id="collections"
        ):
        """
        Groups already stored collections into new named collection
        """
        #def save_as_collection(
        c = self.create_collection(name, description=description)
        if grouping_id == "variables":
            for var in grouping:
                self.add_variable_to_collection(name,var)
                for file in var.in_files:
                    self.file_add_to_collection(c.name, file)
        else:
            for col in grouping:
                files = self.files_retrieve_in_collection(col.name)
                variables = self.variables_retrieve_in_collection(col.name)
                for var in variables:
                    self.variable_add_to_collection(c.name, var)
                    var.save()
                for file in files.distinct():
                    try:
                        self.file_add_to_collection(c.name, file, skipvar=True)

                    except:
                        pass
                    file.save()


    def collection_create(self, collection_name, description=None, kw={}):
        """
        Add a collection and any properties, and return instance
        """
        #def create_collection(self, collection_name, description=None, kw={}):
        if not description:
            description = "No Description"
        # c = Collection.objects.get(name=collection_name)[0]
        c = Collection(
            name=collection_name,
            volume=0,
            description=description,
            batch=1,
            _proxied={},
        )

        # This doesn't exist in the latest GOB code, but I don't know why it was removed
        for k in kw:
            c[k] = kw[k]

        #    c.volume += k.size
        try:
            c.save()
        except Exception as e:
            if str(e).startswith('UNIQUE constraint'):
                raise ValueError('DB IntegrityError: most likely a collection of this name already exists')
        return c

    def collection_delete(self, collection_name, force=False, unique_only=True):
        """
        Remove a collection from the database, ensuring all unique files have already been removed first
        unless force is true. If force is true, delete_colletion will also delete all the unique files
        in the collection. If not unique_only, then all files will be deleted from the collection, even
        those which appear in other collections. Be careful.
        """

        c = self.collection_retrieve(collection_name)
        c.do_empty(force, unique_only)
        c.delete()

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

    def collection_retrieve(
        self, collection_name):
        """
        Retrieve a particular collection via it's name <collection_name>.
        """
        #def retrieve_collection(
        try:
            c = Collection.objects.get(name=collection_name)
        except Collection.DoesNotExist:
            raise ValueError(f"No such collection {collection_name}")
        assert c.name == collection_name
        return c

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
            return Collection.objects.filter(name__contains=name_contains)
        elif description_contains:
            return Collection.objects.filter(description__contains=description_contains)
        elif contains:

            return Collection.objects.filter(
                Q(name__contains=contains) | Q(description__contains=contains)
            )
        elif tagname:
            return Collection.objects.filter(tags__name=tagname)
            # tag = Tag.objects.get(name=tagname)
            # return tag.in_collections
        elif facet:
            # FIXME: I am not sure what is going on with facets and properties right now.
            # So  I've gone back to using the proxied rather than properties option.
            # as of September 2024 to get unit tests going. I expect to be back.
            key, value = facet
            # return Collection.objects.filter(properties_key=key, properties_value=value)
            query = {f'_proxied__{key}': value}
            return Collection.objects.filter(**query)
        else:
            return Collection.objects.all()

    def domain_delete(self, domain_name):
        d = Domain.objects.get(name=domain_name)
        d.delete()
    
    def domain_get_or_create(self, properties):
        """ Attempt to retrieve a domain based on properties, and if it doesn't
        exist, create it and return it."""
        d, created = Domain.objects.get_or_create(**properties)
        if created:
            d.save()
        return d
    
    def domain_retrieve(self, properties):
        """ 
        Retreive domain by properties
        """
        d = Domain.objects.filter(**properties).all()
        if len(d) > 1:
            raise ValueError('Multiple domains match your query')
        return d[0]
    
    def domain_retrieve_by_name(self, name):
        """ 
        Retrieve spatial domain by name
        """
        try:
            return Domain.objects.get(name=name)
        except:
            return None

    def domains_all(self):
        """ 
        Get all the domains
        """
        return Domain.objects.all()
    
    
    def directories_retrieve(self):
        """
        Retrieve directories locations.
        """
        #def retrieve_directories(self):
        dir = Directory.objects.all()
        return dir

    def directory_make(self, path, location, cfa):
        cfa = Directory.objects.create(path=path, location=location,CFA=cfa)
        cfa.save()
        return cfa

    def directory_retrieve_cfa(self):
        """
        Retrieve directories locations.
        """
        cfa = Directory.objects.filter(cfa=True)
        return cfa


    def file_add_to_collection(self,
                               collection_name,
                               file_instance,
                               skipvar=False):
        """
        Add file instance to a collection. Raise an error if it is
        already there. Optionally add variables from file at the same time.
        (Default is always to add variables.)
        """
        f = file_instance
        c = Collection.objects.get(name=collection_name)
        if c.files.filter(name=f.name).exists():
            raise PermissionError(
                f"Attempt to add file {f.name} to {c.name} - but it's already there")
        c.files.add(f)
        c.volume += f.size
        if not skipvar:
            for variable in f.variable_set.all():
                self.add_variable_to_collection(c.name, variable)
        c.save()

    def file_delete_from_collection(self, collection, path):
        """
        Delete a file from a collection
        """

        f = self.file_retrieve_by_properties(path=path)

        c = Collection.objects.get(name=collection)

        if f not in c.files.all():
            print(
                f"Attempt to delete file {f} from {c} - but it's already not there"
            )
        c.files.remove(f)

        f.save()
        c.save()

    def file_find_all_with_variable(self, variable):
        """Find all files with a given variable"""
        return variable.in_files.all()

    def file_remove_from_collection(self, collection_instance, file_instance):
        """
        Remove a file instance from a collection instance
        """
        collection_instance.files.remove(file_instance)

    def file_remove_from_named_collection(self,  collection_name, file_instance,):
        """
        Remove a file instance from a collection with collection identified by
        name.
        """
        c = self.collection_retrieve(collection_name)
        self.file_remove_from_collection(c, file_instance)

    def file_retrieve_by_properties(self, name=None, path=None, size=None, checksum=None, unique=True):
        """
        Find a file instance by propeties. If unique, raise an error if the properties
        provided do not result in a unique file, otherwise return all matching files.
        Raise an error if no such file.
        """
        #def retrieve_file
        #def retrieve_files_by_name
        #def retrieve_file_if_present(self, **kw):
        properties={}
        for k,v in {'name':name, 'path':path, 'size':size, 'checksum':checksum}.items():
            if v is not None:
                properties[k]=v
        fset = File.objects.filter(**properties).all()
        if len(fset) == 0:
            raise FileNotFoundError(f'No file found for {properties}')
        elif len(fset) > 1:
            if unique:
                raise ValueError(f'{properties} describes multiple files')
        return fset[0]

    def files_qsdelete(self, queryset):
        """ 
        Delete a specific set of files returned from some query against the file database
        """
        if queryset.model != File:
            raise PermissionError(f'Attempt to delete a queryset of{queryset.model} with files_delete')
        queryset.delete()
    
    def files_retrieve_from_variables(self, variables):
        files = File.objects.filter(variable__in=variables)
        return files

    def files_retrieve_in_collection(self, collection, match=None, replicants=True):
        """
        Return a list of files in a particular <collection>, possibly including those
        where something in the file name or path matches a particular string <match>
        and/or are replicants. The default <replicants=True> returns all files,
        if <replicants=False> only those files within a collection which have only
        one location are returned.
        """

        dbcollection = self.collection_retrieve(collection)

        if match is None and replicants is True:
            return dbcollection.files.all()

        if match:
            files = dbcollection.files.filter(Q(name__contains=match) | Q(path__contains=match))
        else:
            files = dbcollection.files

        if replicants:
            return files.all()
        else:
            return files.annotate(location_count=Count('locations')).filter(location_count=1).all()


    def files_retrieve_in_collection_and_elsewhere(
        self, collection_name, by_properties=False):
        """
        For a given collection, find all its files which are also in other collections.
        The fast version of this (by_properties=False) simply looks at the files which
        are not unique to this collection. The slow version will look for file duplicates
        which match on properties (name, size, checksum).
        That could be excrutiatingly slow.
        We may need indexes for at least 'name' and 'checksum'.
        """
        #def retrieve_files_in_collection_and_elsewhere(
        #FIXME: replacement for locate_replicants, there will be consequences
        collection = self.collection_retrieve(collection_name)
        files = collection.files.all()
        if not by_properties:
            # We go from files in collection, rather than files in general, because
            # annotating all files will be expensive. It's bad enough we have to do this.
            # this doesn't work because django has been too clever
            # files = files.annotate(collection_count=Count('collection'))

            collection_count_subquery = Collection.objects.filter(
                files=OuterRef('pk')).values('files').annotate(count=Count('id')).values('count')

            files = files.annotate(collection_count=
                                  Subquery(collection_count_subquery)).filter(
                                      collection_count__gt=1).distinct()
        else:
            # this can't be quick without indexes for all these
            # it might be best to index just a couple of those
            # and do it in two steps.
            allfiles = File.objects.all()
            duplicates = allfiles.values('name','size','checksum').annotate(
                        file_count=Count('id'))
            #print([(d['name'],d['file_count']) for d in duplicates])
            duplicates = duplicates.filter(file_count__gt=1)
            files = files.filter(name__in=[item['name'] for item in duplicates],
                                 size__in=[item['size'] for item in duplicates],
                                 checksum__in=[item['checksum'] for item in duplicates])
        return files

    def files_retrieve_in_location(self,location_name):
        files = File.objects.filter(locations__name=location_name).all()
        return files

    def files_retrieve_which_match(self,match):
        """
        Retrieve files where <match> appears in either the path or the name.
        """
        #def retrieve_files_which_match(self, match):
        m = f"%{match}%"
        return (
            File.objects
            .filter(Q(name__contains= m) | Q(path__contains=m))
            .all()
        )

    def location_create(self, location):
        """
        Create a storage <location>. The database is ignorant about what
        "location" means. Other layers of software care about that.
        """
        #def create_location(
        loc = Location.objects.filter(name=location)
        if loc:
            raise PermissionError('Cannot create {location} - it already exists')
        else:
            loc = Location.objects.create(name=location, volume=0)
            loc.save()
            return loc


    def location_delete(self, location_name):
        """
        Remove a location from the database, ensuring all collections have already been removed first.
        #FIXME check collections have been removed first
        """
        loc = Location.objects.filter(name=location_name)
        loc.delete()

    def location_retrieve(self, location_name):
        """
        Retrieve information about a specific location
        """
        #def retrieve_location
        try:
            x = Location.objects.get(name=location_name)
        except Location.DoesNotExist:
            raise ValueError(f"No such collection {location_name}")
        return x

    def locations_retrieve(self):
        """
        Retrieve locations.
        Currently retrieves all known locations.
        """
        #def retrieve_locations
        locs = Location.objects.all()
        return locs


    def organise(self, collection, files, description):
        """
        Organise files already known to the environment into collection,
        (creating collection if necessary)
        No longer needed as files can only be uploaded into collectionsl
        """
        raise NotImplementedError


    def protocol_add(self, protocol_name, locations=[]):
        """
        Add a new protocol to the database, and if desired modify a set of existing or new
        locations by adding the protocol to their list of supported protocols.
        """
        #def add_protocol
        try:
            pdb = Protocol.objects.get(name=protocol_name)
        except Protocol.DoesNotExist:
            pdb = Protocol(name=protocol_name)
            pdb.save()
            if locations:
                existing_locations = [e.name for e in self.locations_retrieve()]
                for p in locations:
                    if p not in existing_locations:
                        loc = Location(name=p)
                    else:
                        loc = self.location_retrieve(p)
                    loc.protocols.add(pdb)
                    loc.save()
        else:
            raise ValueError(f"Attempt to add existing protocol - {protocol_name}")

    def protocols_retrieve(self):
        """
        Retrieve protocols.
        """
        #def retrieve_protocols(self):
        p = Protocol.objects.all()
        return p

    def relationship_add(self, collection_one, collection_two, relationship):
        """
        Add a oneway <relationship> between <collection_one> and <collection_two>.
        e.g. add_relationship('julius','betrayed_by','brutus')
        brutus is not betrayed_by julius.
        """
        #def add_relationship(self, collection_one, collection_two, relationship):
        c1 = self.collection_retrieve(collection_one)
        c2 = self.collection_retrieve(collection_two)

        rel = Relationship(subject=c1, predicate=relationship, related_to=c2)
        rel.save()
        return rel

    def relationship_delete(self, relationshipname):
        """
        Delete a tag, from wherever it is used
        """
        t = Relationship.objects.filter(name=relationshipname).delete()

    def relationships_add(
        self, collection_one, collection_two, relationship_12, relationship_21):
        """
        Add a pair of relationships between <collection_one>  and <collection_two> such that
        collection_one has relationship_12 to collection_two and
        collection_two is a relationship_21 to collection_one.
        e.g. add_relationship('father_x','son_y','parent_of','child_of')
        (It is possible to add a one way relationship by passing relationship_21=None)
        """
        #def add_relationships(
        rel1 = self.relationship_add(collection_one, collection_two, relationship_12)
        if relationship_21 is not None and collection_one != collection_two:
            rel2 = self.relationship_add(
                collection_two, collection_one, relationship_21
            )


    def relationships_retrieve(self, collection, outbound=True, relationship=None):
        """
        Find all relationships from or to a  <collection>, optionally
        which have <relationship> as the predicate.
        """
        #def retrieve_relationships(self, collection, relationship=None):
        c = Collection.objects.get(name=collection)
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


    def tag_collection(self, collection_name, tagname):
        """
        Associate a tag with a collection
        """
        tag,s = self.tag_create(name=tagname)
        c = self.collection_retrieve(collection_name)
        c.tags.add(tag)

    def tag_create(
            self, name):
        """
        Create a tag and insert into a database
        """
        #def create_tag(
        t,s = Tag.objects.get_or_create(name=name)
        t.save()
        return t,s

    def tag_delete(self, tagname):
        """
        Delete a tag, from wherever it is used
        """
        t = Tag.objects.filter(name=tagname).delete()

    def tag_remove_from_collection(self, collection_name, tagname):
        """
        Remove a tag from a collection
        """
        c = self.collection_retrieve(collection_name)
        tag = Tag(name=tagname)
        c.tags.remove(tag)

    def temporal_get_or_create(self, properties):
        """ 
        Get or create a time domain instance from the property string
        """
        td, created = TimeDomain.objects.get_or_create(**properties)
        if created:
            td.save()
        return td
  
       

    def upload_file_to_collection(self, location, collection, f, lazy=0, update=True):
        """
        Convenience API to upload_files_to_collection, simply wraps <f> into a list
        and calls upload_files_to_collection. See that function for explanation of
        arguments.
        """
        return self.upload_files_to_collection(location, collection, 
                                               [f], lazy, update=True, progress=False )[0]


    def upload_files_to_collection(self, location_name, collection_name, list_of_files,
                                   lazy=0, update=True, progress=False):
        """
        Add a list of **new** files from a specific location into the database in a specific
        collection.  The definition of new depends on the value of lazy.

        : location_name : a location already known to the database
        : collection_name : a collection already known to the database
        : list_of_files :  list of files, with each file documented by a dictionary of properties.
            The minimum set of property keys is {name, path, size}. Additional keys that are upderstood
            include {format, checksum, checksum_method}
        : lazy : determines how "new" is interpretted -
            if lazy==0: a pre-existing file with the same full path name is considered a duplicate
            if lazy==1: a pre-existing file with the same full path name  and size is considered a duplicate
            if lazy==2: a pre-existing file with the same checksum is considered a duplicate.
        : update : if a pre-existing file is found in another collection, then we are happy to
                   include it in this collection. (default is True)
        : progress : if True, a progress bar is shown (default is False)


        : return : tuple containing the newly created files in a list, and an updated collection instance.
        """
        # make names a bit more digestable instide the function
        collection, location, files = collection_name, location_name, list_of_files

        try:
            c = Collection.objects.get(name=collection)
            loc = Location.objects.get(name=location)
        except Collection.DoesNotExist:
            raise ValueError("Collection not yet available in database")
        except Location.DoesNotExist:
            raise ValueError("Location not yet available in database")

        results = []
        if progress:
            files = tqdm(files)

        for f in files:
            p = {k:v for k,v in f.items()}
            name, path, size = f["name"], f["path"], f["size"]
            if "checksum" not in f:
                p["checksum"]="Unknown"
            if "format" not in f:
                p["format"] = os.path.splitext(name)[1]
            check = False
            try:
                match lazy:
                    case 0:
                        check = self.file_retrieve_by_properties(
                            path=f['path'], name=f['name'])
                    case 1:
                        check = self.file_retrieve_by_properties(
                            path=f['path'], name=f['name'], size=f['size'])
                    case 2:
                        check = self.file_retrieve_by_properties(
                            path=f['path'], name=f['name'], checksum=f['checksum'])
                    case _:
                        raise ValueError(f"Unexpected value of lazy {lazy}")
            except FileNotFoundError:
                pass

            if check:
                if not update:
                    raise ValueError(
                        f"Cannot upload file {check} as it already exists"
                    )
                else:
                    file, created = File.objects.get_or_create(**p)
                    if created:
                        raise RuntimeError(f'Unexpected additional file created {f}')
            else:
                file, created = File.objects.get_or_create(**p)
                if not created:
                    raise RuntimeError(f'Unexpected failure to create file {f}')

            c.volume += file.size
            c.files.add(file)
            file.locations.add(loc)
            loc.volume += file.size
            file.save()
            # not doing anything with f.replicas right now
            results.append(file)

        c.save()
        loc.save()
        return results

    def variable_add_fragments(self, variable, props):
        """
        As we add variables, we can find CFA fragments, each of
        which had better be new for now!
        """
        #Currently expecting fproperties
        #FIXME: Lots to do with base locations for CFA files
        keys = ['units','calendar','bounds','cfa_file']
        a = Aggregation(**{k:props[k] for k in keys})
        a.save()
        for p, n in zip(props['filenames'],props['cells']):
            size = n*variable.spatial_domain.size
            f = File(name=p,size=size)
            f.part_of.add(a)
            f.save()    

    def variable_add_to_collection(self, collection, variable):
        """
        Add variable to a collection
        """
        c = Collection.objects.get(name=collection)
        # django handles duplicates gracefully
        c.variables.add(variable)
        c.save()

    def variable_delete(self, var_name):
        """
        Remove a variable
        """
        var = Variable.objects.filter(identitity=var_name)
        var.delete()

    def variable_retrieve(self, key, value):
        """Retrieve single variable by arbitrary property"""
        results = self.variables_retrieve_all(key, value)
        if not results.exists():
            return results
        return results[0]

    def variables_retrieve_by_queries(self, queries, from_collection=None):
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
    

    def variable_retrieve_in_collection(self, collection):
        collection = self.collection_retrieve(collection)
        variables = Variable.objects.filter(in_collection__in=collection)
        return variables
    
    def variable_retrieve_or_make(self, varprops):
        """
        If there is a variable corresponding to varprops with the same full set of properties, 
        return it, otherwise create it. Varprops should be a dictionary which includes at least 
        an identiy and an atomic origin in the key properties
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
        return var

    def variable_search(self, key, value):
        return self.variables_retrieve_by_queries([(key,value),])
    
    def variables_all(self):
        return Variable.objects.all()

    def variables_delete_all(self):
        """
        Remove all variables
        """
        vars = Variable.objects.all()
        for var in vars:
            var.delete()

    def variables_retrieve_by_key(self, key, value):
        return self.variables_retrieve_by_queries([(key,value),])


    def variables_retrieve_by_properties(self, properties, from_collection=None, unique=False):
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
                        v = self.file_retrieve_by_properties(**v)
                    queries.append((k,v))
                elif k == 'cell_methods':
                    for x in v:
                        cm = self.cell_method_retrieve(*x)
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
        results = self.variables_retrieve_by_queries(queries, from_collection=from_collection)
        if unique:
            if len(results) > 1:
                raise ValueError('Query retrieved multiple variables ({c}) - but uniqueness was requested')
        return results
        
