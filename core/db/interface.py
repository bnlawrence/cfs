import os

from django import template
from django.db.models import Q, Count,OuterRef, Subquery

from core.db.cfparse_file import cfparse_file, cfparse_file_to_collection
from core.db.models import (Cell_Method, Collection, Domain, File, Location,
                        Protocol, Relationship, Tag, Variable, Directory, Value, VALUE_KEYS)

from tqdm import tqdm

register = template.Library()
@register.filter

def get_obj_field(obj, key):
    return obj[key]
#FIXME: When and how is the filter used, it's come from the old db.py, but it's a GOB thing.

class CollectionDB:

    @property
    def _tables(self):
        """
        List the names of all the tables in the database interface
        """
        return self.engine.table_names()


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

    def cell_method_get_or_make(self, axis, method):
        """
        Retrieve a specfic cell method, if it doesn't exist, create it, and return it.
        """
        try:
            self.cell_method_retrieve(axis=axis, method=method)
        except Cell_Method.DoesNotExist:
            return self.cell_method_add(axis=axis, method=method)

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
        d = Domain.objects.filter(**properties).all()
        if len(d) > 1:
            raise ValueError('Multiple domains match your query')
        return d[0]
    
    
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

    def file_retrieve_or_make(self, match):
        """
        Retrieve a file of the chosen name. If one doesn't exist, it makes one.
        """
        #FIXME: Duplicate functoinality?
        file = File.objects.filter(name__contains=match).all()
        if not file.exists():
            file = File.objects.create(name=match, size=0)
            file.save()
            file = File.objects.filter(name__contains=match).all()
        return file

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

    def location_create(
        self, location, protocols=[], overwrite=False):
        """
        Create a storage <location>. The database is ignorant about what
        "location" means. Other layers of software care about that.
        However, it may have one or more protocols associated with it.
        """
        #def create_location(
        loc = Location.objects.filter(name=location)
        if loc:
            loc = loc[0]
        if not loc:
            loc = Location.objects.create(name=location, volume=0)
        if overwrite and loc:
            loc.delete()
            loc = Location.objects.create(name=location, volume=0)
        if protocols:
            existing_protocols = self.protocols_retrieve()
            for p in protocols:
                if p not in existing_protocols:
                    pdb = Protocol.objects.create(name=p)
                loc.protocols.add(pdb)
        else:
            protocols = [Protocol.objects.get_or_create(name="none")[0]]
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


    def relationships_retrieve(self, collection, relationship=None):
        """
        Find all relationships to <collection>, optionally
        which have <relationship> as the predicate.
        """
        #def retrieve_relationships(self, collection, relationship=None):
        c = Collection.objects.get(name=collection)
        if relationship:
            try:
                r = Relationship.objects.filter(
                        predicate=relationship, subject=c
                        ).all()
                return r
            except KeyError:
                return []
        else:
            return c.related_to


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

    def upload_file_to_collection(self, location, collection, f, lazy=0, update=True):
        """
        Convenience API to upload_files_to_collection, simply wraps <f> into a list
        and calls upload_files_to_collection. See that function for explanation of
        arguments.
        """
        return self.upload_files_to_collection(
            self, location, collection, [f], lazy, update=True, progress=False )[0]


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

    def variable_add_to_collection(self, collection, variable):
        """
        Add variable to a collection
        """
        c = Collection.objects.get(name=collection)
        if variable in c.variable_set.all():
            print(
                f"Attempt to add variable {variable.long_name}/{variable.standard_name} to {c.name} - but it's already there"
            )
        else:
            variable.in_collection.add(c)
            variable.save()
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

    def variable_retrieve_by_query(self, keys, values):
        """Retrieve variable by arbitrary queru"""
        queries = []
        for key,value in keys,values:
            if key in [
                "id" "long_name",
                "standard_name",
                "cfdm_size",
                "cfdm_domain",
                "cell_methods",
            ]:
                queries.append(getattr(Variable, key) == value)
            else:
                queries.append(Variable.with_other_attributes(key, value))
            if key == "in_files":
                queries.append([value == k for k in Variable.in_files])
            if len(queries) == 0:
                raise ValueError("No query received for retrieve variable")
            elif len(queries) == 1:
                results = Variable.objects.all()
            else:
                # FIXME make sure queries format is DJANGOfyed
                results = Variable.objects.filter(*queries).all()
            return results, queries

    def variable_retrieve_in_collection(self, collection):
        collection = self.collection_retrieve(collection)
        variables = Variable.objects.filter(in_collection__in=collection)
        return variables
    
    def variable_retrieve_or_make(self, varprops, extras={}):
        """
        If there is a variable corresponding to varprops, and it has
        the same extra properties, return it, otherwise
        create it. Varprops should be a dictionary which
        inclues at least an identiy and an atomic origin.
        """
       
   
        def construct_properties(varprops):

            definition = {}
            for key in varprops:
                if key in VALUE_KEYS:
                    value, created = Value.objects.get_or_create(key=key, value=varprops[key])
                    definition[key]=value
                elif key == 'domain':
                    definition[key]=self.domain_get_or_create(varprops[key])
                elif key == 'cell_methods':
                    definition[key]=self.cell_method_get_or_make(varprops[key])
                else:
                    raise RuntimeError(f'Unexpected key in variable construction [{key}]')
            if '_proxied' not in varprops:
                definition['_proxied']={}
            return definition
        
        props = construct_properties(varprops)
        candidates = Variable.objects.filter(**props).all()
        for c in candidates:
            if c._proxied == extras:
                return c
        var = Variable(**props)
        for k,v in extras.items():
            var[k]=v
        var.save()
        return var


    def variable_search(self, key, value):
        results = self.variables_retrieve_all(key, value)
        if not results.exists():
            return results
        return results[0]

    def variables_add_from_file(self, filename, cffilelocation):
        """Add all the variables found in a file to the database"""
        #def add_variables_from_file(self, filename, cffilelocation):
        cfparse_file(self, filename, cffilelocation)


    def variables_add_from_file_to_collection(self, filename, collection, cffilelocation):
            #def add_variables_from_file_to_collection(self, filename, collection, cffilelocation):
        """Add all the variables found in a file to the database"""
        cfparse_file_to_collection(self, filename, collection, cffilelocation)


    def variables_delete_all(self):
        """
        Remove all variables
        """
        vars = Variable.objects.all()
        for var in vars:
            var.delete()

    def variables_retrieve_by_key(self, key, value):
        """
        Retrieve all variables that matches arbitrary property
        describe by a key value pair, unless key = all, in which 
        case return all variables.
        """
        if key == "all":
            results = Variable.objects.all()
        else:
            if key in VALUE_KEYS:
                value = Value.objects.get(key=key,value=value)
            elif key == 'domain':
                value = Domain.objects.get(name=value)
            elif key == 'cell_method':
                value = Cell_Method(**value)
            args = {key:value}
            results = Variable.objects.filter(**args)
        if not results.exists():
            return results
        return results

    def variables_search(self, key, value):
        """Retrieve variable by arbitrary property"""
        return self.variables_retrieve_all(self, key, value)