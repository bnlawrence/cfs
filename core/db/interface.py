import os
import sys

from django.db.models import Q, Count
from django import template

from tqdm import tqdm

from core.db.cfparse_file import cfparse_file, cfparse_file_to_collection
from core.db.models import (Cell_Method, Collection, File, Location,
                        Protocol, Relationship, Tag, Variable, Directory)


register = template.Library()

@register.filter
def get_obj_field(obj, key):
    return obj[key]
#FIXME: When and how is the filter used, it's come from the old db.py, but it's a GOB thing.


class CollectionDB:
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

    def add_protocol(self, protocol_name, locations=[]):
        """
        Add a new protocol to the database, and if desired modify a set of existing or new
        locations by adding the protocol to their list of supported protocols.
        """

        try:
            pdb = Protocol.objects.get(name=protocol_name)
        except Protocol.DoesNotExist:
            pdb = Protocol(name=protocol_name)
            pdb.save()
            if locations:
                existing_locations = [e.name for e in self.retrieve_locations()]
                for p in locations:
                    if p not in existing_locations:
                        loc = Location(name=p)
                    else:
                        loc = self.retrieve_location(p)
                    loc.protocols.add(pdb)
                    loc.save()
        else:
            raise ValueError(f"Attempt to add existing protocol - {protocol_name}")

    def add_relationship(self, collection_one, collection_two, relationship):
        """
        Add a oneway <relationship> between <collection_one> and <collection_two>.
        e.g. add_relationship('julius','betrayed_by','brutus')
        brutus is not betrayed_by julius.
        """
        c1 = self.retrieve_collection(collection_one)
        c2 = self.retrieve_collection(collection_two)

        rel = Relationship(subject=c1, predicate=relationship, related_to=c2)
        rel.save()
        return rel

    def add_relationships(
        self, collection_one, collection_two, relationship_12, relationship_21):
        """
        Add a pair of relationships between <collection_one>  and <collection_two> such that
        collection_one has relationship_12 to collection_two and
        collection_two is a relationship_21 to collection_one.
        e.g. add_relationship('father_x','son_y','parent_of','child_of')
        (It is possible to add a one way relationship by passing relationship_21=None)
        """
        rel1 = self.add_relationship(collection_one, collection_two, relationship_12)
        if relationship_21 is not None and collection_one != collection_two:
            rel2 = self.add_relationship(
                collection_two, collection_one, relationship_21
            )

    def add_variables_from_file(self, filename, cffilelocation):
        """Add all the variables found in a file to the database"""
        cfparse_file(self, filename, cffilelocation)

    def add_variables_from_file_to_collection(self, filename, collection, cffilelocation):
        """Add all the variables found in a file to the database"""
        cfparse_file_to_collection(self, filename, collection, cffilelocation)

    def create_collection(self, collection_name, description=None, kw={}):
        """
        Add a collection and any properties, and return instance
        """
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
        # FIXME check for duplicates
        try:
            c.save()
        except Exception as e:
            if str(e).startswith('UNIQUE constraint'):
                raise ValueError('DB IntegrityError: most likely a collection of this name already exists')
        return c

    def save_as_collection(
        self, grouping, name, description="Saved collection", grouping_id="collections"
    ):
        """
        Groups already stored collections into new named collection
        """
        self.create_collection(name, description=description)
        if grouping_id == "variables":
            for var in grouping:
                self.add_variable_to_collection(name,var)
                for file in var.in_files:
                    self.add_file_to_collection(name, file)
        else:
            for col in grouping:
                files = self.retrieve_files_in_collection(col.name)
                variables = self.retrieve_variables_in_collection(col.name)
                for var in variables:
                    self.add_variable_to_collection(name, var)
                    var.save()
                for file in files.distinct():
                    try:
                        self.add_file_to_collection(name, file, skipvar=True)

                    except:
                        pass
                    file.save()

    def create_location(self, location, protocols=[], overwrite=False):
        """
        Create a storage <location>. The database is ignorant about what
        "location" means. Other layers of software care about that.
        However, it may have one or more protocols associated with it.
        """

        loc = Location.objects.filter(name=location)
        if loc:
            loc = loc[0]
        if not loc:
            loc = Location.objects.create(name=location, volume=0)
        if overwrite and loc:
            loc.delete()
            loc = Location.objects.create(name=location, volume=0)
        if protocols:
            existing_protocols = self.retrieve_protocols()
            for p in protocols:
                if p not in existing_protocols:
                    pdb = Protocol.objects.create(name=p)
                loc.protocols.add(pdb)
        else:
            protocols = [Protocol.objects.get_or_create(name="none")[0]]
        loc.save()
        return loc

    def create_tag(self, name):
        """
        Create a tag and insert into a database
        """
        t,s = Tag.objects.get_or_create(name=name)
        t.save()
        return t,s

    def locate_replicants(
        self,
        collection_name,
        strip_base="",
        match_full_path=False,
        try_reverse_for_speed=False,
        check="Both",
    ):
        """
        Locate copies of a file across collections
        strip_base - remove given string from the file string
        match_full_path - find only if the full path matches if true, otherwise only filename
        try_reverse_for_speed - optimization approach that does not yet work and is not implemented
        check - check for "name", "size" or "both", checksum needs to be implemented
        """

        def strip(path, stem):
            """If path starts with stem, return path without the stem, otherwise return the path"""
            if path.startswith(stem):
                return path[len(stem) :]
            else:
                return path

        if try_reverse_for_speed:
            raise NotImplementedError
        else:
            # basic algorithm is to grab all the candidates, and then do a search on those.
            # a SQL wizard would do better.
            c = self.retrieve_collection(collection_name)
            candidates = self.retrieve_files_in_collection(collection_name)
            if check.lower() == "both":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(name=f.name, path=f.path, size=f.size)
                            for f in candidates
                        ]
                    else:
                        possibles = [
                            File.objects.filter(
                                name=f.name,
                                size=f.size,
                            )
                            for f in candidates
                        ]
                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(name=f.name, path=f.path, size=f.size)
                            for f in candidates
                        ]
                    else:
                        possibles = [
                            File.objects.filter(name=f.name, size=f.size)
                            for f in candidates
                        ]
            if check.lower() == "name":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(
                                name=f.name, path=strip(f.path, strip_base), size=f.size
                            )
                            for f in candidates
                        ]
                    else:
                        possibles = [
                            File.objects.filter(name=strip(f.name, strip_base))
                            for f in candidates
                        ]
                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(name=f.name, path=(f.path))
                            for f in candidates
                        ]
                    else:
                        possibles = [
                            File.objects.filter(name=f.name) for f in candidates
                        ]

            if check.lower() == "size":
                if strip_base:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(size=f.size) for f in candidates
                        ]

                    else:
                        possibles = [
                            File.objects.filter(name=f.name, size=f.size)
                            for f in candidates
                        ]

                else:
                    if match_full_path:
                        # likely occurs because ingest required same checksum and/or size and these were not
                        # known at ingest time.
                        possibles = [
                            File.objects.filter(name=f.name, path=f.path)
                            for f in candidates
                        ]

                    else:
                        possibles = [
                            File.objects.filter(size=f.size) for f in candidates
                        ]
        return candidates, possibles

    def retrieve_collection(self, collection_name):
        """
        Retrieve a particular collection via it's name <collection_name>.
        """
        try:
            c = Collection.objects.get(name=collection_name)
        except Collection.DoesNotExist:
            raise ValueError(f"No such collection {collection_name}")
        assert c.name == collection_name
        return c

    def retrieve_collections(
        self,
        name_contains=None,
        description_contains=None,
        contains=None,
        tagname=None,
        facet=None,
    ):
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
            #tag = Tag.objects.get(name=tagname)
            #return tag.in_collections
        elif facet:
            #FIXME: I am not sure what is going on with facets and properties right now.
            # So  I've gone back to using the proxied rather than properties option.
            # as of September 2024 to get unit tests going. I expect to be back.
            key, value = facet
            #return Collection.objects.filter(properties_key=key, properties_value=value)
            query={f'_proxied__{key}':value}
            return Collection.objects.filter(**query)
        else:
            return Collection.objects.all()

    def retrieve_file(self, path, name, size=None, checksum=None):
        """
        Retrieve a file with <path> and <name>.

        If one of <size> or <checksum> are provided (both is an error), make sure
        it has the correct size or checksum.

        (The use case for extra detail is to look for specific files amongst duplicates.)
        """

        if size and checksum:
            raise ValueError("Can only retrieve files by size OR checksum, not both!")
        if size:
            x = File.objects.filter(name=name, path=path, size=size).all()
        elif checksum:
            x = File.objects.filter(name=name, path=path, checksum=checksum).all()
        else:
            x = File.objects.filter(name=name, path=path).all()
        if x:
            assert len(x) == 1
            return x[0]
        else:
            raise FileNotFoundError(f"file {name} not found")

    def retrieve_files_by_name(self, filename):
        x = File.objects.filter(name__contains=filename).all()
        if x:
            assert len(x) > 0
            return x
        else:
            x = None

    def retrieve_file_if_present(self, fullpath, size=None, checksum=None):
        """
        Retrieve a file with <path> and <name>.

        If one of <size> or <checksum> are provided (both is an error), make sure
        it has the correct size or checksum.

        (The use case for extra detail is to look for specific files amongst duplicates.)
        """
        pathname, filename = os.path.split(fullpath)

        if size and checksum:
            raise ValueError("Can only retrieve files by size OR checksum, not both!")
        if size:
            x = File.objects.filter(name=filename, path=pathname, size=size).all()
        elif checksum:
            x = File.objects.filter(
                name=filename, path=pathname, checksum=checksum
            ).all()
        else:
            x = File.objects.filter(name=filename).all()
        if x:
            assert len(x) > 0
            return x[0]
        else:
            x = None

    def retrieve_location(self, location_name):
        """
        Retrieve information about a specific location
        """
        try:
            x = Location.objects.get(name=location_name)
        except Location.DoesNotExist:
            raise ValueError(f"No such collection {location_name}")
        return x

    def retrieve_locations(self):
        """
        Retrieve locations.
        Currently retrieves all known locations.
        """
        locs = Location.objects.all()
        return locs

    def retrieve_directories(self):
        """
        Retrieve directories locations.
        """
        dir = Directory.objects.all()
        return dir

    def retrieve_protocols(self):
        """
        Retrieve protocols.
        """
        p = Protocol.objects.all()
        return p

    def retrieve_relationships(self, collection, relationship=None):
        """
        Find all relationships to <collection>, optionally
        which have <relationship> as the predicate. 
        """
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

    def retrieve_files_which_match(self, match):
        """
        Retrieve files where <match> appears in either the path or the name.
        """
        m = f"%{match}%"
        return (
            File.objects
            .filter(Q(name__contains= m) | Q(path__contains=m))
            .all()
        )

    def retrieve_files_in_location(self,location_name):
        files = File.objects.filter(locations__name=location_name).all()
        return files

    def retrieve_CFA_directory(self):
        """
        Retrieve directories locations.
        """
        cfa = Directory.objects.filter(cfa=True)
        return cfa

    def make_directory(self, path, location, cfa):
        cfa = Directory.objects.create(path=path, location=location,CFA=cfa)
        cfa.save()
        return cfa

    def retrieve_or_make_file(self, match):
        """
        Retrieve a file of the chosen name. If one doesn't exist, it makes one.
        """
        file = File.objects.filter(name__contains=match).all()
        if not file.exists():
            file = File.objects.create(name=match, size=0)
            file.save()
            file = File.objects.filter(name__contains=match).all()
        return file

    def retrieve_files_in_collection(self, collection, match=None, replicants=True):
        """
        Return a list of files in a particular <collection>, possibly including those
        where something in the file name or path matches a particular string <match>
        and/or are replicants. The default <replicants=True> returns all files,
        if <replicants=False> only those files within a collection which have only
        one location are returned.
        """
        
        dbcollection = self.retrieve_collection(collection)
        
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


    def retrieve_files_from_variables(self, variables):
        files = File.objects.filter(variable__in=variables)
        return files

    def delete_file_from_collection(self, collection, file):
        """
        Delete a file from a collection
        """

        path, filename = os.path.split(str(file))
        f = self.retrieve_file(path, filename)

        c = Collection.objects.get(name=collection)

        if f not in c.files.all():
            print(
                f"Attempt to delete file {file} from {c} - but it's already not there"
            )

        c.files.remove(f)
        c.volume -= f.size

        f.save()
        c.save()

    def retrieve_variable(self, key, value):
        """Retrieve single variable by arbitrary property"""
        if key == "identity":
            results = Variable.objects.filter(identity=value)
        elif key == "id":
            results = Variable.objects.filter(id=value)
        elif key == "long_name":
            results = Variable.objects.filter(long_name=value)
        elif key == "standard_name":
            results = Variable.objects.filter(standard_name=value)
        elif key == "cfdm_size":
            results = Variable.objects.filter(cfdm_size=value)
        elif key == "cfdm_domain":
            results = Variable.objects.filter(cfdm_domain=value)
        elif key == "cell_methods":
            results = Variable.objects.filter(cell_methods__in=value)
        elif key == "in_files":
            results = Variable.objects.filter(in_files__in=value)
        elif key == "all":
            return Variable.objects.all()
        else:
            results = None

        if not results.exists():
            return results
        return results[0]

    def retrieve_all_variables(self, key, value):
        """Retrieve all variables that matches arbitrary property"""
        if key == "identity":
            results = Variable.objects.filter(identity=value)
        if key == "id":
            results = Variable.objects.filter(id=value)
        if key == "long_name":
            results = Variable.objects.filter(long_name=value)
        if key == "standard_name":
            results = Variable.objects.filter(standard_name=value)
        if key == "cfdm_size":
            results = Variable.objects.filter(cfdm_size=value)
        if key == "cfdm_domain":
            results = Variable.objects.filter(cfdm_domain=value)
        if key == "cell_methods":
            results = Variable.objects.filter(cell_methods__in=value)
        if key == "in_files":
            results = Variable.objects.filter(in_files__in=value)
        if key == "all":
            return Variable.objects.all()

        if not results.exists():
            return results
        return results

    def retrieve_variable_query(self, keys, values):
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

    def retrieve_or_make_variable(self, standard_name, long_name, identity, cfdm_size, cfdm_domain,realm,location):
        """Retrieve variable by arbitrary property"""
        var = Variable.objects.filter(
            standard_name=standard_name,
            long_name=long_name,
            cfdm_size=cfdm_size,
            cfdm_domain=cfdm_domain,
        ).first()
        if not var:
            var = Variable(
                standard_name=standard_name,
                long_name=long_name,
                identity=identity,
                cfdm_size=cfdm_size,
                cfdm_domain=cfdm_domain,
                realm=realm,
                location=location
            )
        return var

    def generatecollection(self, replacedb, col):
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

    def clone(self, instance):
        instance.pk = None
        instance.save()
        return instance

    def search_variables(self, key, value):
        """Retrieve variable by arbitrary property"""
        if key == "identity" or key == "name":
            results = Variable.objects.filter(identity__contains=value)
        if key == "id":
            results = Variable.objects.filter(id__contains=value)
        if key == "long_name":
            results = Variable.objects.filter(long_name__contains=value)
        if key == "standard_name":
            results = Variable.objects.filter(standard_name__contains=value)
        if key == "cfdm_size":
            results = Variable.objects.filter(cfdm_size__contains=value)
        if key == "cfdm_domain":
            results = Variable.objects.filter(cfdm_domain__contains=value)
        if key == "cell_methods":
            results = Variable.objects.filter(cell_methods__in=value)
        if key == "in_files":
            results = Variable.objects.filter(in_files__in=value)
        if key == "all":
            return Variable.objects.all()

        if not results.exists():
            return results
        return results

    def search_variable(self, key, value):
        results = self.search_variables(key, value)
        if not results.exists():
            return results
        return results[0]

    def show_collections_with_variable(self, variable):
        """Find all collections with a given variable"""
        coldict = {}
        for file in variable.in_files.all():
            for collection in Collection.objects.filter(files=file).all():
                if collection not in coldict:
                    coldict[collection] = 1
                else:
                    coldict[collection] += 1
        return coldict

    def show_files_with_variable(self, variable):
        """Find all files with a given variable"""
        return variable.in_files.all()

    def retrieve_variables_in_collection(self, collection):
        collection = self.retrieve_collection(collection)
        variables = Variable.objects.filter(in_collection__in=collection)
        return variables

    def delete_collection(self, collection_name, force=False):
        """
        Remove a collection from the database, ensuring all files have already been removed first.
        """
        files = self.retrieve_files_in_collection(collection_name)
        if files and force:
            for f in files:
                self.delete_file_from_collection(collection_name, f.path + "/" + f.name)
            c = self.retrieve_collection(collection_name)
            c.save()
            c.delete()

        elif files:
            raise PermissionError(f"Cannot delete: {collection_name} not empty (contains {len(files)} files)"
            )
        else:
            c = self.retrieve_collection(collection_name)
            c.save()
            c.delete()

    def delete_location(self, location_name):
        """
        Remove a location from the database, ensuring all collections have already been removed first.
        #FIXME check collections have been removed first
        """
        loc = Location.objects.filter(name=location_name)
        loc.delete()

    def delete_var(self, var_name):
        """
        Remove a variable
        """
        var = Variable.objects.filter(identitity=var_name)
        var.delete()

    def delete_all_var(self):
        """
        Remove all variables
        """
        vars = Variable.objects.all()
        for var in vars:
            var.delete()

    def delete_tag(self, tagname):
        """
        Delete a tag, from wherever it is used
        """
        t = Tag.objects.filter(name=tagname).delete()

    def delete_relationship(self, relationshipname):
        """
        Delete a tag, from wherever it is used
        """
        t = Relationship.objects.filter(name=relationshipname).delete()

    def add_file_to_collection(self, collection, file, skipvar=False):
        """
        Add file to a collection
        """
        c = Collection.objects.get(name=collection)
        if c.files.filter(name=file.name).exists():
            raise ValueError(
                f"Attempt to add file {file} to {c} - but it's already there"
            )
        c.files.add(file)
        c.volume += file.size
        if not skipvar:
            for variable in file.variable_set.all():
                self.add_variable_to_collection(collection, variable)
        c.save()

    def add_variable_to_collection(self, collection, variable):
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

    def collection_info(self, name):
        """
        Return information about a collection
        """
        try:
            c = Collection.objects.get(name=name)
        except Collection.DoesNotExist:
            raise ValueError(f"No such collection {name}")
        return c.name,c.description,c.files.all()

    def byte_format(self, num, suffix="B"):
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, "Yi", suffix)

    def organise(self, collection, files, description):
        """
        Organise files already known to the environment into collection,
        (creating collection if necessary)
        """
        try:
            c = self.retrieve_collection(collection)
        except ValueError:
            if not description:
                description = "Manually organised collection"
            # c = self.create_collection(collection, description, {})
            c = Collection(name=collection, volume=0, description=description)
            self.session.add(c)
        missing = []
        for f in files:
            path, name = os.path.split(f)
            try:
                ff = self.retrieve_file(path, name)
                c.files.add(ff)
            except FileNotFoundError:
                missing.append(f)
        if missing:
            message = "ERROR: Operation not completed: The following files were not found in database:\n-> "
            message += "\n-> ".join(missing)
            raise FileNotFoundError(message)
        self.session.commit()

    def tag_collection(self, collection_name, tagname):
        """
        Associate a tag with a collection
        """
        
        tag,s = self.create_tag(name=tagname)
        c = self.retrieve_collection(collection_name)
        c.tags.add(tag)

    def remove_tag_from_collection(self, collection_name, tagname):
        """
        Remove a tag from a collection
        """
        c = self.retrieve_collection(collection_name)
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
        
      
    def upload_files_to_collection(self, location, collection, files, 
                                   lazy=0, update=True, progress=False):
        """
        Add a list of (potentially) new files <files> from <location> into the database, 
        and add details to <collection> (both of which must already be known to the system).

        The assumption here is that each file is _new_, as otherwise we would be using
        organise to put the file into a collection. However, depending on the value
        of update, we can decide whether or not to enforce that assumption.

        for each <f> in <files>, <f> is a dictionary of properties of the file. 
        The minimum set of properties are name, path, and size. format and checksum are optional. 
        format is inferred from the full path if it is not provided as a dictionary entry.

        We check that assumption, by looking for

            if lazy==0: a file with the same path before uploading it ,or
            if lazy==1: a file with the same path and size,
            if lazy==2: a file with the same path and checksum.

        If we do find existing files, and <update> is True, then we will simply add
        a link to the new file as a replica. If <update> is False, we raise an error.
        
        if <progress> is true, a progress bar is shown
        
        """

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
            name, path, size = f["name"], f["path"], f["size"]
            if "checksum" not in f:
                f["checksum"] = "None"
            if "format" not in f:
                f["format"] = os.path.splitext(name)[1]
            check = False
            try:
                if lazy == 0:
                    check = self.retrieve_file(path, name)
                elif lazy == 1:
                    check = self.retrieve_file(path, name, size=size)
                elif lazy == 2:
                    check = self.retrieve_file(path, name, checksum=f["checksum"])
                else:
                    raise ValueError(f"Unexpected value of lazy {lazy}")
            except FileNotFoundError:
                pass

            if check:
                if not update:
                    raise ValueError(
                        f"Cannot upload file {os.path.join(path, name)} as it already exists"
                    )
                else:
                    file, created = File.objects.get_or_create(**f)
                    if created:
                        raise RuntimeError(f'Unexpected additional file created {f}')
            else:
                file, created = File.objects.get_or_create(**f) 
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

    def remove_file_from_collection(
        self, collection, file_path, file_name, checksum=None
    ):
        """
        Remove a file described by <path_name> and <file_name> (and optionally <checksum> from a particular
        <collection>. Raise an error if already removed from collection (or, I suppose, if it was never
        in that collection, the database wont know!)
        """
        f = self.retrieve_file(file_path, file_name)
        c = self.retrieve_collection(collection)
        if f not in c.files.all():
            raise ValueError(f"{collection} - file {file_path}/{file_name} not present!")
        else:
            c.files.remove(f)
            c.volume -= f.size
        

    @property
    def _tables(self):
        """
        List the names of all the tables in the database interface
        """
        return self.engine.table_names()

    def delete_collection_with_files(collection):
        pass
