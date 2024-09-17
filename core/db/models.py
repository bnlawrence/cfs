from django.db import models
from django.db.models import Q, Count,OuterRef, Subquery, UniqueConstraint
from django.core.exceptions import ValidationError
from django.db.models.signals import pre_delete, m2m_changed, post_delete
import hashlib
import enum

from django.dispatch import receiver


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)



VALUE_KEYS = ['standard_name','long_name', 'identity', 'atomic_origin']

class Domain(models.Model):
    """ 
    Store a representation of a model spatial domain for comparison between variables.
    : name : shorthand name for domain (e.g. N216)
    : region : global or name of domain (e.g. Europe)
    : nominal_resolution : xy resolution as used in CMIP, eg. 50km
    : size : number of xyz points
    : coordinates: comma separarated spatial coordinate names
    """
    class Meta:
        app_label="db"
    id = models.AutoField(primary_key=True)
    # we can't make name unique ... 
    name = models.CharField(max_length=64)
    region = models.CharField(max_length=20)
    nominal_resolution = models.CharField(max_length=12)
    size = models.IntegerField()
    coordinates = models.CharField(max_length=256)
    bbox_tl = models.FloatField(null=True)
    bbox_tr = models.FloatField(null=True)
    bbox_bl = models.FloatField(null=True)
    bbox_br = models.FloatField(null=True)

    def __str__(self):
        return f'{self.name}({self.nominal_resolution})'
    
    def dump(self):
        s = f'{self} - {self.region} ({self.size})\n{self.coordinates}'
        if self.bbox_tl is not None:
            s+= '\n'+','.join([self.bbox_bl,self.bbox_br,self.bbox_tl,self.bbox_tr])
        return s+'\n'

    
class TimeDomain(models.Model):

    class Meta:
        app_label="db"

    interval =  models.IntegerField()
    interval_units = models.CharField(max_length=3,default='d')
    units = models.CharField(max_length=12,default='days')
    calendar = models.CharField(max_length=12, default="standard")
    starting = models.FloatField()
    ending = models.FloatField()
    
    def resolution(self):
        return f'{self.interval} {self.units}'
    
    def __str__(self):
        return f'{self.interval} ({self.units}) from {self.starting} to {self.ending}'


class Location(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    volume = models.IntegerField()
    
    def __str__(self):
        return f'{self.name} ({sizeof_fmt(self.volume)})'

class FileType(models.TextChoices):
    """ 
    Types of file recognised:
     - CFA Atomic Dataseet files, CFA Quark Files
     - Standalone Files, Fragment Files
    """

    ATYPE = 'A', "CFA File holds atomic dataset(s)"
    QTYPE = 'Q', "CFA File holds quark(s)"
    STYPE = 'S', "Standalone File"
    FTYPE = 'F', "Fragment File"

class File(models.Model):
    """
    Files are the physical manifestation that we have to manage, so
    we need a bit of language. Each entry in the file table is 
    one logical file as instantiated in one or more locations, but that's
    not enough to understand what is going on in "cfstore" thinking.
    This file entity keeps track of the presence of this file in
    various locations, but NOT, the presence of the file in multiple
    collections. Collections know what files they contain.
    Files that are part of aggregations are hidden from collections,
    only the aggregation file appears there.

    It is possible for one logical file to have more than one physical
    representation in one storage location, but we don't care about thos
    extra physical copies beyone ensuring that we do not delete a 
    collection which has the last reference to a file in a location,
    we don't allow that to happen. 

    """
   
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    
    # mandatory properties
    name = models.CharField(max_length=256)
    path = models.CharField(max_length=256)
    size = models.IntegerField()
    type = models.CharField(max_length=1,choices=FileType)
    # mandatory for our logic, not for django:
    locations = models.ManyToManyField("Location")

    #optional properties:
    checksum = models.CharField(max_length=1024, null=True)
    checksum_method = models.CharField(max_length=256,null=True)
    uuid = models.UUIDField(null=True)
    format = models.CharField(max_length=256, null=True)
    cfa_file = models.ForeignKey(
        'self', on_delete=models.SET_NULL,
        related_name='fragments',
        null=True
    )

    def __str__(self):
        """ 
        String representation.
        """
        locations = ','.join([x.name for x in self.locations.all()])
        return f"{self.name}({locations})"
    
    def dump(self):
        """ Provide a comprehensive view of this file """
        s = f"{self.name} ({self.format},{self.size}, checksum: {self.checksum}[{self.checksum_method}])\n"
        s += f"{self.uid}[{self.type}]] is in locations:\n"
        s += ','.join([x.name for x in self.locations.all()])
        s += f'\n{self.path}'
        return s
    
    def predelete(self):
        """ 
        When a file is deleted, we need to make sure that the volume associated with
        it is removed from any collections and locations. 
        We explicitly delete the variables because the sql delete cascade does not 
        run the variable delete method, and we need that to happen,
        """
        for c in self.collection_set.all():
            c.volume -= self.size
            c.save()
        for l in self.locations.all():
            l.volume -= self.size
            l.save()
        candidates = self.variable_set.all()
        for v in candidates: 
            v.delete()

    def delete(self,*args,**kwargs):
        self.predelete()
        super().delete(*args,**kwargs)



class Tag(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=64)

#    collection_id = models.ForeignKey(Collection.id)
#    collections = models.ManyToManyField(Collection)


class CollectionType(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    value = models.TextField()
    key = models.CharField(max_length=128)


class Collection(models.Model):
    class Meta:
        app_label = "db"

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def __str__(self):
        return f"{self.name} (v{self.variables.count()},f{self.n_files},{sizeof_fmt(self.volume)})"

    _proxied = models.JSONField()
    name = models.CharField(max_length=256, unique=True)
    volume = models.IntegerField()
    description = models.TextField()
    id = models.AutoField(primary_key=True)
    batch = models.BooleanField()
    files = models.ManyToManyField(File)
    type = models.ManyToManyField(CollectionType)
    tags = models.ManyToManyField(Tag)
    variables = models.ManyToManyField("Variable")

    @property
    def n_files(self):
        return self.files.count()
    
    def unique_files(self):
        """ 
        Return a query set of all those files in this collection that are 
        only in this collection
        """
        # this doesn't work because django has been too clever
        # files = files.annotate(collection_count=Count('collection'))
        # ignore what ChatGPT thinks!
        collection_count_subquery = Collection.objects.filter(
                files=OuterRef('pk')).values('files').annotate(count=Count('id')).values('count')
    
        files = self.files.annotate(collection_count=
                                  Subquery(collection_count_subquery)).filter(
                                      collection_count=1).all()
        return files

    def do_empty(self, delete_files=False, unique_only=True):
        """ 
        Remove all files from collection, possibly deleting them in the process 
        depending on the arguments. Normally used as part of deletion logic elsewhere. 
        
        1: <delete_files=T><unique_only=T> Delete just the unique files. 
        Raise an error if there are non-unique files in the collection.
        
        2: <delete_files=T><unique_only=F> Delete all the files in the collection.
        This will delete files from other collections too. Be careful.
        
        3: <delete_files=F><unique_only=T> This should raise an error if there are any
        unique files in the collection, otherwise it will remove files from the 
        collection which (will still) exist in other collections.
        
        4: <delete_files=F><unique_only=F> Will raise an error if there are any files 
        in the collection at all.
        """
        all_files = self.files.all()
        unique_files = self.unique_files()
        n_unique, n_all  = len(unique_files), len(all_files)
        non_unique = n_all > n_unique
        if delete_files:
            if unique_only: # option 1
                if non_unique:
                    raise PermissionError(
                        f'Cannot empty {self.name} (contains {n_unique} non-unique files)')
                else:
                    # avoid having go do a pre delete hook, since queryset
                    # deletes do not run instnace delete methods 
                    # unique_files.delete()
                    for f in unique_files:
                        f.delete()
            else:  # option 2
                for f in all_files:
                    f.delete()
        else:
            if unique_only: # option 3
                if n_unique == 0:
                    self.files.remove(*all_files)
                else:
                    raise PermissionError(
                        f'Cannot empty {self.name} (contains {n_unique} unique files)')
            else: # option 4
                if n_all > 0:
                    raise PermissionError(
                        f'Cannot empty {self.name} (contains {n_all} files)')

    def delete(self,*args,**kwargs):
        if self.n_files  > 0 :
            raise PermissionError(f'Cannot delete non-empty collection {self.name} (has {self.n_files} files)')
        super().delete(*args,**kwargs)

    def list_files(self):
        return self.name,[f.name for f in self.files.all()]

@receiver(m2m_changed,sender=Collection.files.through)
def intercept_file_removal(sender, instance, action, reverse, model, pk_set, **kwargs):
    """
    This function intercepts when a file is being removed from a collection. We are trying
    to ensure that no file is removed from a collection if it exists only in that
    collection. Such files can only be removed by a forced delete (see do_empty).
    
    Arguments:
    - sender: The model managing the many-to-many relationship (Collection.files.through).
    - instance: The Collection instance from which files are being removed.
    - action: The type of action ('pre_add', 'post_add', 'pre_remove', 'post_remove', etc.).
    - reverse: If True, reverse relation is being affected (related model's field instead).
    - model: The model that is being added or removed (File).
    - pk_set: The primary key set of the objects being added or removed.
    """
    # We're only interested in 'pre_remove'
    if action == 'pre_remove':
        for pk in pk_set:
            file = File.objects.get(pk=pk)
            collection_count = file.collection_set.count()
            if collection_count <= 1:  # Only linked to the current collection
                raise PermissionError(
                    f"File '{file.name}' cannot be removed from collection '{instance.name}' because it is unique to this collection."
                )
            instance.volume -= file.size


class Relationship(models.Model):
    class Meta:
        app_label = "db"

    def __str__(self):
        return f'[{self.subject.name}] [{self.predicate}] [{self.related_to.name}]'

    predicate = models.CharField(max_length=50)
    subject = models.ForeignKey(Collection, related_name="related_to",on_delete=models.CASCADE)
    related_to = models.ForeignKey(Collection, related_name="subject",on_delete=models.CASCADE)

class Cell_Method(models.Model):
    class Meta:
        app_label = "db"
    method = models.CharField(max_length=256)
    axis = models.CharField(max_length=64)
    def __str__(self):
        return f"{self.axis} : {self.method}"

class Cell_MethodSet(models.Model):
    """ 
    A hash table to make cell method searches efficient when needed
    for variable identity matching. THe other option of directly
    linking cell method make creating new variables very inefficient.
    """
    class Meta:
        app_label = "db"
    methods = models.ManyToManyField(Cell_Method)
    key = models.CharField(max_length=64, unique=True)
    def __str__(self):
        return ','.join([str(m) for m in self.methods.all()])

    @staticmethod
    def generate_key(methods):
        """Generate a unique key (e.g., hash) for a list of method ids."""
        method_ids = sorted([str(method.id) for method in methods])  # Sort to avoid ordering issues
        key_string = ",".join(method_ids)  # Create a string with sorted method ids
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()  # Create an MD5 hash

    @classmethod
    def get_or_create_from_methods(cls, methods):
        """Retrieve or create a Cell_MethodSet based on the list of methods."""
        key = cls.generate_key(methods)
        method_set, created = cls.objects.get_or_create(key=key)
        if created:
            method_set.methods.set(methods)  # Set methods if it's a new Cell_MethodSet
        return method_set

class VariablePropertyKeys(models.TextChoices):
    """ 
    Variable properties that should get converted to variable
    properties (other properties are inserted into _proxied).
    """

    SNAME = 'SN', "standard_name"
    LNAME = 'LN', "long_name"
    IDENT = 'ID', "identity"
    ATOMIC = 'AO', "atomic_origin"

    def mykey(self,myvalue):
        reversed = {v:k for k,v in self.choices}
        return reversed[myvalue]

class VariableProperty(models.Model):
    """ 
    We hold all the properties which get used as keys and values of 
    heavily used properties so we can speed things up.
    """
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    key = models.CharField(max_length=2, choices=VariablePropertyKeys)
    value = models.CharField(max_length=1024, null=True)
    def __str__(self):
        return f'{self.key}:{self.value}'


class Variable(models.Model):
    class Meta:
        app_label = "db"
        constraints = [
            UniqueConstraint(fields=['_proxied','spatial_domain', 'time_domain', 'cell_methods', 'in_file'], 
                             name='unique_combination_of_fk_fields')
        ]

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def keys(self):
        return self._proxied.keys()

    def exists(self):
        return True
    
    def __str__(self):
        return self.get_kp('identity')

    id = models.AutoField(primary_key=True)
    _proxied = models.JSONField()
    key_properties = models.ManyToManyField(VariableProperty)
    spatial_domain = models.ForeignKey(Domain, null=True,on_delete=models.SET_NULL)
    time_domain = models.ForeignKey(TimeDomain, null=True, on_delete=models.SET_NULL)
    cell_methods = models.ForeignKey(Cell_MethodSet, null=True, on_delete=models.SET_NULL)
    in_file = models.ForeignKey(File, on_delete=models.CASCADE)

    def get_kp(self, key):
        """ Return a specific key property by name """
        # Use the mykey method from VariablePropertyKeys to map from prop_name to the enum value
        try:
            key_code = VariablePropertyKeys.mykey(VariablePropertyKeys, key)
        except KeyError:
            raise ValueError(f"'{key}' is not a valid key in VariablePropertyKeys.")
        
        # Search for the VariableProperty instance with the matching key
        try:
            var_prop = self.key_properties.get(key=key_code)
            return var_prop.value
        except VariableProperty.DoesNotExist:
            return None  # Return None if no matching property is found


    def save(self, *args, **kwargs):
         # Check uniqueness of other fields first
        try:
            kp = kwargs.pop('key_properties')
            keys = [k.key for k in kp]
            assert 'ID' in keys
        except:
            # This is not normal Django behaviour, but the Variable uniqueness isn't normal Django either
            raise ValueError('Variables must be saved with key properties, one of which must be identity!')
        
        existing_instance = Variable.objects.filter(
            _proxied=self._proxied,
            spatial_domain=self.spatial_domain,
            time_domain=self.time_domain,
            cell_methods=self.cell_methods,
            in_file=self.in_file,
            ).first()
        
        # Manually enforce uniqueness of key_properties in combination with the other fields
        if existing_instance:

            existing_key_properties = set(existing_instance.key_properties.all())
            new_key_properties = set(kp)

            if existing_key_properties == new_key_properties:
                raise ValueError('An instance with this combination of fields and key_properties already exists.')

        # Now that checks have passed, we can save the object
        super().save(*args, **kwargs)  # Save the instance
        self.key_properties.set(kp)   

    @classmethod
    def get_or_create_unique_instance(cls, _proxied, key_properties, spatial_domain, time_domain, cell_methods, in_file):
        """ Sadly repeats a lot of the save logic"""
        # Ensure that key_properties contains the required 'identity'
        keys = [k.key for k in key_properties]
        if 'ID' not in keys:
            raise ValueError("One of the key_properties ({key_properties}) must be 'identity'!")

        # Try to find an existing instance with the same non-ManyToMany fields
        existing_instance = cls.objects.filter(
            _proxied=_proxied,
            spatial_domain=spatial_domain,
            time_domain=time_domain,
            cell_methods=cell_methods,
            in_file=in_file,
        ).first()

        if existing_instance:
            # Check if the key_properties match
            existing_key_properties = set(existing_instance.key_properties.all())
            input_key_properties = set(key_properties)

            if existing_key_properties == input_key_properties:
                return existing_instance, False  # Instance with matching key_properties exists

        # If no matching instance is found, create a new instance
        new_instance = cls(
            _proxied=_proxied,
            spatial_domain=spatial_domain,
            time_domain=time_domain,
            cell_methods=cell_methods,
            in_file=in_file
        )

        # Save the new instance with the key_properties passed as kwargs
        new_instance.save(key_properties=key_properties)

        return new_instance, True  # Return the new instance and a flag indicating creation
    
    def predelete(self):
        """ 
        When a variable is deleted, we need to make sure that any cell_methods, domains,
        and terms which are unique to this variable are also deleted.
        """
        for x in ['spatial_domain','cell_methods','time_domain']:
            y = getattr(self,x)
            if y is not None:
                if y.variable_set.count() == 1:
                    y.delete()

        # Check for orphaned key_properties
        for key_property in self.key_properties.all():
        # If no other variable references this key_property, delete it
            if key_property.variable_set.count() == 1:
                key_property.delete()

    def delete(self,*args,**kwargs):
        self.predelete()
        super().delete(*args,**kwargs)
