from django.db import models
from django.db.models import Q, Count,OuterRef, Subquery
from django.core.exceptions import ValidationError
from django.db.models.signals import pre_delete, m2m_changed, post_delete
import hashlib

from django.dispatch import receiver


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)

class Value(models.Model):
    """ 
    We hold all the terms used as values of properties, so 
    as to minimise database stuff and speed up 
    querying
    """
    id = models.AutoField(primary_key=True)
    value = models.CharField(max_length=1024, null=True)
    def __str__(self):
        if self.value is not None:
            return self.value
        else:
            return "None"

VALUE_KEYS = ['standard_name','long_name', 'identity', 'atomic_origin','temporal_resolution']

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
    name = models.CharField(max_length=64)
    region = models.CharField(max_length=20)
    nominal_resolution = models.CharField(max_length=12)
    size = models.IntegerField()
    coordinates = models.CharField(max_length=256)

    def __str__(self):
        return f'{self.name}({self.nominal_resolution})'


class VDM(models.Model):
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

    class Meta:
        app_label = "db"


class Protocol(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)


class Location(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    volume = models.IntegerField()
    protocols = models.ManyToManyField(Protocol)
    
    @property
    def protocolset(self):
        return self.protocols.all()
    def __str__(self):
        return f'{self.name} ({sizeof_fmt(self.volume)})'


class File(models.Model):
    """
    Files are the physical manifestation that we have to manage, so
    we need a bit of language. Each entry in the file table is 
    one logical file as instantiated in one or more locations, but that's
    not enough to understand what is going on in "cfstore" thinking.
    This file entity keeps track of the presence of this file in
    various locations, but NOT, the presence of the file in multiple
    collections. Collections know what files they contain.
    #FIXME: Both collections and locations handle deletion of files
    carefully!

    It is possible for one logical file to have more than one physical
    representation in one storage location, but we don't care about thos
    extra physical copies beyone ensuring that we do not delete a 
    collection which has the last reference to a file in a location,
    we don't allow that to happen. 

    """
    class Meta:
        app_label = "db"

    path = models.CharField(max_length=256)
    checksum = models.CharField(max_length=1024)
    checksum_method = models.CharField(max_length=256)
    size = models.IntegerField()
    format = models.CharField(max_length=256, default="Unknown format")
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    # Files may be found in multiple locations, and there may be 
    # multiple copies in different collections in one location, but
    # don't count them as extras. 
    locations = models.ManyToManyField(Location)

    def __str__(self):
        locations = ','.join([x.name for x in self.locations.all()])
        return f"{self.name}({locations})"
    
    def dump(self):
        """ Provide a comprehensive view of this file """
        s = f"{self.name} ({self.format},{self.size}, checksum: {self.checksum}[{self.checksum_method}])\n"
        s += f"[{self.path}]] is in locations:\n"
        s += '\n'.join([x.name for x in self.locations.all()])
        return s
    
    def delete(self,*args,**kwargs):
        """ 
        When a file is deleted, we need to make sure that the volume associated with
        it is removed from any collections and locations. We also need to delete any
        variables where this is the last file which references it. 
        """
        for c in self.collection_set.all():
            c.volume -= self.size
            c.save()
        for l in self.locations.all():
            l.volume -= self.size
            l.save()
        candidates = self.variable_set.all()
        for var in candidates:
            if var.in_files.count() == 1:
                var.delete()
        #chatgpt wants me to call super().delete(*args,**kwargs) here
        # but that clashes with the on_file_delete hooks, so we don't do that
        #to avoid an infinite loop!

@receiver(pre_delete, sender=File)
def on_file_delete(sender, instance, **kwargs):
    # This will be executed before a File object is deleted
    instance.delete()  # Call the custom file deletion logic


class Tag(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=64)

#    collection_id = models.ForeignKey(Collection.id)
#    collections = models.ManyToManyField(Collection)


class CollectionProperty(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    value = models.TextField()
    key = models.CharField(max_length=128)
    # Collection_id = models.ForeignKey(Collection)


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
    properties = models.ManyToManyField(CollectionProperty)
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
                    unique_files.delete()
            else:  # option 2
                all_files.delete()
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


class Variable(models.Model):
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

    def keys(self):
        return self._proxied.keys()

    def exists(self):
        return True
    
    def __str__(self):
        return self.identity.value

    id = models.AutoField(primary_key=True)
    _proxied = models.JSONField()
    long_name = models.ForeignKey(Value, related_name='long_name', null=True, on_delete=models.CASCADE)
    standard_name = models.ForeignKey(Value, related_name='standard_name', null=True, on_delete=models.CASCADE)
    identity = models.ForeignKey(Value, related_name='identity', on_delete=models.CASCADE)
    atomic_origin = models.ForeignKey(Value, related_name='atomic_origin', on_delete=models.CASCADE)
    temporal_resolution =  models.ForeignKey(Value, related_name='temporal_resolution', null=True,on_delete=models.CASCADE)
    domain = models.ForeignKey(Domain, null=True,on_delete=models.SET_NULL)
    cell_methods = models.ForeignKey(Cell_MethodSet, null=True, on_delete=models.SET_NULL) 
    in_files = models.ManyToManyField(File)

    def predelete(self):
        """ 
        When a variable is deleted, we need to make sure that any cell_methods, domains,
        and terms which are unique to this variable are also deleted.
        """
        for x in ['domain','cell_methods']:
            y = getattr(self,x)
            if y.variable_set.count() == 1:
                y.delete()
    def delete(self,*args,**kwargs):
        self.predelete()
        super().delete(*args,**kwargs)




class Var_Metadata(models.Model):
    class Meta:
        app_label = "db"

    type = models.CharField(max_length=16)
    collection_id = models.ForeignKey(Collection, on_delete=models.CASCADE)
    json = models.BooleanField()
    boolean_value = models.BooleanField()
    char_value = models.TextField()
    int_value = models.BigIntegerField()
    real_value = models.FloatField()
    key = models.CharField(max_length=128)




class Directory(models.Model):
    class Meta:
        app_label = "db"
    
    id = models.AutoField(primary_key=True)
    path = models.CharField(max_length=1024)
    location = models.ManyToManyField(Location)
    CFA = models.BooleanField()