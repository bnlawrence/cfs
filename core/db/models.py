from django.db import models
from django.db.models import Q, Count,OuterRef, Subquery
from django.db.models.signals import pre_delete, m2m_changed

from django.dispatch import receiver


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


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
        """ When a file is deleted, we need to make sure that the volume associated with
        it is removed from any collections and locations """
        for c in self.collection_set.all():
            c.volume -= self.size
            c.save()
        for l in self.locations.all():
            l.volume -= self.size
            l.save()

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
        return f"{self.name} ({self.n_files},{sizeof_fmt(self.volume)})"

    _proxied = models.JSONField()
    name = models.CharField(max_length=256, unique=True)
    volume = models.IntegerField()
    description = models.TextField()
    id = models.AutoField(primary_key=True)
    batch = models.BooleanField()
    files = models.ManyToManyField(File)
    properties = models.ManyToManyField(CollectionProperty)
    tags = models.ManyToManyField(Tag)

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

    def __repr__(self):
        return f'[{self.subject.name}] [{self.predicate}] [{self.related_to.name}]'

    predicate = models.CharField(max_length=50)
    subject = models.ForeignKey(Collection, related_name="subject",on_delete=models.CASCADE)
    related_to = models.ForeignKey(Collection, related_name="related",on_delete=models.CASCADE)


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

    _cell_methods = models.JSONField()
    _proxied = models.JSONField()
    cfdm_size = models.BigIntegerField()
    long_name = models.CharField(max_length=1024, null=True)
    id = models.AutoField(primary_key=True)
    cfdm_domain = models.CharField(max_length=1024)
    standard_name = models.CharField(max_length=1024, null=True)
    in_collection = models.ManyToManyField(Collection)
    in_files = models.ManyToManyField(File)
    identity = models.CharField(max_length=1024)


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


class Cell_Method(models.Model):
    class Meta:
        app_label = "db"

    id = models.AutoField(primary_key=True)
    method = models.CharField(max_length=1024)
    axis = models.CharField(max_length=256)

class Directory(models.Model):
    class Meta:
        app_label = "db"
    
    id = models.AutoField(primary_key=True)
    path = models.CharField(max_length=1024)
    location = models.ManyToManyField(Location)
    CFA = models.BooleanField()