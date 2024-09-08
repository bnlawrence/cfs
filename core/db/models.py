from django.db import models

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
    holds_files = models.ManyToManyField("File")
    
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

    def __repr__(self):
        return self.name + ":" + str(self.volume)

    _proxied = models.JSONField()
    name = models.CharField(max_length=256, unique=True)
    volume = models.IntegerField()
    description = models.TextField()
    id = models.AutoField(primary_key=True)
    batch = models.BooleanField()
    files = models.ManyToManyField(File)
    properties = models.ManyToManyField(CollectionProperty)
    tags = models.ManyToManyField(Tag)


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