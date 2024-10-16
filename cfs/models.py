from django.db import models
from django.db.models import Q, Count,OuterRef, Subquery, UniqueConstraint
from django.core.exceptions import ValidationError
from django.db.models.signals import pre_delete, m2m_changed, post_delete
import hashlib
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

from django.dispatch import receiver

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


class Cell_Method(models.Model):
    
    class Meta:
        app_label = 'cfs'

    method = models.CharField(max_length=64)
    axis = models.CharField(max_length=64)
    qualifier = models.CharField(max_length=64, null=True)
    intervals = models.CharField(max_length=64, null=True)
    def __str__(self):
        s = f"{self.axis} : {self.method}"
        if self.qualifier:
            s+=f' {self.qualifier}'
        if self.intervals:
            s+=f' ({self.intervals})'
        return s

class Cell_MethodSet(models.Model):
    """ 
    A hash table to make cell method searches efficient when needed
    for variable identity matching. THe other option of directly
    linking cell method make creating new variables very inefficient.
    """
    class Meta:
        app_label = 'cfs'
   
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


class CollectionType(models.Model):

    class Meta:
        app_label = 'cfs'
    
    id = models.AutoField(primary_key=True)
    value = models.TextField()
    key = models.CharField(max_length=128)


class Collection(models.Model):

    class Meta:
        app_label = 'cfs'
    
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
        return f"{self.name} (v{self.variables.count()})"

    _proxied = models.JSONField()
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField()
    id = models.AutoField(primary_key=True)
    type = models.ManyToManyField(CollectionType)
    tags = models.ManyToManyField("Tag")
    variables = models.ManyToManyField("Variable", related_name="contained_in")

    def unique_variables(self):
        """ 
        Return a queryset containing all variables in this collection that
        are only in this collection.
        """
        collection_count_subquery = Collection.objects.filter(
                variables=OuterRef('pk')).values('variables').annotate(count=Count('id')).values('count')
        vars = self.variables.annotate(collection_count=
                                Subquery(collection_count_subquery)).filter(collection_count=1).all()
        return vars

    def do_empty(self, force=False):
        """ 
        Empty contents of collection so we can control all the implications for 
        referenced variables and (from them) files.
        : force : boolean - if true, force deletion of variables and files even
        if this collection is the last one which references them.
        """
        unique_vars = self.unique_variables()
        nunique = unique_vars.count()
        if nunique > 0 and not force:
            raise PermissionError(f'Cannot empty collection {self} with unique variables with force=False')
        if hasattr(self,'variables'):
            for v in self.variables.all():
                v.delete()

    def delete(self,*args,**kwargs):
        force = kwargs.pop('force',False)
        self.do_empty(force=force)
        if self.unique_variables().count() > 0:
            raise PermissionError(f'Cannot delete collection {self} with unique variables')
        super().delete(*args,**kwargs)

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
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    # we can't make name unique ... 
    name = models.CharField(max_length=64)
    region = models.CharField(max_length=20)
    nominal_resolution = models.CharField(max_length=12)
    size = models.PositiveIntegerField()
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


class FileType(models.TextChoices):
    """ 
    Types of file recognised:
     - Generic CFA files.
     - CFA Atomic Dataseet files, CFA Quark Files
     - Standalone Files, Fragment Files
    """

    CTYPE = 'C', "CFA File"
    ATYPE = 'A', "CFA File holds atomic dataset(s)"
    QTYPE = 'Q', "CFA File holds quark(s)"
    STYPE = 'S', "Standalone File"
    FTYPE = 'F', "Fragment File"

    @classmethod
    def get_value(cls, key):
        """ Return the display value for the input key """
        for choice_key, choice_value in cls.choices:
            if choice_key == key:
                return choice_value
        raise ValueError(f"Invalid key: {key}")

    def mykey(self,myvalue):
        reversed = {v:k for k,v in self.choices}
        return reversed[myvalue]

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
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    
    # mandatory properties
    name = models.CharField(max_length=128)
    path = models.CharField(max_length=256)
    size = models.PositiveIntegerField(null=True)
    type = models.CharField(max_length=1,choices=FileType)
    # mandatory for our logic, not for django:
    locations = models.ManyToManyField("Location")

    #optional properties:
    checksum = models.CharField(max_length=64, null=True)
    checksum_method = models.CharField(max_length=8,null=True)
    uuid = models.UUIDField(null=True)
    format = models.CharField(max_length=3, null=True)
    #cfa_file = models.ForeignKey(
    #    'self', on_delete=models.SET_NULL,
    #    related_name='fragments',
    #    null=True
    #)

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
    
    def predelete(self, islastvar):
        """ 
        When a file is deleted, we need to make sure that the volume associated with
        it is removed from any collections and locations. 
        We can arrive here via two routes: 
        (1) The file is being deleted, in which case we want to delete all the variables
        because the sql delete cascade does not run the variable delete method, and we
        need that to happen, or
        (2) This file is being deleted as the last variable which references is it
        is being deleted.
        : islastvar: boolean, False = option 1, True = option 2
        """
        if self.size is not None:
            for l in self.locations.all():
                l.volume -= self.size
                l.save()
        if self.type == 'F':
            return
        candidates = self.variable_set.all()
        if not islastvar:
            for v in candidates: 
                v.delete()
        # We need to get rid of fragments here, if we a manifest, since
        # when we are deleted, our manifest goes too, leaving the fragments
        # isolated, and we need to handle their deletion to get volumes
        # to work properly
        if hasattr(self,'manifests'):
            for m in self.manifests.all():
                for f in m.fragments.all():
                    f.delete()

    def delete(self,*args,**kwargs):
        is_lastvar = kwargs.pop('islastvar',False)
        self.predelete(is_lastvar)
        super().delete(*args,**kwargs)


class Location(models.Model):
  
    class Meta:
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256)
    volume = models.PositiveBigIntegerField(default=0)
    
    def __str__(self):
        return f'{self.name} ({sizeof_fmt(self.volume)})'
    
    def delete(self):
        if self.volume != 0:
            raise ValueError(f'Cannot delete location (still holding {sizeof_fmt(self.volume)})')
        else:
            super().delete()


class Manifest(models.Model):
    """
    Carrys information about the set of fragments sufficient to be able to temporally subset 
    based on the time bounds associated with each fragment.
    """

    class Meta:
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    cfa_file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="manifests")
    fragments = models.ManyToManyField(File,related_name='fragment_set')
    bounds = models.BinaryField(null=True)
    units = models.CharField(null=True, max_length=20)
    calendar = models.CharField(null=True,max_length=20)
    total_size = models.PositiveBigIntegerField(null=True)
    parent_uuid = models.UUIDField(null=True)

    def delete(self,*args,**kwargs):
        ignore = kwargs.pop('islastvar',None)
        for f in self.fragments.all():
            f.delete()
        super().delete(*args,**kwargs)

    def __str__(self):
        fcount = self.fragments.count()
        return f'Manifest ({fcount} fragments from {self.cfa_file.name})\n             (first file {self.fragments.first()}).'

    def fragments_as_text(self):
        """ Download a list of fragments for action"""
        return '\n'.join([f.name for f in self.fragments.all()])

class Relationship(models.Model):

    class Meta:
        app_label = 'cfs'

    def __str__(self):
        return f'[{self.subject.name}] [{self.predicate}] [{self.related_to.name}]'

    predicate = models.CharField(max_length=50)
    subject = models.ForeignKey(Collection, related_name="related_to",on_delete=models.CASCADE)
    related_to = models.ForeignKey(Collection, related_name="subject",on_delete=models.CASCADE)


class Tag(models.Model):

    class Meta:
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=64)

class TimeDomain(models.Model):
    """ 
    Represents the domain of sampling
    """
    class Meta:
        app_label = 'cfs'
    
    # sampling interval
    interval =  models.PositiveIntegerField()
    # offset
    interval_offset = models.PositiveIntegerField(null=True)
    # interval units
    interval_units = models.CharField(max_length=3,default='d')

    # date units and calendar
    units = models.CharField(max_length=12,default='days')
    calendar = models.CharField(max_length=12, default="standard")
    # start and end dates
    starting = models.FloatField()
    ending = models.FloatField()
  
    
    def resolution(self):
        return f'{self.interval} {self.units}'
    
    def __str__(self):
        return f'From {self.starting} to {self.ending} ({self.units}, interval {self.interval})'


class VariablePropertyKeys(models.TextChoices):
    """ 
    Variable properties that should get converted to variable
    properties (other properties are inserted into _proxied).
    """

    SNAME = 'SN', "standard_name"
    LNAME = 'LN', "long_name"
    IDENT = 'ID', "identity"
    ATOMIC = 'AO', "atomic_origin"
    FREQ = 'F', "frequency"
    SOURCE = "S", "source"
    SOURCE_ID = 'SI', "source_id"
    VARIANT = 'VL', "variant_label"
    REALM = "R", "realm"
    EXPERIMENT = 'E', "experiment"
    RUNID = 'RI', "runid"

    @classmethod
    def mykey(cls,myvalue):
        reversed = {v:k for k,v in cls.choices}
        return reversed[myvalue]

class VariableProperty(models.Model):
    """ 
    We hold all the properties which get used as keys and values of 
    heavily used properties so we can speed things up.
    """

    class Meta:
        app_label = 'cfs'

    id = models.AutoField(primary_key=True)
    key = models.CharField(max_length=2, choices=VariablePropertyKeys)
    value = models.CharField(max_length=1024, null=True)
    def __str__(self):
        return f'{self.key}:{self.value}'


class VariablePropertySet(models.Model):
    """ 
    Provides a single view of all the properties associated with a 
    variable. We need this to simplify concepts of uniqueness, and 
    improve performance in filtering variables on sets of properties.
    """
    properties = models.ManyToManyField(VariableProperty)
    key = models.CharField(max_length=64, unique=True)

    def __str__(self):
        return ','.join([str(prop) for prop in self.properties.all()])

    @staticmethod
    def generate_key(properties):
        """Generate a unique key (e.g., hash) for a list of property ids."""
        property_ids = sorted([str(prop.id) for prop in properties])
        key_string = ",".join(property_ids)
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()

    @classmethod
    def get_or_create_from_properties(cls, properties):
        """Retrieve or create a VariablePropertySet based on the list of properties."""
        key = cls.generate_key(properties)
        property_set, created = cls.objects.get_or_create(key=key)
        if created:
            property_set.properties.set(properties)  # Set properties if it's a new VariablePropertySet
        return property_set, created


class Variable(models.Model):
    """
    Holds the logic for atomic dataset handling, including building up an atomic
    dataset out of re-usable entities which atomic dataset have in common, such
    as spatial and temporal domains and cell methods.
    """
    class Meta:
        app_label = 'cfs'
        constraints = [
            UniqueConstraint(fields=['_proxied','key_properties','spatial_domain', 'time_domain', 'cell_methods', 'in_file','in_manifest'], 
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

    id = models.AutoField(primary_key=True)
    _proxied = models.JSONField()
    # we force key_properties to be non null in the save, but doing this allows us
    # to handle deletion of orphaned key properties sensibly.
    key_properties = models.ForeignKey(VariablePropertySet,null=True, on_delete=models.SET_NULL)
    spatial_domain = models.ForeignKey(Domain, null=True,on_delete=models.SET_NULL)
    time_domain = models.ForeignKey(TimeDomain, null=True, on_delete=models.SET_NULL)
    cell_methods = models.ForeignKey(Cell_MethodSet, null=True, on_delete=models.SET_NULL)
    in_file = models.ForeignKey(File, on_delete=models.CASCADE)
    in_manifest = models.ForeignKey(Manifest, null=True, on_delete=models.SET_NULL)

    def __str__(self):
        """ String representation using the identity key property"""
        return self.get_kp('identity')
    
    def _vars(self, with_proxied=True):
        s = ''
        for x in ['cell_methods','spatial_domain','time_domain','in_file','in_manifest','_proxied']:
            if x != '_proxied' or with_proxied:
                value = getattr(self,x,None)
                if value is not None:
                    s+= f'  {x}: {value}\n'
        return s

    
    def dump(self, with_proxied=False):
        """ 
        Dump a fuller representation of the variable, including all
        the important re-usable subcomponents.
        """
        s = f"\nField: {self.get_kp('identity')} ({self.get_kp('atomic_origin')})\n"
        s+=f"  sn: {self.get_kp('standard_name')}; ln: {self.get_kp('long_name')}\n"
        s+=self._vars(with_proxied=with_proxied)
        return s
    
    
    def get_kp(self, key):
        """ Return a specific key property by name """
        # Use the mykey method from VariablePropertyKeys to map from prop_name to the enum value
        try:
            key_code = VariablePropertyKeys.mykey(key)
            return self.key_properties.properties.get(key=key_code).value
        except (KeyError, VariableProperty.DoesNotExist):
            return None  # Return None if no matching property is found


    @classmethod    
    def __check_uniqueness(cls, kp, _proxied, spatial_domain, time_domain, cell_methods, in_file, in_manifest):
        """ 
        Check whether or not the incoming variable description has been seen before. 
        """
        property_set, _ = VariablePropertySet.get_or_create_from_properties(kp)       
        existing = Variable.objects.filter(
            _proxied=_proxied,
            key_properties=property_set,
            spatial_domain=spatial_domain,
            time_domain=time_domain,
            cell_methods=cell_methods,
            in_file=in_file,
            in_manifest=in_manifest,
            ).first()
        
        return existing is None, existing
        
    def save(self, *args, **kwargs):
        """ 
        We have our own specific save method as we need to check uniqueness before attempting to save.
        (calls __check_uniqueness__)
        """
        kp = kwargs.pop('key_properties', [])
        if kp:
            if isinstance(kp, list):
                self.key_properties, _ = VariablePropertySet.get_or_create_from_properties(kp)
            else:
                self.key_properties = kp
        if self.get_kp('identity') is None:
            raise ValueError(f"One of the key_properties ({kp}) must be 'identity'!")

        unique, _ = self.__check_uniqueness(kp, self._proxied, self.spatial_domain, self.time_domain,
                                    self.cell_methods, self.in_file, self.in_manifest)
        if unique:
            super().save(*args, **kwargs)
        else:
            raise ValueError(f'Cannot save non-unique variable with value {args}')   

    @classmethod
    def get_or_create_unique_instance(cls, _proxied, key_properties, spatial_domain, time_domain, cell_methods, 
                                      in_file, in_manifest):
        """ 
        We have our own specific get_or_create_unique_instance method
        (calls __check_uniqueness__)
        """
        unique, instance =  cls.__check_uniqueness(key_properties, _proxied, spatial_domain, time_domain, cell_methods, 
                                      in_file, in_manifest)
        if unique:
            key_properties_set, _ = VariablePropertySet.get_or_create_from_properties(key_properties)
            new_instance = cls(
                _proxied=_proxied,
                key_properties=key_properties_set,
                spatial_domain=spatial_domain,
                time_domain=time_domain,
                cell_methods=cell_methods,
                in_file=in_file,
                in_manifest=in_manifest
                )

            # Save the new instance with the key_properties passed as kwargs
            try:
                new_instance.save()
            except:
                logger.debug('Crash coming. Details follow')
                logger.debug(new_instance._vars(with_proxied=False))
                logger.debug(key_properties)
                raise

            return new_instance, True  # Return the new instance and a flag indicating creation
            
        else:
            return  instance, False
        
    def predelete(self):
        """ 
        When a variable is deleted, we need to make sure that any cell_methods, domains,
        and time domains which are unique to this variable are also deleted. Also delete any
        files and manifests if this is the last variable in which they exist.
        Note that key_properties is dealt with post-delete.
        """

         #Check for orphaned properties etc
        if self.key_properties.variable_set.count() == 1:
            # If no other Variable is using this VariablePropertySet, delete it
            property_set = self.key_properties
            # Now, check if any VariableProperty in this set is orphaned (unused by any other set)
            for prop in property_set.properties.all():  # Use .all() to iterate over ManyToManyField
                if prop.variablepropertyset_set.count() == 1:
                    # If no other VariablePropertySet uses this VariableProperty, delete it
                    prop.delete()
            property_set.delete()


        for x in ['spatial_domain','cell_methods','time_domain']:
            y = getattr(self,x)
            if y is not None:
                if y.variable_set.count() ==  1:
                    y.delete()

        #files and manifests:
        for entity in ['in_manifest','in_file']:
            instance = getattr(self,entity)
            if instance is not None:
                if instance.variable_set.count() == 1:
                    instance.delete(islastvar=True)
    
    def delete(self,*args,**kwargs):
        self.predelete()
        super().delete(*args,**kwargs)


   

  
