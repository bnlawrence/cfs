from rest_framework import serializers
from cfs.models import Variable, VariableProperty, VariablePropertySet, TimeDomain, Domain, sizeof_fmt
import cf

class VariablePropertySerializer(serializers.ModelSerializer):
    class Meta:
        model = VariableProperty
        fields = ['key', 'value']

class VariablePropertySetSerializer(serializers.ModelSerializer):
    properties = VariablePropertySerializer(many=True)
    class Meta:
        model = VariablePropertySet
        fields = ['properties']

class TimeDomainSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeDomain
        fields = ['starting','ending','interval']
    def to_representation(self, instance):
        representation = super().to_representation(instance)
        # would it be quicker to check the rest was zeros with strftime, does it matter?
        Y1, M1, D1, h1, m1, s1, _, _, _ = t = instance.cfstart.datetime_array.item().timetuple()
        Y2, M2, D2, h2, m2, s2, _, _, _ = instance.cfend.datetime_array.item().timetuple()
        if all(x==0 for x in (h1,m1,s1,h2,m2,s2)):
            s = f'{Y1}-{M1:02d}-{D1:02d} {instance.calendar}'
            e = f'{Y2}-{M2:02d}-{D2:02d} {instance.calendar}'
        else:
            s = instance.cfstart
            e = instance.cfend

        representation['starting'] = s
        representation['ending'] = e
        representation['interval'] = f'{instance.resolution} ({instance.nt} samples).'
        return representation

class DomainSerializer(serializers.ModelSerializer):
    class Meta:
        model = Domain
        fields = ['name','nominal_resolution','size','coordinates']
    def to_representation(self,instance):
        # make the size more human readable
        representation = super().to_representation(instance)
        size = sizeof_fmt(representation['size']*4)
        representation['size'] = size
        return representation


class VariableSerializer(serializers.ModelSerializer):
    key_properties = VariablePropertySetSerializer()  # Ensure this is linked correctly
    time_domain = TimeDomainSerializer(allow_null=True,read_only=True)
    spatial_domain = DomainSerializer(allow_null=True, read_only=True)
    # the following is the magic incantation to use the get_cell_methods function on this class:
    cell_methods = serializers.SerializerMethodField()

    class Meta:
        model = Variable
        fields = ['key_properties','time_domain','spatial_domain','cell_methods']

    def get_cell_methods(self, instance):
        if instance.cell_methods:
            return str(instance.cell_methods)
        return None

    def to_representation(self, instance):
        # Get the default representation
        representation = super().to_representation(instance)
        
        # Select properties to go to display
        properties_to_include = ['ID','AO','VL']
        
        # Assuming key_properties is a dict-like structure
        properties = representation['key_properties']['properties']
       
        # Create a filtered list of properties
        filtered_properties = [
            prop for prop in properties if prop['key'] in properties_to_include
        ]

        # Update the representation to use the filtered properties
        # properties to include starts out as a list of dictionaries
        representation['key_properties'] =  {item['key']: item['value'] for item in filtered_properties}

        return representation

