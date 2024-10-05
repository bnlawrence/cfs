from rest_framework import serializers
from cfs.models import Variable, VariableProperty, VariablePropertySet, TimeDomain, Domain, sizeof_fmt



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
        fields = ['starting','ending','units','calendar']
    
class DomainSerializer(serializers.ModelSerializer):
    class Meta:
        model = Domain
        fields = ['name','nominal_resolution','size','coordinates']
        def to_representation(self,instance):
            # make the size more human readable
            representation = super().to_representation(instance)
            size = sizeof_fmt(representation['size']*4)
            representation['size'] = size
            print(representation)
            return representation


class VariableSerializer(serializers.ModelSerializer):
    key_properties = VariablePropertySetSerializer()  # Ensure this is linked correctly
    time_domain = TimeDomainSerializer(allow_null=True,read_only=True)
    spatial_domain = DomainSerializer(allow_null=True, read_only=True)

    class Meta:
        model = Variable
        fields = ['key_properties','time_domain','spatial_domain']

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
        
        # human readable size
        size = sizeof_fmt(representation['spatial_domain']['size']*4)
        representation['spatial_domain']['size'] = size

        return representation

