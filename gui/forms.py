from django import forms

class RelationshipForm(forms.Form):
    known_collection = forms.CharField(widget=forms.HiddenInput())   
    related_collection = forms.CharField(widget=forms.Select())  
    relationship_to = forms.CharField(max_length=64, required=True)  
    relationship_from = forms.CharField(max_length=64, required=False)

