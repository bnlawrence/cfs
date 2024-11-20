from django import forms

class RelationshipForm(forms.Form):
    known_collection = forms.CharField(widget=forms.HiddenInput())   
    related_collection = forms.CharField(widget=forms.Select())  
    relationship_to = forms.CharField(max_length=64, required=True)  
    relationship_from = forms.CharField(max_length=64, required=False)

class DateRangeForm(forms.Form):

    MONTHS = [
        ('','Month'),
        (1,'01'),(2,'02'),(3,'03'),(4,'04'),(5,'05'),(6,'06'),
        (7,'07'),(8,'08'),(9,'09'),(10,'10'),(11,'11'),(12,'12'),        
    ]

    # Define fields for Start date
    start_day = forms.CharField(
        max_length=2,
        required=True,
        widget=forms.TextInput(attrs={
            'id': 'day1',
            'pattern': '^(0?[1-9]|[12][0-9]|3[01])$',
            'title': 'Enter a day between 1 and 31',
            'size': '2',
            'maxlength': '2',
            'value': '1',
        })
    )
    start_month = forms.ChoiceField(
        choices=MONTHS,
        required=True,
        widget=forms.Select(attrs={'id': 'mon1'})
    )
    start_year = forms.CharField(
        max_length=5,
        required=True,
        widget=forms.TextInput(attrs={
            'id': 'yr1',
            'pattern': '^-?\d{1,5}$',
            'title': 'Year Start/End',
            'placeholder': '-4000',
        })
    )

    # Define fields for End date
    end_day = forms.CharField(
        max_length=2,
        required=True,
        widget=forms.TextInput(attrs={
            'id': 'day2',
            'pattern': '^(0?[1-9]|[12][0-9]|3[01])$',
            'title': 'Enter a day between 1 and 31',
            'size': '2',
            'maxlength': '2',
            'value': '1',
        })
    )
    end_month = forms.ChoiceField(
        choices=MONTHS,
        required=True,
        widget=forms.Select(attrs={'id': 'mon2'})
    )
    end_year = forms.CharField(
        max_length=5,
        required=True,
        widget=forms.TextInput(attrs={
            'id': 'yr2',
            'pattern': '^-?\d{1,5}$',
            'title': 'Year Start/End',
            'placeholder': '-4000',
        })
    )

    # Define field for Quark Collection Name
    quark_name = forms.CharField(
        max_length=100,
        required=True,
        widget=forms.TextInput(attrs={
            'id': 'inputTextQ',
        })
    )

