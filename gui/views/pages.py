# views/core.py
from django.shortcuts import render
from gui.forms import DateRangeForm

def index(request):
    """Core index view."""
    return render(request, 'gui/home.html')

def projects(request):
    """Projects page view."""
    return render(request, 'gui/projects.html')

def view(request):
    """Selection view with DateRangeForm."""
    return render(request, 'gui/selection.html', {'form': DateRangeForm})