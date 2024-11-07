from django.urls import path

#from . import views
from .views import index, projects, view, collections, get_manifests, get_variable_property_by_key
from .views.api import *
urlpatterns = [
    # landing page
    path("", index, name="home"),

    #main search
    path('collections',collections,name='collections'),
    path("view1",view,name="selection"),
    path('projects',projects,name="projects"),
    path('manifest/<int:col_id>/', get_manifests,name='manifest'),

    # cell methods
    path('api/cell-methods/', get_cell_methods, name='get_cell_methods'),

    #route for variable property queries
    path('api/variable-properties/', get_variable_properties, name='get_variable_properties'),
    
    #route for variable property queries (old version)
    path('api/variable-propertiesk/', get_variable_properties_by_key, name='get_variable_properties_by_key'),
    
    path('api/vocab-select/', vocab_select, name='vocab_select'),
    path('api/entity-select/', entity_select, name='entity_select'),
    path('api/get-variables-from-selection/',select_variables, name='select_variables'),
    path('api/add-to-collection/',add_to_collection, name ='add_to_collection'),
    path('api/update-collection-description/',update_collection_description, name ='update_collection_description'),
    path('api/delete-collection/<int:id>/',delete_collection, name='delete_collection'),
    path('api/get-variables-from-collection/', get_collection,name='get_collection'),
    path('api/new-related',new_related,name='new_related'),
    path('api/update-tags/<int:collection_id>/',update_tags,name='update tags'),
    path('api/make-quarks/',make_quarks,name="make_quarks"),

    # route for fetching available keys for variable property queries
    path('api/variable-property-keys/', get_available_keys, name='get_available_keys'),
]
   