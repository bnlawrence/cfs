from django.urls import path

from . import views

urlpatterns = [
    # landing page
    path("", views.index, name="home"),

    #main search
    path('collections',views.collections,name='collections'),
    path("view1",views.view,name="selection"),
    path('projects',views.projects,name="projects"),
    path('manifest/<int:col_id>/', views.get_manifests,name='manifest'),

    # cell methods
    path('api/cell-methods/', views.get_cell_methods, name='get_cell_methods'),

    #route for variable property queries
    path('api/variable-properties/', views.get_variable_properties, name='get_variable_properties'),
    
    #route for variable property queries (old version)
    path('api/variable-propertiesk/', views.get_variable_properties_by_key, name='get_variable_properties_by_key'),
    
    path('api/vocab-select/', views.vocab_select, name='vocab_select'),
    path('api/entity-select/', views.entity_select, name='entity_select'),
    path('api/get-variables-from-selection/',views.select_variables, name='select_variables'),
    path('api/add-to-collection/',views.add_to_collection, name ='add_to_collection'),
    path('api/update-collection-description/',views.update_collection_description, name ='update_collection_description'),
    path('api/delete-collection/<int:id>/',views.delete_collection, name='delete_collection'),
    path('api/get-variables-from-collection/', views.get_collection,name='get_collection'),
    path('api/new-related',views.new_related,name='new_related'),
    path('api/update-tags/<int:collection_id>/',views.update_tags,name='update tags'),

    # route for fetching available keys for variable property queries
    path('api/variable-property-keys/', views.get_available_keys, name='get_available_keys'),
]
   