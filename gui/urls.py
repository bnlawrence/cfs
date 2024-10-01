from django.urls import path

from . import views

urlpatterns = [
    # landing page
    path("", views.index, name="index"),

    #main search
    path("view",views.view,name="view"),

    #main search
    path("oldview",views.oldview,name="oldview"),

    # cell methods
    path('api/cell-methods/', views.get_cell_methods, name='get_cell_methods'),

    #route for variable property queries
    path('api/variable-properties/', views.get_variable_properties, name='get_variable_properties'),
    
    #route for variable property queries (old version)
    path('api/variable-propertiesk/', views.get_variable_properties_by_key, name='get_variable_properties_by_key'),
    

    # route for fetching available keys for variable property queries
    path('api/variable-property-keys/', views.get_available_keys, name='get_available_keys'),

    path('api/get-initial-options/',views.get_view_initial_options, name='get_view_initial_options'),
]